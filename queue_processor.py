#!/usr/bin/env python3
"""
Azure Queue Processor for olmOCR
Polls Azure Storage Queue, processes PDFs with olmOCR, and uploads results.
"""

import os
import sys
import json
import time
import base64
import tempfile
import traceback
import subprocess
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

# Configuration from environment variables
AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
QUEUE_NAME = os.environ.get('QUEUE_NAME', 'pdf-processing-queue')
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '600'))  # 10 minutes default
POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', '5'))  # 5 seconds between polls

# Container names
INPUT_CONTAINER = 'input-pdfs'
OUTPUT_CONTAINER = 'output-markdown'
ERROR_CONTAINER = 'pdf-errors'

def setup_azure_clients():
    """Initialize Azure Storage clients."""
    if not AZURE_CONNECTION_STRING:
        raise ValueError("AZURE_CONNECTION_STRING environment variable not set")
    
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    queue_client = QueueClient.from_connection_string(AZURE_CONNECTION_STRING, QUEUE_NAME)
    
    # Ensure queue exists (using proper exception handling)
    try:
        queue_client.create_queue()
        print(f"Created queue: {QUEUE_NAME}")
    except ResourceExistsError:
        print(f"Queue already exists: {QUEUE_NAME}")
    except Exception as e:
        print(f"Error with queue: {e}")
        raise
    
    # Ensure containers exist
    for container_name in [INPUT_CONTAINER, OUTPUT_CONTAINER, ERROR_CONTAINER]:
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            print(f"Created container: {container_name}")
        except ResourceExistsError:
            print(f"Container already exists: {container_name}")
        except Exception as e:
            print(f"Container {container_name} setup: {e}")
            pass  # Continue even if container setup has issues
    
    return blob_service_client, queue_client

def download_pdf(blob_service_client, document_id, local_path):
    """Download PDF from blob storage."""
    blob_name = f"{document_id}.pdf"
    blob_client = blob_service_client.get_blob_client(
        container=INPUT_CONTAINER, 
        blob=blob_name
    )
    
    with open(local_path, 'wb') as download_file:
        download_file.write(blob_client.download_blob().readall())
    
    print(f"Downloaded {blob_name} to {local_path}")

def process_with_olmocr(pdf_path, output_dir):
    """Run olmOCR on the PDF file."""
    # Ensure the output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        'python', '-m', 'olmocr.pipeline',
        str(output_dir),
        '--markdown',
        '--pdfs', str(pdf_path),
        '--workers', '1'  # Use single worker for debugging
    ]
    
    print(f"Running olmOCR command:")
    print(f"  {' '.join(cmd)}")
    print(f"  PDF exists: {pdf_path.exists()}")
    print(f"  PDF size: {pdf_path.stat().st_size} bytes")
    print(f"  Output dir exists: {output_dir.exists()}")
    
    # Run without timeout first to see if it completes
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=VISIBILITY_TIMEOUT - 60
        )
    except subprocess.TimeoutExpired as e:
        print(f"olmOCR timed out after {VISIBILITY_TIMEOUT - 60} seconds")
        print(f"Stdout so far: {e.stdout}")
        print(f"Stderr so far: {e.stderr}")
        raise
    
    # Print full output for debugging
    if result.stdout:
        print(f"olmOCR stdout:\n{result.stdout}")
    if result.stderr:
        print(f"olmOCR stderr:\n{result.stderr}")
    print(f"olmOCR return code: {result.returncode}")
    
    if result.returncode != 0:
        raise Exception(f"olmOCR failed with return code {result.returncode}")
    
    print(f"olmOCR completed successfully")
    
    # List all files created in output directory
    print(f"\nFiles in output directory after olmOCR:")
    for root, dirs, files in os.walk(output_dir):
        level = root.replace(str(output_dir), '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            file_path = os.path.join(root, file)
            size = os.path.getsize(file_path)
            print(f"{subindent}{file} ({size} bytes)")
    
    return True

def upload_markdown(blob_service_client, document_id, output_dir):
    """Upload markdown result to blob storage."""
    markdown_dir = output_dir / "markdown"
    
    print(f"Looking for markdown files...")
    print(f"Markdown directory path: {markdown_dir}")
    print(f"Output directory path: {output_dir}")
    
    # List everything in the output directory
    print(f"Contents of output directory:")
    for item in output_dir.rglob('*'):
        print(f"  {item.relative_to(output_dir)} ({'dir' if item.is_dir() else 'file'})")
    
    # Try multiple locations for markdown files
    possible_locations = [
        markdown_dir.glob('*.md'),  # Direct .md files in markdown dir
        markdown_dir.glob('**/*.md'),  # Recursive search in markdown dir
        output_dir.glob('*.md'),  # Direct .md files in output dir
        output_dir.rglob('*.md'),  # Recursive search in entire output
        output_dir.parent.rglob('*.md'),  # Check parent directory too
    ]
    
    markdown_files = []
    for location in possible_locations:
        files = list(location)
        if files:
            print(f"Found markdown files at {location}: {files}")
            markdown_files.extend(files)
            break
    
    if not markdown_files:
        # Check for JSONL files (Dolma format) as fallback
        jsonl_files = list(output_dir.rglob('*.jsonl'))
        if jsonl_files:
            print(f"No .md files found, but found JSONL files: {jsonl_files}")
            print("Note: olmOCR might be outputting Dolma format instead of markdown")
            raise Exception("No markdown file generated - found JSONL instead. May need to adjust olmOCR parameters.")
        else:
            raise Exception("No markdown or JSONL files generated")
    
    # Use the first markdown file found
    markdown_path = markdown_files[0]
    print(f"Using markdown file: {markdown_path}")
    
    blob_name = f"{document_id}.md"
    blob_client = blob_service_client.get_blob_client(
        container=OUTPUT_CONTAINER,
        blob=blob_name
    )
    
    with open(markdown_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"Uploaded markdown to {OUTPUT_CONTAINER}/{blob_name}")

def mark_error(blob_service_client, document_id, error_msg=""):
    """Create an error marker file in the error container."""
    blob_name = f"{document_id}.error"
    blob_client = blob_service_client.get_blob_client(
        container=ERROR_CONTAINER,
        blob=blob_name
    )
    
    # Upload empty file or error message
    blob_client.upload_blob(error_msg.encode('utf-8'), overwrite=True)
    print(f"Marked error for {document_id}")

def process_message(blob_service_client, message_content):
    """Process a single queue message."""
    try:
        # First check if the content is base64 encoded
        # Base64 JSON always starts with "eyJ" (which is "{" in base64)
        if isinstance(message_content, str) and message_content.startswith('eyJ'):
            # Decode base64 first
            decoded_bytes = base64.b64decode(message_content)
            decoded_str = decoded_bytes.decode('utf-8')
            print(f"Decoded base64 message: {decoded_str}")
            data = json.loads(decoded_str)
        elif isinstance(message_content, str):
            # Try direct JSON parse
            data = json.loads(message_content)
        elif isinstance(message_content, bytes):
            data = json.loads(message_content.decode('utf-8'))
        else:
            data = message_content
            
    except Exception as e:
        print(f"Error parsing message: {e}")
        print(f"Message content: {repr(message_content)}")
        raise
    
    document_id = data['document_id']
    print(f"Processing document: {document_id}")
    
    # Create temporary working directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        pdf_path = temp_path / f"{document_id}.pdf"
        output_dir = temp_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        try:
            # Download PDF
            download_pdf(blob_service_client, document_id, pdf_path)
            
            # Process with olmOCR
            process_with_olmocr(pdf_path, output_dir)
            
            # Upload result - pass the output_dir, not markdown_dir
            upload_markdown(blob_service_client, document_id, output_dir)
            
            print(f"Successfully processed {document_id}")
            return True
            
        except Exception as e:
            print(f"Error processing {document_id}: {str(e)}")
            traceback.print_exc()
            
            # Mark as error
            try:
                mark_error(blob_service_client, document_id, str(e))
            except Exception as error_e:
                print(f"Failed to mark error: {error_e}")
            
            return False

def main():
    """Main processing loop."""
    print("Starting olmOCR Queue Processor")
    print(f"Queue: {QUEUE_NAME}")
    print(f"Visibility Timeout: {VISIBILITY_TIMEOUT}s")
    print(f"Poll Interval: {POLL_INTERVAL}s")
    
    # Setup Azure clients
    blob_service_client, queue_client = setup_azure_clients()
    
    print("\nReady to process messages. Polling queue...\n")
    
    while True:
        try:
            # Get messages from queue
            messages = queue_client.receive_messages(
                visibility_timeout=VISIBILITY_TIMEOUT,
                max_messages=1
            )
            
            message_found = False
            for message in messages:
                message_found = True
                print(f"Received message: {message.id}")
                
                # The message content is in message.content for QueueMessage
                # Try different ways to access the content
                message_text = None
                if hasattr(message, 'content'):
                    message_text = message.content
                elif hasattr(message, 'message_text'):
                    message_text = message.message_text
                else:
                    print(f"Message object attributes: {dir(message)}")
                    raise Exception("Cannot find message content")
                
                print(f"Message content type: {type(message_text)}")
                print(f"Message content preview: {str(message_text)[:200]}")
                
                try:
                    # Process the message (use message_text, not message.content)
                    success = process_message(blob_service_client, message_text)
                    
                    # Delete message from queue (success or failure)
                    queue_client.delete_message(message)
                    print(f"Deleted message from queue: {message.id}")
                    
                except Exception as e:
                    print(f"Fatal error processing message: {e}")
                    traceback.print_exc()
                    # Don't delete message on fatal errors - let it become visible again
            
            if not message_found:
                print(f"No messages in queue. Waiting {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
                
        except KeyboardInterrupt:
            print("\nShutting down gracefully...")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            traceback.print_exc()
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()