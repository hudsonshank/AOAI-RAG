#!/usr/bin/env python3
"""
Bulk Fix Document Keys in Azure Storage

This script efficiently fixes the chunk_id values that contain parentheses
by processing documents in batches and only updating those with invalid keys.
"""

import os
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from dotenv import load_dotenv
import time

load_dotenv()

def clean_chunk_id(chunk_id):
    """Clean a chunk_id by removing invalid characters for Azure Search"""
    if not chunk_id:
        return chunk_id
    
    # Replace parentheses and other invalid characters with underscores
    # Azure Search keys can only contain letters, digits, underscore (_), dash (-), or equal sign (=)
    cleaned = re.sub(r'[^a-zA-Z0-9_\-=]', '_', chunk_id)
    
    # Remove consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    
    return cleaned

def process_blob(blob_name, container_client):
    """Process a single blob to fix chunk_id if needed"""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download and parse JSON content
        content = blob_client.download_blob().readall().decode('utf-8')
        data = json.loads(content)
        
        # Check if chunk_id needs cleaning
        original_chunk_id = data.get('chunk_id', '')
        if not original_chunk_id:
            return None, "No chunk_id found"
            
        # Check if it contains invalid characters
        if not re.search(r'[^a-zA-Z0-9_\-=]', original_chunk_id):
            return None, "Already valid"
            
        # Clean the chunk_id
        cleaned_chunk_id = clean_chunk_id(original_chunk_id)
        
        if cleaned_chunk_id != original_chunk_id:
            data['chunk_id'] = cleaned_chunk_id
            
            # Upload the fixed content
            blob_client.upload_blob(
                json.dumps(data, indent=2),
                overwrite=True
            )
            
            return blob_name, f"Fixed: {original_chunk_id} -> {cleaned_chunk_id}"
        else:
            return None, "No change needed"
            
    except json.JSONDecodeError as e:
        return blob_name, f"JSON decode error: {str(e)}"
    except Exception as e:
        return blob_name, f"Error: {str(e)}"

def fix_document_keys_bulk():
    """Fix document keys in bulk with parallel processing"""
    
    print("🔧 Bulk Fixing Document Keys in Azure Storage")
    print("=" * 60)
    
    # Connect to blob storage
    try:
        blob_service = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )
        container_client = blob_service.get_container_client("jennifur-processed")
        
        print("✅ Connected to Azure Storage")
        
    except Exception as e:
        print(f"❌ Failed to connect to storage: {str(e)}")
        return
    
    # Get all JSON blobs
    print("\n📋 Scanning for JSON documents...")
    json_blobs = []
    
    try:
        for blob in container_client.list_blobs():
            if blob.name.endswith('.json'):
                json_blobs.append(blob.name)
        
        print(f"✅ Found {len(json_blobs):,} JSON documents")
        
        if len(json_blobs) == 0:
            print("❌ No JSON documents found!")
            return
            
    except Exception as e:
        print(f"❌ Error listing blobs: {str(e)}")
        return
    
    # Process blobs in parallel batches
    print(f"\n🚀 Processing documents with {min(20, len(json_blobs))} parallel workers...")
    
    start_time = time.time()
    updated_count = 0
    error_count = 0
    processed_count = 0
    
    # Process in batches to avoid overwhelming the system
    batch_size = 1000
    
    for batch_start in range(0, len(json_blobs), batch_size):
        batch_blobs = json_blobs[batch_start:batch_start + batch_size]
        
        print(f"\n📦 Processing batch {batch_start//batch_size + 1}/{(len(json_blobs)-1)//batch_size + 1} "
              f"({len(batch_blobs)} documents)...")
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all tasks in this batch
            future_to_blob = {
                executor.submit(process_blob, blob_name, container_client): blob_name
                for blob_name in batch_blobs
            }
            
            # Process completed tasks
            for future in as_completed(future_to_blob):
                blob_name = future_to_blob[future]
                processed_count += 1
                
                try:
                    result, message = future.result()
                    
                    if result:  # Document was updated
                        updated_count += 1
                        if updated_count <= 10:  # Show first 10 updates
                            print(f"  ✅ {message}")
                        elif updated_count % 100 == 0:  # Show progress every 100
                            print(f"  📈 Updated {updated_count} documents so far...")
                    
                    if "Error" in message:
                        error_count += 1
                        if error_count <= 5:  # Show first 5 errors
                            print(f"  ❌ {blob_name}: {message}")
                
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:
                        print(f"  ❌ {blob_name}: Exception {str(e)}")
                
                # Show progress every 1000 documents
                if processed_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed
                    remaining = len(json_blobs) - processed_count
                    eta = remaining / rate if rate > 0 else 0
                    print(f"  📊 Progress: {processed_count:,}/{len(json_blobs):,} "
                          f"({processed_count/len(json_blobs)*100:.1f}%) - "
                          f"ETA: {eta/60:.1f} minutes")
    
    # Final summary
    elapsed_time = time.time() - start_time
    print(f"\n📊 BULK CLEANUP COMPLETED")
    print("=" * 60)
    print(f"Total Documents Scanned: {processed_count:,}")
    print(f"Documents Updated: {updated_count:,}")
    print(f"Documents with Errors: {error_count:,}")
    print(f"Processing Time: {elapsed_time/60:.1f} minutes")
    print(f"Average Rate: {processed_count/elapsed_time:.1f} docs/second")
    
    if updated_count > 0:
        print(f"\n✅ SUCCESS: Fixed {updated_count:,} documents with invalid chunk_id values")
        print(f"🔄 NEXT STEP: Reset and run the Azure Search indexer")
        print(f"📈 EXPECTED RESULT: Document count should increase dramatically")
        print(f"🎯 TARGET: Should now index close to {len(json_blobs):,} documents")
    else:
        print(f"\n⚠️ No documents needed updating")
        print(f"This suggests the chunk_id cleanup may have been done already")
        print(f"Check if there are other issues preventing indexing")

if __name__ == "__main__":
    fix_document_keys_bulk()