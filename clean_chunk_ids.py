
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import json
import re

load_dotenv()

# Connect to blob storage
blob_service = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
container = blob_service.get_container_client("jennifur-processed")

# Get all blobs
blobs = container.list_blobs()
updated_count = 0

for blob in blobs:
    if blob.name.endswith('.json'):
        # Download blob content
        blob_client = container.get_blob_client(blob.name)
        content = blob_client.download_blob().readall()
        
        try:
            data = json.loads(content)
            
            # Check if chunk_id has parentheses
            if 'chunk_id' in data and '(' in data['chunk_id']:
                # Clean the chunk_id by removing parentheses and replacing with underscores
                old_chunk_id = data['chunk_id']
                new_chunk_id = re.sub(r'[()]+', '_', old_chunk_id)
                data['chunk_id'] = new_chunk_id
                
                # Upload the fixed content
                blob_client.upload_blob(
                    json.dumps(data, indent=2),
                    overwrite=True
                )
                
                updated_count += 1
                print(f"Updated: {old_chunk_id} -> {new_chunk_id}")
                
        except json.JSONDecodeError:
            continue

print(f"\nCleaned {updated_count} documents with invalid chunk_id values")
