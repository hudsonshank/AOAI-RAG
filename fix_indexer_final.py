#!/usr/bin/env python3
"""
Fix Azure Search Indexer Configuration - Final Version

The issue is that chunk_id (the key field) contains parentheses, which are invalid.
Since we can't modify the key field through mappings, we need to:
1. Configure proper field mappings for metadata
2. Increase max_failed_items to handle the invalid keys temporarily
3. Guide the user to fix the source data

The real solution requires cleaning the source data in blob storage.
"""

import os
import json
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping,
    IndexingParameters
)
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def fix_indexer_configuration():
    """Fix indexer configuration and provide guidance for source data cleanup"""
    
    # Initialize client
    indexer_client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    indexer_name = "jennifur-rag-indexer"
    
    try:
        print(f"🔧 Fixing indexer configuration for: {indexer_name}")
        print("=" * 60)
        
        # Get current indexer
        indexer = indexer_client.get_indexer(indexer_name)
        print("✅ Retrieved current indexer configuration")
        
        print(f"\n📋 Configuring optimal field mappings")
        
        # Configure field mappings properly (don't touch the key field)
        indexer.field_mappings = [
            # Map storage metadata
            FieldMapping(
                source_field_name="metadata_storage_name",
                target_field_name="title"
            ),
            # Map all the critical metadata fields
            FieldMapping(
                source_field_name="client_name",
                target_field_name="client_name"
            ),
            FieldMapping(
                source_field_name="pm_initial",
                target_field_name="pm_initial"
            ),
            FieldMapping(
                source_field_name="document_category",
                target_field_name="document_category"
            ),
            FieldMapping(
                source_field_name="is_client_specific",
                target_field_name="is_client_specific"
            ),
            FieldMapping(
                source_field_name="document_path",
                target_field_name="document_path"
            ),
            FieldMapping(
                source_field_name="filename",
                target_field_name="filename"
            ),
            FieldMapping(
                source_field_name="chunk",
                target_field_name="chunk"
            )
        ]
        
        print(f"✅ Configured {len(indexer.field_mappings)} field mappings")
        
        # Configure indexing parameters to handle the key validation issues temporarily
        print(f"\n⚙️ Configuring indexing parameters")
        
        if not indexer.parameters:
            indexer.parameters = IndexingParameters()
        
        indexer.parameters.parsing_mode = "jsonArray"
        # Increase failed items limit to handle the 303 invalid keys
        indexer.parameters.max_failed_items = 500  
        indexer.parameters.max_failed_items_per_batch = 50
        
        # Add configuration to continue processing despite failures
        indexer.parameters.configuration = {
            "indexedFileNameExtensions": ".json",
            "failOnUnsupportedContentType": False,
            "failOnUnprocessableDocument": False
        }
        
        print("✅ Configured indexing parameters")
        print(f"   - Parsing Mode: {indexer.parameters.parsing_mode}")
        print(f"   - Max Failed Items: {indexer.parameters.max_failed_items}")
        print(f"   - Max Failed Items Per Batch: {indexer.parameters.max_failed_items_per_batch}")
        
        # Clear any problematic output field mappings
        indexer.output_field_mappings = []
        
        # Update the indexer
        print(f"\n🚀 Updating indexer configuration")
        
        updated_indexer = indexer_client.create_or_update_indexer(indexer)
        print("✅ Indexer configuration updated successfully!")
        
        # Reset and run the indexer
        print(f"\n🔄 Resetting and running indexer")
        
        indexer_client.reset_indexer(indexer_name)
        print("✅ Indexer reset completed")
        
        indexer_client.run_indexer(indexer_name)
        print("✅ Indexer run started")
        
        print(f"\n📊 CONFIGURATION APPLIED")
        print("=" * 60)
        print("✅ All metadata field mappings configured correctly")
        print("✅ Max failed items increased to handle key validation errors")
        print("✅ Indexer will process valid documents and skip invalid keys")
        
        print(f"\n🚨 CRITICAL ISSUE IDENTIFIED:")
        print("The chunk_id field (document key) contains parentheses in 303 documents.")
        print("Examples of problematic keys:")
        print("  - '01R2ELWPQDEYHRWFKLWFCJXSMZ4AZSVJ6N_sheet_Autobahn_(old)_0'")
        print("  - Documents with '(old)', '(copy)', '(final)' in filenames")
        
        print(f"\n🛠️ PERMANENT SOLUTION REQUIRED:")
        print("You need to clean the source data in the 'jennifur-processed' container.")
        print("\n1. IMMEDIATE FIX - Run this Python script:")
        
        cleanup_script = '''
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

print(f"\\nCleaned {updated_count} documents with invalid chunk_id values")
'''
        
        print("Save this as 'clean_chunk_ids.py' and run it.")
        
        print(f"\n2. AFTER CLEANUP:")
        print("   - Reset and run the indexer again")
        print("   - The 303 failed documents should now index successfully")
        print("   - Reduce max_failed_items back to 10-50")
        
        print(f"\n3. MANUAL STEPS STILL NEEDED:")
        print("   Add output field mapping in Azure Portal:")
        print("   - Go to: Azure Portal > Search Service > Indexers > jennifur-rag-indexer")
        print("   - Edit JSON and add:")
        print('     "outputFieldMappings": [')
        print('       {')
        print('         "sourceFieldName": "/document/normalized_pages/*/text_vector",')
        print('         "targetFieldName": "contentVector"')
        print('       }')
        print('     ]')
        
        print(f"\n📈 EXPECTED RESULTS AFTER CLEANUP:")
        print("- Document count should increase from ~700 to ~1000+")
        print("- All client metadata will be properly indexed and filterable")
        print("- RAG responses will be accurate and client-specific")
        print("- No more document key validation errors")
        
        # Save the cleanup script
        with open('clean_chunk_ids.py', 'w') as f:
            f.write(cleanup_script)
        print(f"\n💾 Saved cleanup script as 'clean_chunk_ids.py'")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_indexer_configuration()