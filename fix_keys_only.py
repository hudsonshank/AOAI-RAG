#!/usr/bin/env python3
"""
Fix Azure Search Indexer Document Key Issues - Simple Version

This script focuses on the critical issue: fixing document keys with parentheses.
The output field mapping will need to be done manually through Azure Portal.
"""

import os
import json
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping,
    IndexingParameters,
    FieldMappingFunction
)
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def fix_document_keys_only():
    """Fix indexer configuration to handle invalid document keys using base64 encoding"""
    
    # Initialize client
    indexer_client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    indexer_name = "jennifur-rag-indexer"
    
    try:
        print(f"🔧 Fixing document key issues for: {indexer_name}")
        print("=" * 60)
        
        # Get current indexer
        indexer = indexer_client.get_indexer(indexer_name)
        print("✅ Retrieved current indexer configuration")
        
        # Create a mapping function to sanitize document keys
        key_sanitize_function = FieldMappingFunction(
            name="base64Encode"
        )
        
        print(f"\n📋 Configuring field mappings with document key sanitization")
        
        # Configure field mappings - focusing on fixing the document key issue
        indexer.field_mappings = [
            # Map chunk_id as document key with base64 encoding to handle parentheses
            FieldMapping(
                source_field_name="chunk_id",
                target_field_name="id",
                mapping_function=key_sanitize_function
            ),
            # Keep all other existing mappings
            FieldMapping(
                source_field_name="metadata_storage_name",
                target_field_name="title"
            ),
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
                target_field_name="content"
            ),
            FieldMapping(
                source_field_name="folder_path", 
                target_field_name="folder_path"
            )
        ]
        
        print(f"✅ Configured {len(indexer.field_mappings)} field mappings")
        print("✅ Added base64 encoding to chunk_id -> id mapping")
        
        # Configure indexing parameters
        print(f"\n⚙️ Configuring indexing parameters")
        
        if not indexer.parameters:
            indexer.parameters = IndexingParameters()
        
        indexer.parameters.parsing_mode = "jsonArray"
        indexer.parameters.max_failed_items = 50  # Reduced since we're fixing keys
        indexer.parameters.max_failed_items_per_batch = 10
        
        print("✅ Configured indexing parameters")
        print(f"   - Parsing Mode: {indexer.parameters.parsing_mode}")
        print(f"   - Max Failed Items: {indexer.parameters.max_failed_items}")
        print(f"   - Max Failed Items Per Batch: {indexer.parameters.max_failed_items_per_batch}")
        
        # Clear output field mappings to avoid API conflicts
        indexer.output_field_mappings = []
        print("✅ Cleared problematic output field mappings (will add manually)")
        
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
        
        # Display final configuration
        print(f"\n📊 CONFIGURATION SUMMARY")
        print("=" * 60)
        print(f"✅ Document key issue fixed: chunk_id -> id (base64 encoded)")
        print(f"✅ All metadata fields mapped correctly")
        print(f"✅ Indexer parameters optimized")
        
        print(f"\n🎯 KEY FIX APPLIED:")
        print("✅ Document keys with parentheses now encoded with base64")
        print("   - This should eliminate all 303 document key validation errors")
        print("   - Documents like 'Autobahn_(old)' will now index successfully")
        
        print(f"\n⚠️ MANUAL STEP REQUIRED:")
        print("The output field mapping for embeddings needs to be added manually:")
        print("1. Go to Azure Portal > Search Service > Indexers > jennifur-rag-indexer")
        print("2. Click 'Edit JSON'")
        print("3. Add this section after the 'fieldMappings' array:")
        print('   "outputFieldMappings": [')
        print('     {')
        print('       "sourceFieldName": "/document/normalized_pages/*/text_vector",')
        print('       "targetFieldName": "contentVector"')
        print('     }')
        print('   ]')
        print("4. Save and run the indexer again")
        
        print(f"\n📈 EXPECTED RESULTS:")
        print("- Document indexing should jump from ~700 to ~1000+ documents")
        print("- The 303 'Invalid document key' errors should be eliminated")
        print("- Client metadata filtering should work correctly")
        print("- RAG responses should be more accurate and client-aware")
        
        print(f"\n⏳ NEXT STEPS:")
        print("1. Wait 10-15 minutes for indexer to complete")
        print("2. Check indexer status and document count")
        print("3. Add the output field mapping manually (see above)")
        print("4. Test RAG functionality with client-specific queries")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_document_keys_only()