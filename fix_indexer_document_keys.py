#!/usr/bin/env python3
"""
Fix Azure Search Indexer Document Key Issues

This script addresses two critical problems:
1. Invalid document keys containing parentheses (303 failed items)
2. Missing output field mapping for embeddings

Solution approach:
- Use a function mapping to sanitize document keys during indexing
- Add proper output field mappings
- Configure optimal indexing parameters
"""

import os
import json
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping,
    OutputFieldMappingEntry,
    IndexingParameters,
    FieldMappingFunction
)
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def fix_indexer_document_keys():
    """Fix indexer configuration to handle invalid document keys and add embeddings"""
    
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
        
        # STEP 1: Configure field mappings with key sanitization
        print("\n📋 STEP 1: Configuring field mappings with key sanitization")
        
        # Create a mapping function to sanitize the document key
        # This will replace invalid characters in chunk_id with underscores
        key_sanitize_function = FieldMappingFunction(
            name="base64Encode"  # Use base64 encoding to handle all invalid chars
        )
        
        indexer.field_mappings = [
            # Map chunk_id as document key with sanitization
            FieldMapping(
                source_field_name="chunk_id",
                target_field_name="id",  # This becomes the document key
                mapping_function=key_sanitize_function
            ),
            # Map storage metadata
            FieldMapping(
                source_field_name="metadata_storage_name",
                target_field_name="title"
            ),
            # Map JSON document fields
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
        print("✅ Added base64 encoding function to sanitize document keys")
        
        # STEP 2: Configure output field mappings for embeddings
        print("\n🔗 STEP 2: Configuring output field mappings for embeddings")
        
        indexer.output_field_mappings = [
            OutputFieldMappingEntry(
                name="embedding_mapping",
                source_field_name="/document/normalized_pages/*/text_vector",
                target_field_name="contentVector"
            )
        ]
        
        print("✅ Added output field mapping for content embeddings")
        
        # STEP 3: Configure indexing parameters
        print("\n⚙️ STEP 3: Configuring indexing parameters")
        
        if not indexer.parameters:
            indexer.parameters = IndexingParameters()
        
        indexer.parameters.parsing_mode = "jsonArray"
        indexer.parameters.max_failed_items = 50  # Reduce this now that we're fixing the keys
        indexer.parameters.max_failed_items_per_batch = 10
        
        # Configure additional parameters for better performance
        indexer.parameters.configuration = {
            "indexedFileNameExtensions": ".json",
            "failOnUnsupportedContentType": False,
            "failOnUnprocessableDocument": False
        }
        
        print("✅ Configured indexing parameters")
        print(f"   - Parsing Mode: {indexer.parameters.parsing_mode}")
        print(f"   - Max Failed Items: {indexer.parameters.max_failed_items}")
        print(f"   - Max Failed Items Per Batch: {indexer.parameters.max_failed_items_per_batch}")
        
        # STEP 4: Update the indexer
        print("\n🚀 STEP 4: Updating indexer configuration")
        
        updated_indexer = indexer_client.create_or_update_indexer(indexer)
        print("✅ Indexer configuration updated successfully!")
        
        # STEP 5: Reset and run the indexer
        print("\n🔄 STEP 5: Resetting and running indexer")
        
        indexer_client.reset_indexer(indexer_name)
        print("✅ Indexer reset completed")
        
        indexer_client.run_indexer(indexer_name)
        print("✅ Indexer run started")
        
        # STEP 6: Display final configuration summary
        print("\n📊 FINAL CONFIGURATION SUMMARY")
        print("=" * 60)
        print(f"Indexer Name: {updated_indexer.name}")
        print(f"Data Source: {updated_indexer.data_source_name}")
        print(f"Target Index: {updated_indexer.target_index_name}")
        print(f"Skillset: {updated_indexer.skillset_name}")
        
        print(f"\nField Mappings ({len(updated_indexer.field_mappings)}):")
        for mapping in updated_indexer.field_mappings:
            func_info = f" (via {mapping.mapping_function.name})" if mapping.mapping_function else ""
            print(f"  ✓ {mapping.source_field_name} -> {mapping.target_field_name}{func_info}")
        
        print(f"\nOutput Field Mappings ({len(updated_indexer.output_field_mappings) if updated_indexer.output_field_mappings else 0}):")
        if updated_indexer.output_field_mappings:
            for mapping in updated_indexer.output_field_mappings:
                print(f"  ✓ {mapping.source_field_name} -> {mapping.target_field_name}")
        
        print(f"\n🎯 KEY FIXES APPLIED:")
        print("1. ✅ Document keys sanitized using base64 encoding")
        print("2. ✅ Output field mapping added for content embeddings")
        print("3. ✅ Optimal indexing parameters configured")
        print("4. ✅ All metadata fields properly mapped")
        
        print(f"\n⏳ Next Steps:")
        print("1. Monitor the indexer run for completion")
        print("2. Check indexer status in ~10-15 minutes")
        print("3. Verify that document count has increased significantly")
        print("4. Test RAG functionality with client-specific queries")
        
        print(f"\n💡 Expected Results:")
        print("- Document key validation errors should be eliminated")
        print("- All 303+ previously failed documents should now index successfully")  
        print("- Content embeddings will be available for semantic search")
        print("- Client metadata filtering should work correctly")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        print(f"\n🛠️ Troubleshooting:")
        print("1. Verify Azure Search endpoint and admin key in .env")
        print("2. Check that indexer 'jennifur-rag-indexer' exists")
        print("3. Ensure you have admin permissions on the search service")

if __name__ == "__main__":
    fix_indexer_document_keys()