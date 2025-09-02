#!/usr/bin/env python3
"""
Simplified fix for Azure Search Indexer Configuration
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
    """Fix the indexer configuration with proper API structure"""
    
    # Initialize client
    indexer_client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    indexer_name = "jennifur-rag-indexer"
    
    try:
        # Get current indexer
        indexer = indexer_client.get_indexer(indexer_name)
        print(f"Current indexer configuration retrieved")
        
        # Configure field mappings
        print("\n=== CONFIGURING FIELD MAPPINGS ===")
        
        # Set up field mappings without the chunk_id problematic mapping first
        indexer.field_mappings = [
            # Map storage metadata to title
            FieldMapping(
                source_field_name="metadata_storage_name",
                target_field_name="title"
            ),
            # Map the JSON fields directly
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
        
        print(f"Configured {len(indexer.field_mappings)} field mappings")
        
        # Configure parameters
        if not indexer.parameters:
            indexer.parameters = IndexingParameters()
        
        indexer.parameters.parsing_mode = "jsonArray"
        indexer.parameters.max_failed_items = 1000
        indexer.parameters.max_failed_items_per_batch = 100
        
        print("\n=== PARAMETERS CONFIGURED ===")
        print(f"Parsing Mode: {indexer.parameters.parsing_mode}")
        print(f"Max Failed Items: {indexer.parameters.max_failed_items}")
        print(f"Max Failed Items Per Batch: {indexer.parameters.max_failed_items_per_batch}")
        
        # IMPORTANT: We'll handle the output field mappings through Azure Portal
        # since the SDK has issues with the OutputFieldMappingEntry structure
        print("\n=== OUTPUT FIELD MAPPINGS ===")
        print("NOTE: You'll need to add the following output field mapping in Azure Portal:")
        print("  Source: /document/normalized_pages/*/text_vector")
        print("  Target: contentVector")
        
        # Update the indexer
        print("\n=== UPDATING INDEXER ===")
        updated_indexer = indexer_client.create_or_update_indexer(indexer)
        print("✅ Indexer updated successfully!")
        
        # Print final configuration
        print("\n=== FINAL CONFIGURATION ===")
        print(f"Field Mappings: {len(updated_indexer.field_mappings)}")
        for mapping in updated_indexer.field_mappings:
            print(f"  - {mapping.source_field_name} -> {mapping.target_field_name}")
        
        # Reset and run the indexer
        print("\n=== RESETTING AND RUNNING INDEXER ===")
        indexer_client.reset_indexer(indexer_name)
        print("Indexer reset completed")
        
        indexer_client.run_indexer(indexer_name)
        print("Indexer run started")
        
        print("\n✅ Configuration partially fixed!")
        print("\n⚠️ IMPORTANT MANUAL STEP REQUIRED:")
        print("1. Go to Azure Portal > Search Service > Indexers > jennifur-rag-indexer")
        print("2. Click 'Edit JSON' in the top menu")
        print("3. Add this to the 'outputFieldMappings' section:")
        print("""
    "outputFieldMappings": [
        {
            "sourceFieldName": "/document/normalized_pages/*/text_vector",
            "targetFieldName": "contentVector"
        }
    ]
        """)
        print("4. Save and run the indexer again")
        print("\nThe chunk_id issue with parentheses will need to be handled by:")
        print("- Either cleaning the data in blob storage to remove parentheses")
        print("- Or using a different field as the document key")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_indexer_configuration()