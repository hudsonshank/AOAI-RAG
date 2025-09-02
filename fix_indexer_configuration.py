#!/usr/bin/env python3
"""
Fix Azure Search Indexer Configuration Issues
"""

import os
import json
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping,
    OutputFieldMappingEntry,
    IndexingParameters
)
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def fix_indexer_configuration():
    """Fix the indexer configuration issues"""
    
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
        
        # Fix 1: Add output field mapping for text_vector -> contentVector
        print("\n=== FIXING OUTPUT FIELD MAPPINGS ===")
        
        # Initialize output field mappings if not exists
        if not indexer.output_field_mappings:
            indexer.output_field_mappings = []
        
        # Check if text_vector mapping already exists
        has_vector_mapping = any(
            m.source_field_name == "/document/normalized_pages/*/text_vector" 
            for m in indexer.output_field_mappings
        )
        
        if not has_vector_mapping:
            # Add the missing output field mapping
            vector_mapping = OutputFieldMappingEntry(
                name="text_vector",
                source_field_name="/document/normalized_pages/*/text_vector",
                target_field_name="contentVector"
            )
            indexer.output_field_mappings.append(vector_mapping)
            print("Added output field mapping: text_vector -> contentVector")
        else:
            print("Output field mapping for text_vector already exists")
        
        # Fix 2: Configure field mappings for metadata
        print("\n=== CONFIGURING FIELD MAPPINGS ===")
        
        # Clear existing field mappings and set up correct ones
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
            ),
            # Fix the chunk_id to handle parentheses issue
            FieldMapping(
                source_field_name="chunk_id",
                target_field_name="chunk_id",
                mapping_function={
                    "name": "base64Encode",
                    "parameters": {
                        "useHttpServerUtilityUrlTokenEncode": True
                    }
                }
            )
        ]
        
        print("Field mappings configured with base64 encoding for chunk_id")
        
        # Fix 3: Ensure parameters are set correctly
        if not indexer.parameters:
            indexer.parameters = IndexingParameters()
        
        indexer.parameters.parsing_mode = "jsonArray"
        indexer.parameters.max_failed_items = 100
        indexer.parameters.max_failed_items_per_batch = 10
        
        print("\n=== PARAMETERS CONFIGURED ===")
        print(f"Parsing Mode: {indexer.parameters.parsing_mode}")
        print(f"Max Failed Items: {indexer.parameters.max_failed_items}")
        print(f"Max Failed Items Per Batch: {indexer.parameters.max_failed_items_per_batch}")
        
        # Update the indexer
        print("\n=== UPDATING INDEXER ===")
        updated_indexer = indexer_client.create_or_update_indexer(indexer)
        print("✅ Indexer updated successfully!")
        
        # Print final configuration
        print("\n=== FINAL CONFIGURATION ===")
        print(f"Field Mappings: {len(updated_indexer.field_mappings)}")
        for mapping in updated_indexer.field_mappings:
            print(f"  - {mapping.source_field_name} -> {mapping.target_field_name}")
            if hasattr(mapping, 'mapping_function') and mapping.mapping_function:
                print(f"    (with function: {mapping.mapping_function.get('name', 'unknown')})")
        
        print(f"\nOutput Field Mappings: {len(updated_indexer.output_field_mappings)}")
        for mapping in updated_indexer.output_field_mappings:
            print(f"  - {mapping.source_field_name} -> {mapping.target_field_name}")
        
        # Reset and run the indexer
        print("\n=== RESETTING AND RUNNING INDEXER ===")
        indexer_client.reset_indexer(indexer_name)
        print("Indexer reset completed")
        
        indexer_client.run_indexer(indexer_name)
        print("Indexer run started")
        
        print("\n✅ Configuration fixed and indexer restarted!")
        print("Check the Azure Portal in a few minutes to monitor progress.")
        
    except Exception as e:
        print(f"❌ Error fixing indexer: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_indexer_configuration()