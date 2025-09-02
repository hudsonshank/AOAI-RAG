#!/usr/bin/env python3
"""
Analyze Azure Search Indexer Warnings and Errors
"""

import os
import json
from collections import defaultdict
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def analyze_indexer_issues():
    """Analyze indexer warnings and errors in detail"""
    
    # Initialize client
    indexer_client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    indexer_name = "jennifur-rag-indexer"
    
    try:
        # Get indexer status
        status = indexer_client.get_indexer_status(indexer_name)
        
        print(f"=== INDEXER STATUS: {indexer_name} ===\n")
        print(f"Current Status: {status.status}")
        
        if status.last_result:
            print(f"\n=== LAST RUN SUMMARY ===")
            print(f"Status: {status.last_result.status}")
            print(f"Start Time: {status.last_result.start_time}")
            print(f"End Time: {status.last_result.end_time}")
            print(f"Items Processed: {status.last_result.item_count - status.last_result.failed_item_count}")
            print(f"Items Failed: {status.last_result.failed_item_count}")
            print(f"Total Items: {status.last_result.item_count}")
            success_rate = ((status.last_result.item_count - status.last_result.failed_item_count) / 
                           status.last_result.item_count * 100) if status.last_result.item_count > 0 else 0
            print(f"Success Rate: {success_rate:.2f}%")
            
            # Analyze warnings
            if status.last_result.warnings:
                print(f"\n=== ⚠️ WARNINGS ANALYSIS ({len(status.last_result.warnings)}) ===")
                
                warning_types = defaultdict(list)
                for warning in status.last_result.warnings:
                    msg = warning.message if hasattr(warning, 'message') else str(warning)
                    key = warning.key if hasattr(warning, 'key') else 'unknown'
                    
                    # Categorize warnings
                    if "Could not map output field 'text_vector'" in msg:
                        warning_types['text_vector_mapping'].append(key)
                    elif "Could not parse document" in msg:
                        warning_types['parse_error'].append(key)
                    elif "Invalid document key" in msg:
                        warning_types['invalid_key'].append(key)
                    else:
                        warning_types['other'].append((key, msg[:100]))
                
                # Display warning summary
                if warning_types['text_vector_mapping']:
                    print(f"\n📍 Text Vector Mapping Issues: {len(warning_types['text_vector_mapping'])}")
                    print("   Problem: The embedding field 'text_vector' is not being mapped to the index")
                    print("   Impact: Documents won't have embeddings for semantic search")
                    print("   Solution: Add output field mapping: /document/normalized_pages/*/text_vector -> contentVector")
                    print(f"   Example affected docs: {', '.join(warning_types['text_vector_mapping'][:3])}")
                
                if warning_types['parse_error']:
                    print(f"\n📍 Document Parse Errors: {len(warning_types['parse_error'])}")
                    print("   Problem: Some documents couldn't be parsed")
                    print(f"   Example affected docs: {', '.join(warning_types['parse_error'][:3])}")
                
                if warning_types['invalid_key']:
                    print(f"\n📍 Invalid Document Keys: {len(warning_types['invalid_key'])}")
                    print("   Problem: Document keys contain invalid characters")
                    print(f"   Example affected docs: {', '.join(warning_types['invalid_key'][:3])}")
                
                if warning_types['other']:
                    print(f"\n📍 Other Warnings: {len(warning_types['other'])}")
                    for key, msg in warning_types['other'][:3]:
                        print(f"   - {key}: {msg}")
            
            # Analyze errors
            if status.last_result.errors:
                print(f"\n=== ❌ ERRORS ANALYSIS ({len(status.last_result.errors)}) ===")
                
                error_types = defaultdict(list)
                for error in status.last_result.errors:
                    msg = error.error_message if hasattr(error, 'error_message') else str(error)
                    key = error.key if hasattr(error, 'key') else 'unknown'
                    
                    # Categorize errors
                    if "Invalid document key" in msg:
                        # Extract the problematic part
                        if "Keys can only contain" in msg:
                            error_types['invalid_chars'].append(key)
                    elif "could not be mapped" in msg:
                        error_types['mapping_error'].append((key, msg[:100]))
                    else:
                        error_types['other'].append((key, msg[:100]))
                
                # Display error summary
                if error_types['invalid_chars']:
                    print(f"\n📍 Invalid Character in Keys: {len(error_types['invalid_chars'])}")
                    print("   Problem: Document keys contain parentheses or other invalid characters")
                    print("   Impact: These documents are not being indexed")
                    print("   Solution: Clean chunk_id values in blob storage or use a different key field")
                    
                    # Show examples
                    print("   Example problematic keys:")
                    for key in error_types['invalid_chars'][:5]:
                        print(f"     - {key}")
                
                if error_types['mapping_error']:
                    print(f"\n📍 Field Mapping Errors: {len(error_types['mapping_error'])}")
                    for key, msg in error_types['mapping_error'][:3]:
                        print(f"   - {key}: {msg}")
                
                if error_types['other']:
                    print(f"\n📍 Other Errors: {len(error_types['other'])}")
                    for key, msg in error_types['other'][:3]:
                        print(f"   - {key}: {msg}")
        
        # Get current configuration
        print(f"\n=== CURRENT INDEXER CONFIGURATION ===")
        indexer = indexer_client.get_indexer(indexer_name)
        
        print(f"Data Source: {indexer.data_source_name}")
        print(f"Target Index: {indexer.target_index_name}")
        print(f"Skillset: {indexer.skillset_name if indexer.skillset_name else 'None'}")
        
        if indexer.parameters:
            if hasattr(indexer.parameters, 'parsing_mode'):
                print(f"Parsing Mode: {indexer.parameters.parsing_mode}")
            if hasattr(indexer.parameters, 'max_failed_items'):
                print(f"Max Failed Items: {indexer.parameters.max_failed_items}")
        
        print(f"\nField Mappings: {len(indexer.field_mappings) if indexer.field_mappings else 0}")
        if indexer.field_mappings:
            for mapping in indexer.field_mappings[:5]:
                print(f"  - {mapping.source_field_name} -> {mapping.target_field_name}")
            if len(indexer.field_mappings) > 5:
                print(f"  ... and {len(indexer.field_mappings) - 5} more")
        
        print(f"\nOutput Field Mappings: {len(indexer.output_field_mappings) if indexer.output_field_mappings else 0}")
        if indexer.output_field_mappings:
            for mapping in indexer.output_field_mappings[:5]:
                print(f"  - {mapping.source_field_name} -> {mapping.target_field_name}")
        
        # Provide actionable recommendations
        print(f"\n=== 🎯 ACTION ITEMS ===")
        print("\n1. FIX OUTPUT FIELD MAPPING (Critical):")
        print("   - Go to Azure Portal > Search Service > Indexers > jennifur-rag-indexer")
        print("   - Click 'Edit JSON'")
        print("   - Add this to outputFieldMappings:")
        print('   "outputFieldMappings": [')
        print('     {')
        print('       "sourceFieldName": "/document/normalized_pages/*/text_vector",')
        print('       "targetFieldName": "contentVector"')
        print('     }')
        print('   ]')
        
        print("\n2. FIX INVALID DOCUMENT KEYS:")
        print("   - The chunk_id field contains parentheses which are invalid")
        print("   - Options:")
        print("     a) Clean the data in blob storage (remove parentheses from chunk_id)")
        print("     b) Use a different field as the document key")
        print("     c) Apply a base64 encoding function to the chunk_id field mapping")
        
        print("\n3. VERIFY FIELD MAPPINGS:")
        print("   - Ensure all metadata fields are mapped correctly")
        print("   - Current mappings look good for client_name, pm_initial, etc.")
        
        print("\n4. RESET AND RERUN:")
        print("   - After fixing the above, reset and rerun the indexer")
        print("   - Monitor for reduced warnings/errors")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_indexer_issues()