#!/usr/bin/env python3
"""
Check Azure Search Indexer Warnings and Errors
"""

import os
import json
from datetime import datetime
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def check_indexer_warnings():
    """Check the indexer status, warnings, and errors"""
    
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
        print(f"Overall Status: {status.status}")
        
        if status.last_result:
            print(f"\n=== LAST RUN SUMMARY ===")
            print(f"Status: {status.last_result.status}")
            print(f"Start Time: {status.last_result.start_time}")
            print(f"End Time: {status.last_result.end_time}")
            print(f"Items Processed: {status.last_result.items_processed}")
            print(f"Items Failed: {status.last_result.items_failed}")
            
            # Show warnings
            if status.last_result.warnings:
                print(f"\n=== ⚠️ WARNINGS ({len(status.last_result.warnings)}) ===")
                
                # Group warnings by type
                warning_types = {}
                for warning in status.last_result.warnings:
                    warning_msg = warning.message if hasattr(warning, 'message') else str(warning)
                    warning_key = warning.key if hasattr(warning, 'key') else 'unknown'
                    
                    # Extract warning type from message
                    if "Could not map output field" in warning_msg:
                        warning_type = "Output Field Mapping"
                    elif "Could not parse document" in warning_msg:
                        warning_type = "Document Parsing"
                    elif "Invalid document key" in warning_msg:
                        warning_type = "Invalid Document Key"
                    else:
                        warning_type = "Other"
                    
                    if warning_type not in warning_types:
                        warning_types[warning_type] = []
                    warning_types[warning_type].append({
                        'key': warning_key,
                        'message': warning_msg
                    })
                
                # Display grouped warnings
                for warning_type, warnings in warning_types.items():
                    print(f"\n{warning_type} ({len(warnings)} warnings):")
                    # Show first 5 examples of each type
                    for i, w in enumerate(warnings[:5], 1):
                        print(f"  {i}. Key: {w['key']}")
                        print(f"     Message: {w['message'][:200]}...")
                    if len(warnings) > 5:
                        print(f"  ... and {len(warnings) - 5} more {warning_type} warnings")
            
            # Show errors
            if status.last_result.errors:
                print(f"\n=== ❌ ERRORS ({len(status.last_result.errors)}) ===")
                
                # Group errors by type
                error_types = {}
                for error in status.last_result.errors[:50]:  # Analyze first 50 errors
                    error_msg = error.error_message if hasattr(error, 'error_message') else str(error)
                    error_key = error.key if hasattr(error, 'key') else 'unknown'
                    
                    # Extract error type
                    if "Invalid document key" in error_msg:
                        error_type = "Invalid Document Key"
                    elif "could not be mapped" in error_msg:
                        error_type = "Field Mapping Error"
                    else:
                        error_type = "Other"
                    
                    if error_type not in error_types:
                        error_types[error_type] = []
                    error_types[error_type].append({
                        'key': error_key,
                        'message': error_msg
                    })
                
                # Display grouped errors
                for error_type, errors in error_types.items():
                    print(f"\n{error_type} ({len(errors)} errors):")
                    for i, e in enumerate(errors[:3], 1):
                        print(f"  {i}. Key: {e['key']}")
                        print(f"     Message: {e['message'][:300]}...")
                    if len(errors) > 3:
                        print(f"  ... and {len(errors) - 3} more {error_type} errors")
        
        # Get current indexer configuration
        print(f"\n=== CURRENT CONFIGURATION ===")
        indexer = indexer_client.get_indexer(indexer_name)
        
        print(f"Data Source: {indexer.data_source_name}")
        print(f"Target Index: {indexer.target_index_name}")
        print(f"Skillset: {indexer.skillset_name}")
        print(f"Parsing Mode: {indexer.parameters.parsing_mode if indexer.parameters else 'default'}")
        
        # Check field mappings
        print(f"\nField Mappings: {len(indexer.field_mappings) if indexer.field_mappings else 0}")
        if indexer.field_mappings:
            for mapping in indexer.field_mappings[:10]:
                print(f"  - {mapping.source_field_name} -> {mapping.target_field_name}")
        
        # Check output field mappings
        print(f"\nOutput Field Mappings: {len(indexer.output_field_mappings) if indexer.output_field_mappings else 0}")
        if indexer.output_field_mappings:
            for mapping in indexer.output_field_mappings[:10]:
                print(f"  - {mapping.source_field_name} -> {mapping.target_field_name}")
        
        # Analyze the problems
        print(f"\n=== 🔍 PROBLEM ANALYSIS ===")
        
        if status.last_result:
            total_items = status.last_result.items_processed + status.last_result.items_failed
            success_rate = (status.last_result.items_processed / total_items * 100) if total_items > 0 else 0
            
            print(f"Success Rate: {success_rate:.1f}% ({status.last_result.items_processed}/{total_items})")
            
            if status.last_result.warnings:
                # Check for output field mapping issues
                output_mapping_warnings = [w for w in status.last_result.warnings 
                                          if "output field" in str(w).lower()]
                if output_mapping_warnings:
                    print(f"\n⚠️ Output field mapping issues detected: {len(output_mapping_warnings)} warnings")
                    print("  This means the embedding vectors are not being mapped correctly")
                
                # Check for document key issues
                key_warnings = [w for w in status.last_result.warnings 
                               if "document key" in str(w).lower()]
                if key_warnings:
                    print(f"\n⚠️ Document key issues detected: {len(key_warnings)} warnings")
                    print("  This means some documents have invalid characters in their keys")
        
        print("\n=== 📋 RECOMMENDATIONS ===")
        print("1. Add output field mapping for text_vector -> contentVector")
        print("2. Fix document keys with invalid characters (parentheses, spaces)")
        print("3. Ensure JSON parsing mode is set correctly")
        print("4. Verify field mappings match your JSON structure")
        
    except Exception as e:
        print(f"Error checking indexer: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_indexer_warnings()