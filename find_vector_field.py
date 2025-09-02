#!/usr/bin/env python3
"""
Find the correct vector field name in the Azure Search index
"""

import os
from azure.search.documents.indexes import SearchIndexClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def find_vector_field():
    """Find vector fields in the index"""
    
    # Initialize client
    index_client = SearchIndexClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    index_name = "jennifur-rag"
    
    try:
        # Get index definition
        index = index_client.get_index(index_name)
        
        print(f"=== SEARCHING FOR VECTOR FIELDS IN INDEX: {index_name} ===\n")
        
        # Check all fields
        vector_fields = []
        text_fields = []
        other_relevant_fields = []
        
        for field in index.fields:
            field_name = field.name
            field_type = str(field.type)
            
            # Check for vector fields
            if hasattr(field, 'vector_search_dimensions') and field.vector_search_dimensions:
                vector_fields.append({
                    'name': field_name,
                    'type': field_type,
                    'dimensions': field.vector_search_dimensions
                })
            
            # Check for potential vector field names
            elif any(term in field_name.lower() for term in ['vector', 'embedding', 'embed']):
                text_fields.append({
                    'name': field_name,
                    'type': field_type
                })
            
            # Check for text_vector field specifically
            elif field_name == 'text_vector':
                other_relevant_fields.append({
                    'name': field_name,
                    'type': field_type,
                    'searchable': field.searchable if hasattr(field, 'searchable') else False
                })
        
        # Display findings
        if vector_fields:
            print("✅ VECTOR FIELDS FOUND:")
            for vf in vector_fields:
                print(f"  Field Name: '{vf['name']}'")
                print(f"  Type: {vf['type']}")
                print(f"  Dimensions: {vf['dimensions']}")
                print()
                
            print("📝 USE THIS IN OUTPUT FIELD MAPPING:")
            for vf in vector_fields:
                print(f'"outputFieldMappings": [')
                print(f'  {{')
                print(f'    "sourceFieldName": "/document/normalized_pages/*/text_vector",')
                print(f'    "targetFieldName": "{vf["name"]}"')
                print(f'  }}')
                print(f']')
                break  # Show first vector field
        else:
            print("❌ NO VECTOR FIELDS FOUND IN INDEX!")
            
            if text_fields:
                print("\n🔍 Fields with 'vector' or 'embedding' in name (but not vector type):")
                for tf in text_fields:
                    print(f"  - {tf['name']}: {tf['type']}")
            
            if other_relevant_fields:
                print("\n🔍 Other relevant fields found:")
                for of in other_relevant_fields:
                    print(f"  - {of['name']}: {of['type']} (searchable: {of.get('searchable', 'unknown')})")
            
            print("\n⚠️ PROBLEM: The index doesn't have a vector field configured!")
            print("This means:")
            print("1. The index was created without a vector field")
            print("2. OR the vector field has a different name than expected")
            print("\nYou may need to:")
            print("1. Add a vector field to the index")
            print("2. OR skip the output field mapping for vectors if semantic search isn't needed")
        
        # List all fields for reference
        print("\n=== ALL INDEX FIELDS ===")
        for field in index.fields[:20]:  # Show first 20 fields
            print(f"  - {field.name}: {field.type}")
        
        if len(index.fields) > 20:
            print(f"  ... and {len(index.fields) - 20} more fields")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_vector_field()