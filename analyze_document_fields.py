#!/usr/bin/env python3
"""
Analyze Document Field Structure

Check what fields are actually available across different documents
to understand why 'chunk' field is missing in some cases.
"""

import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

def analyze_document_fields():
    """Analyze field structure across a sample of documents"""
    
    print("🔍 ANALYZING DOCUMENT FIELD STRUCTURE")
    print("=" * 60)
    
    try:
        blob_service = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )
        container_client = blob_service.get_container_client("jennifur-processed")
        
        # Sample 100 documents to analyze field patterns
        field_counts = defaultdict(int)
        content_field_stats = {
            'chunk': 0,
            'content': 0, 
            'both': 0,
            'neither': 0,
            'chunk_empty': 0,
            'content_empty': 0
        }
        
        sample_size = 100
        processed = 0
        
        print(f"📊 Analyzing first {sample_size} documents...")
        
        for blob in container_client.list_blobs():
            if processed >= sample_size:
                break
                
            if blob.name.endswith('.json'):
                try:
                    blob_client = container_client.get_blob_client(blob.name)
                    content = blob_client.download_blob().readall().decode('utf-8')
                    data = json.loads(content)
                    
                    # Count all fields
                    for field in data.keys():
                        field_counts[field] += 1
                    
                    # Analyze content fields specifically
                    has_chunk = 'chunk' in data and data['chunk'] and str(data['chunk']).strip()
                    has_content = 'content' in data and data['content'] and str(data['content']).strip()
                    
                    if has_chunk and has_content:
                        content_field_stats['both'] += 1
                    elif has_chunk:
                        content_field_stats['chunk'] += 1
                    elif has_content:
                        content_field_stats['content'] += 1
                    else:
                        content_field_stats['neither'] += 1
                    
                    # Check for empty fields
                    if 'chunk' in data and not str(data['chunk']).strip():
                        content_field_stats['chunk_empty'] += 1
                    if 'content' in data and not str(data['content']).strip():
                        content_field_stats['content_empty'] += 1
                    
                    processed += 1
                    
                except Exception as e:
                    print(f"Error processing {blob.name}: {str(e)}")
                    continue
        
        # Results
        print(f"\n📊 FIELD ANALYSIS RESULTS ({processed} documents):")
        print("=" * 50)
        
        print(f"\n🔤 MOST COMMON FIELDS:")
        sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
        for field, count in sorted_fields[:15]:
            percentage = (count / processed) * 100
            print(f"  {field}: {count}/{processed} ({percentage:.1f}%)")
        
        print(f"\n📝 CONTENT FIELD ANALYSIS:")
        for stat, count in content_field_stats.items():
            percentage = (count / processed) * 100
            print(f"  {stat}: {count}/{processed} ({percentage:.1f}%)")
        
        # Determine best strategy
        print(f"\n💡 RECOMMENDED SKILLSET STRATEGY:")
        
        chunk_available = content_field_stats['chunk'] + content_field_stats['both']
        content_available = content_field_stats['content'] + content_field_stats['both']
        
        if chunk_available > processed * 0.8:
            print("✅ Use 'chunk' field directly (80%+ coverage)")
            print("   Most documents have chunk field with content")
        elif content_available > processed * 0.8:
            print("✅ Use 'content' field directly (80%+ coverage)")  
            print("   Most documents have content field with content")
        else:
            print("⚠️ Need ConditionalSkill or multiple field approach")
            print("   Documents have mixed field structures")
            
            # Show the conditional skill configuration
            print(f"\n🔧 MULTI-FIELD SOLUTION:")
            multi_field_solution = '''
Use a ConditionalSkill that tries multiple fields in order:

{
  "@odata.type": "#Microsoft.Skills.Util.ConditionalSkill",
  "inputs": [
    {
      "name": "condition",
      "source": "/document/chunk"
    },
    {
      "name": "whenTrue",
      "source": "/document/chunk"
    },
    {
      "name": "whenFalse",
      "source": "/document/content"
    }
  ],
  "outputs": [
    {
      "name": "output",
      "targetName": "text_content"
    }
  ]
}

OR create a custom merge skill that tries multiple sources:

{
  "@odata.type": "#Microsoft.Skills.Util.DocumentExtractionSkill",
  "inputs": [
    {
      "name": "text",
      "source": "= coalesce(/document/chunk, /document/content, '')"
    }
  ]
}
'''
            print(multi_field_solution)
            
    except Exception as e:
        print(f"❌ Error analyzing documents: {str(e)}")

if __name__ == "__main__":
    analyze_document_fields()