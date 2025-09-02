#!/usr/bin/env python3
"""
Fix Skillset Input Issues

The skillset is getting 400 warnings because the conditional skill inputs
are not properly configured for the JSON document structure.
"""

import os
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexerSkillset,
    ConditionalSkill,
    SplitSkill,
    AzureOpenAIEmbeddingSkill,
    InputFieldMappingEntry,
    OutputFieldMappingEntry
)
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def fix_skillset_inputs():
    """Fix the skillset to properly handle JSON document inputs"""
    
    client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    skillset_name = "jennifur-rag-skillset"
    
    print("🔧 FIXING SKILLSET INPUT ISSUES")
    print("=" * 60)
    
    try:
        # Get the current skillset
        skillset = client.get_skillset(skillset_name)
        print("✅ Retrieved current skillset")
        
        print(f"\n📋 ISSUE ANALYSIS:")
        print("400 warnings: 'required skill input is missing or empty'")
        print("Problem: Conditional skill expecting /document/normalizedContent")
        print("Reality: JSON docs have 'chunk' field with text content")
        
        # Create updated skills
        print(f"\n🛠️ CREATING FIXED SKILLS:")
        
        # Skill 1: Conditional skill to get text content
        # This checks if 'chunk' field exists, otherwise fallback to 'content'
        conditional_skill = ConditionalSkill(
            inputs=[
                InputFieldMappingEntry(name="condition", source="= $(/document/chunk)"),
                InputFieldMappingEntry(name="whenTrue", source="/document/chunk"), 
                InputFieldMappingEntry(name="whenFalse", source="/document/content")
            ],
            outputs=[
                OutputFieldMappingEntry(name="output", target_name="normalizedContent")
            ]
        )
        
        # Skill 2: Split text into pages/chunks for processing
        split_skill = SplitSkill(
            text_split_mode="pages",
            maximum_page_length=2000,
            page_overlap_length=200,
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/normalizedContent")
            ],
            outputs=[
                OutputFieldMappingEntry(name="textItems", target_name="pages")
            ]
        )
        
        # Skill 3: Generate embeddings for each page/chunk
        embedding_skill = AzureOpenAIEmbeddingSkill(
            resource_uri=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            deployment_id="text-embedding-ada-002",  # or your embedding model deployment
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/pages/*")
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="text_vector")
            ]
        )
        
        print("✅ Skill 1: Conditional skill (chunk or content -> normalizedContent)")
        print("✅ Skill 2: Split skill (normalizedContent -> pages)")  
        print("✅ Skill 3: Embedding skill (pages -> text_vector)")
        
        # Update the skillset
        skillset.skills = [conditional_skill, split_skill, embedding_skill]
        
        # Update the skillset
        print(f"\n🚀 UPDATING SKILLSET:")
        updated_skillset = client.create_or_update_skillset(skillset)
        print("✅ Skillset updated successfully!")
        
        print(f"\n📊 EXPECTED IMPROVEMENTS:")
        print("- 400 skill input warnings should be eliminated")
        print("- Text content will be properly extracted from 'chunk' field")
        print("- Embeddings will be generated for all document chunks")
        print("- Documents will be split into optimal search-sized chunks")
        
        # Reset and run the indexer
        print(f"\n🔄 RESETTING AND RUNNING INDEXER:")
        client.reset_indexer("jennifur-rag-indexer")
        client.run_indexer("jennifur-rag-indexer")
        print("✅ Indexer restarted with fixed skillset")
        
        print(f"\n🎯 NEXT STEPS:")
        print("1. Monitor indexer run - should process all 88k docs without skill warnings")
        print("2. Add output field mapping manually:")
        print("   sourceFieldName: '/document/pages/*/text_vector'")
        print("   targetFieldName: 'text_vector'")
        print("3. Test RAG system - should now have 30+ clients with embeddings")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        
        # Alternative approach if skillset update fails
        print(f"\n🔧 ALTERNATIVE APPROACH:")
        print("If skillset update failed, try this manual fix:")
        print("1. Go to Azure Portal > Search Service > Skillsets")
        print("2. Edit jennifur-rag-skillset JSON")
        print("3. Ensure conditional skill condition is: '= $(/document/chunk)'")
        print("4. Ensure whenTrue source is: '/document/chunk'")
        print("5. Ensure whenFalse source is: '/document/content'")

if __name__ == "__main__":
    fix_skillset_inputs()