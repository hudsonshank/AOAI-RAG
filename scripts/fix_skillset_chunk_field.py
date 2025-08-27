#!/usr/bin/env python3
"""
Fix Skillset to Handle Chunk Field Properly
Updates the skillset to properly handle the chunk field and avoid warnings
"""

import os
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexerSkillset, 
    InputFieldMappingEntry,
    OutputFieldMappingEntry,
    SplitSkill,
    ConditionalSkill
)
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class SkillsetFixer:
    """Fix skillset to handle chunk field without warnings"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.skillset_name = "jennifur-skillset"
        
        if not all([self.endpoint, self.admin_key]):
            raise ValueError("Missing required Azure Search configuration")
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.indexer_client = SearchIndexerClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
    
    def create_robust_skillset(self) -> SearchIndexerSkillset:
        """Create a skillset that handles chunk field robustly"""
        
        print("🔧 Creating robust skillset that handles chunk field properly...")
        
        skills = []
        
        # 1. Conditional skill to check if chunk field exists and is not empty
        chunk_validator = ConditionalSkill(
            name="chunk-validator",
            description="Validate that chunk field exists and is not empty",
            context="/document",
            inputs=[
                InputFieldMappingEntry(name="chunk", source="/document/chunk")
            ],
            outputs=[
                OutputFieldMappingEntry(name="validChunk", target_name="validated_chunk")
            ],
            condition="$(/document/chunk) != null && len($(/document/chunk)) > 0",
            when_true={
                "validChunk": "/document/chunk"
            },
            when_false={
                "validChunk": ""
            }
        )
        skills.append(chunk_validator)
        
        # 2. Text splitting skill that only operates on validated chunks
        text_split_skill = SplitSkill(
            name="text-splitter",
            description="Split validated chunks into smaller pieces if needed",
            context="/document",
            text_split_mode="pages",
            maximum_page_length=8000,
            page_overlap_length=200,
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/validated_chunk")
            ],
            outputs=[
                OutputFieldMappingEntry(name="textItems", target_name="pages")
            ]
        )
        skills.append(text_split_skill)
        
        # Create the skillset
        skillset = SearchIndexerSkillset(
            name=self.skillset_name,
            description="Robust skillset that handles chunk field validation to prevent warnings",
            skills=skills
        )
        
        return skillset
    
    def create_simple_no_split_skillset(self) -> SearchIndexerSkillset:
        """Create a minimal skillset that doesn't process the chunk field at all"""
        
        print("🔧 Creating minimal skillset that avoids chunk field processing...")
        
        # Create an empty skillset - just pass through the data without processing
        skillset = SearchIndexerSkillset(
            name=self.skillset_name,
            description="Minimal skillset that passes through data without chunk processing to avoid warnings",
            skills=[]  # No skills - just pass through
        )
        
        return skillset
    
    def update_skillset(self, use_minimal: bool = True) -> bool:
        """Update the skillset"""
        try:
            if use_minimal:
                new_skillset = self.create_simple_no_split_skillset()
            else:
                new_skillset = self.create_robust_skillset()
            
            print(f"📤 Updating skillset '{self.skillset_name}'...")
            result = self.indexer_client.create_or_update_skillset(new_skillset)
            
            print(f"✅ Skillset updated successfully!")
            print(f"   Name: {result.name}")
            print(f"   Skills: {len(result.skills)}")
            print(f"   Description: {result.description}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error updating skillset: {str(e)}")
            return False

def main():
    """Main execution function"""
    print("🤖 SKILLSET CHUNK FIELD FIX")
    print("=" * 50)
    print("Fixing skillset to handle chunk field without warnings")
    print()
    
    try:
        fixer = SkillsetFixer()
        
        print(f"✅ Connected to Azure Search: {fixer.endpoint}")
        print(f"   Skillset: {fixer.skillset_name}")
        print()
        
        # Try minimal approach first (no chunk processing)
        print("🔧 Approach 1: Minimal skillset (no chunk processing)")
        if fixer.update_skillset(use_minimal=True):
            print("✅ Minimal skillset created - this should eliminate warnings")
            print()
            print("💡 The documents already have proper chunk content and client metadata.")
            print("   The indexer can now process them without additional skillset processing.")
        else:
            print("❌ Minimal skillset update failed")
            
            # Try robust approach
            print("\n🔧 Approach 2: Robust skillset (with validation)")
            if fixer.update_skillset(use_minimal=False):
                print("✅ Robust skillset created with chunk validation")
            else:
                print("❌ Both approaches failed")
                return False
        
        print("\n🎉 SKILLSET FIXED!")
        print("Ready to reset and run indexer without chunk field warnings.")
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()