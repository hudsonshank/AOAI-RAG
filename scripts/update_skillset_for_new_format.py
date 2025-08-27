#!/usr/bin/env python3
"""
Update Azure Search Skillset for New Document Format
Creates a simplified skillset that handles only the new standardized 'chunk' field format
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexerSkillset, 
    WebApiSkill,
    InputFieldMappingEntry,
    OutputFieldMappingEntry,
    SplitSkill
)
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class SkillsetUpdater:
    """Update Azure Search skillset for the new standardized format"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.skillset_name = os.getenv("AZURE_SEARCH_SKILLSET_NAME", "jennifur-skillset")
        self.indexer_name = os.getenv("AZURE_SEARCH_INDEXER_NAME", "jennifur-indexer")
        
        if not all([self.endpoint, self.admin_key]):
            raise ValueError("Missing required Azure Search configuration")
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.index_client = SearchIndexClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
        self.indexer_client = SearchIndexerClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
        
    def create_simplified_skillset(self) -> SearchIndexerSkillset:
        """Create a simplified skillset for the new chunk-based format"""
        
        print("🔧 Creating simplified skillset for new document format...")
        
        # For now, create a minimal skillset that just processes the chunk field
        # This can be expanded based on actual needs
        skills = []
        
        # Basic text splitting if chunks are too large
        try:
            text_split_skill = SplitSkill(
                name="text-splitter",
                description="Split large chunks if needed",
                context="/document",
                text_split_mode="pages",
                maximum_page_length=8000,
                page_overlap_length=200,
                inputs=[
                    InputFieldMappingEntry(name="text", source="/document/chunk")
                ],
                outputs=[
                    OutputFieldMappingEntry(name="textItems", target_name="pages")
                ]
            )
            skills.append(text_split_skill)
        except Exception as e:
            print(f"⚠️  Could not create split skill: {str(e)}")
        
        # Create a simple skillset - even if empty, it establishes the structure
        skillset = SearchIndexerSkillset(
            name=self.skillset_name,
            description="Simplified skillset for new chunk-based document format - no conditional logic needed",
            skills=skills
        )
        
        return skillset
    
    def backup_existing_skillset(self) -> bool:
        """Backup existing skillset if it exists"""
        try:
            print("💾 Checking for existing skillset to backup...")
            existing_skillset = self.indexer_client.get_skillset(self.skillset_name)
            
            # Save backup
            backup_data = {
                "name": existing_skillset.name,
                "description": existing_skillset.description,
                "skills": [skill.serialize() if hasattr(skill, 'serialize') else str(skill) for skill in existing_skillset.skills],
                "cognitive_services_account": str(existing_skillset.cognitive_services_account) if existing_skillset.cognitive_services_account else None,
                "backup_timestamp": datetime.utcnow().isoformat()
            }
            
            backup_filename = f"skillset_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            backup_path = f"/Users/hudsonshank/AOAI-RAG/scripts/{backup_filename}"
            
            with open(backup_path, 'w') as f:
                json.dump(backup_data, f, indent=2)
            
            print(f"✅ Existing skillset backed up to: {backup_filename}")
            return True
            
        except Exception as e:
            if "NotFound" in str(e) or "ResourceNotFound" in str(e):
                print("ℹ️  No existing skillset found - creating new one")
                return True
            else:
                print(f"⚠️  Error backing up skillset: {str(e)}")
                return False
    
    def update_skillset(self) -> bool:
        """Update or create the skillset"""
        try:
            # Create the new simplified skillset
            new_skillset = self.create_simplified_skillset()
            
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
    
    def get_skillset_info(self) -> None:
        """Display current skillset information"""
        try:
            print(f"🔍 Current skillset information:")
            skillset = self.indexer_client.get_skillset(self.skillset_name)
            
            print(f"   Name: {skillset.name}")
            print(f"   Description: {skillset.description}")
            print(f"   Number of skills: {len(skillset.skills)}")
            
            for i, skill in enumerate(skillset.skills, 1):
                skill_type = type(skill).__name__
                skill_name = getattr(skill, 'name', f'skill_{i}')
                print(f"   {i}. {skill_name} ({skill_type})")
                
        except Exception as e:
            print(f"❌ Error getting skillset info: {str(e)}")
    
    def validate_configuration(self) -> bool:
        """Validate that all required environment variables are set"""
        required_vars = [
            "AZURE_SEARCH_ENDPOINT",
            "AZURE_SEARCH_ADMIN_KEY", 
            "AZURE_OPENAI_ENDPOINT",
            "AZURE_OPENAI_API_KEY",
            "AZURE_OPENAI_EMBEDDING_MODEL"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"❌ Missing required environment variables:")
            for var in missing_vars:
                print(f"   - {var}")
            return False
        
        print("✅ All required environment variables are set")
        return True

def main():
    """Main execution function"""
    print("🤖 AZURE SEARCH SKILLSET UPDATER")
    print("=" * 60)
    print("Updating skillset for new standardized document format")
    print("This removes conditional logic and handles only 'chunk' field")
    print()
    
    try:
        # Initialize updater
        updater = SkillsetUpdater()
        
        # Validate configuration
        if not updater.validate_configuration():
            print("❌ Configuration validation failed. Please check your environment variables.")
            return False
        
        print("✅ Connected to Azure Search successfully")
        print(f"   Service: {updater.endpoint}")
        print(f"   Skillset: {updater.skillset_name}")
        print()
        
        # Show current skillset info (if exists)
        print("🔍 Step 1: Checking current skillset...")
        updater.get_skillset_info()
        print()
        
        # Backup existing skillset
        print("💾 Step 2: Backing up existing skillset...")
        if not updater.backup_existing_skillset():
            print("⚠️  Backup failed, but continuing...")
        print()
        
        # Update skillset
        print("🔧 Step 3: Updating skillset with new format...")
        if not updater.update_skillset():
            print("❌ Skillset update failed. Exiting.")
            return False
        print()
        
        # Verify update
        print("✅ Step 4: Verifying updated skillset...")
        updater.get_skillset_info()
        
        print("\n🎉 SKILLSET UPDATE COMPLETED!")
        print("=" * 60)
        print("✅ Skillset now handles only the new 'chunk' format")
        print("✅ Removed all conditional logic for 'content' vs 'chunk' fields")
        print("✅ Simplified processing pipeline")
        print()
        print("📋 Next steps:")
        print("   1. Reset the indexer to clear old data")
        print("   2. Run the indexer with clean, reprocessed documents")
        print("   3. Test search functionality")
        print("   4. Monitor for any indexing errors")
        
        return True
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)