#!/usr/bin/env python3
"""
List Azure Search Resources
Shows all available indexes, indexers, skillsets, and data sources
"""

import os
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class ResourceLister:
    """List all Azure Search resources"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        
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
    
    def list_indexes(self):
        """List all search indexes"""
        print("📊 SEARCH INDEXES")
        print("-" * 30)
        try:
            indexes = list(self.index_client.list_indexes())
            if indexes:
                for i, index in enumerate(indexes, 1):
                    print(f"{i}. {index.name}")
                    print(f"   Fields: {len(index.fields)}")
                    print(f"   Description: {getattr(index, 'description', 'N/A') or 'N/A'}")
            else:
                print("No indexes found")
        except Exception as e:
            print(f"Error listing indexes: {str(e)}")
        print()
    
    def list_indexers(self):
        """List all indexers"""
        print("⚙️  INDEXERS")
        print("-" * 30)
        try:
            indexers = list(self.indexer_client.get_indexers())
            if indexers:
                for i, indexer in enumerate(indexers, 1):
                    print(f"{i}. {indexer.name}")
                    print(f"   Target Index: {indexer.target_index_name}")
                    print(f"   Data Source: {indexer.data_source_name}")
                    print(f"   Skillset: {indexer.skillset_name or 'None'}")
                    print(f"   Description: {indexer.description or 'N/A'}")
                    print()
            else:
                print("No indexers found")
        except Exception as e:
            print(f"Error listing indexers: {str(e)}")
        print()
    
    def list_skillsets(self):
        """List all skillsets"""
        print("🧠 SKILLSETS")
        print("-" * 30)
        try:
            skillsets = list(self.indexer_client.get_skillsets())
            if skillsets:
                for i, skillset in enumerate(skillsets, 1):
                    print(f"{i}. {skillset.name}")
                    print(f"   Skills: {len(skillset.skills)}")
                    print(f"   Description: {skillset.description or 'N/A'}")
                    for j, skill in enumerate(skillset.skills, 1):
                        skill_type = type(skill).__name__
                        skill_name = getattr(skill, 'name', f'skill_{j}')
                        print(f"     {j}. {skill_name} ({skill_type})")
                    print()
            else:
                print("No skillsets found")
        except Exception as e:
            print(f"Error listing skillsets: {str(e)}")
        print()
    
    def list_data_sources(self):
        """List all data sources"""
        print("📁 DATA SOURCES")
        print("-" * 30)
        try:
            data_sources = list(self.indexer_client.get_data_sources())
            if data_sources:
                for i, ds in enumerate(data_sources, 1):
                    print(f"{i}. {ds.name}")
                    print(f"   Type: {ds.type}")
                    print(f"   Description: {ds.description or 'N/A'}")
                    print()
            else:
                print("No data sources found")
        except Exception as e:
            print(f"Error listing data sources: {str(e)}")
        print()

def main():
    """Main execution function"""
    print("🔍 AZURE SEARCH RESOURCES INVENTORY")
    print("=" * 60)
    
    try:
        lister = ResourceLister()
        
        print(f"Service: {lister.endpoint}")
        print()
        
        lister.list_indexes()
        lister.list_indexers()
        lister.list_skillsets()
        lister.list_data_sources()
        
        print("=" * 60)
        print("INVENTORY COMPLETE")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()