"""
Add Client Metadata to Existing Azure Search Index
Updates all existing documents in jennifur-rag index with client metadata
"""

import os
import sys
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchField, SearchFieldDataType, SearchIndex,
    SimpleField, SearchableField
)
from azure.core.credentials import AzureKeyCredential
from utils.client_metadata_extractor import ClientMetadataExtractor

load_dotenv()

class ClientMetadataIndexUpdater:
    """Update Azure Search index with client metadata"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.index_name = os.getenv("EXISTING_INDEX_NAME")
        
        if not all([self.endpoint, self.admin_key, self.index_name]):
            raise ValueError("Missing required Azure Search configuration")
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.search_client = SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=self.credential
        )
        self.index_client = SearchIndexClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
        
        self.extractor = ClientMetadataExtractor()
        
    def check_index_fields(self) -> Dict[str, bool]:
        """Check if client metadata fields exist in the index"""
        try:
            index = self.index_client.get_index(self.index_name)
            field_names = {field.name for field in index.fields}
            
            required_fields = {
                'client_name',
                'pm_initial', 
                'document_category',
                'is_client_specific',
                'metadata_updated_timestamp'
            }
            
            existing = {field: field in field_names for field in required_fields}
            
            print(f"📋 Index field status:")
            for field, exists in existing.items():
                status = "✅" if exists else "❌"
                print(f"   {status} {field}")
                
            return existing
            
        except Exception as e:
            print(f"❌ Error checking index fields: {str(e)}")
            return {}
    
    def add_metadata_fields_to_index(self) -> bool:
        """Add client metadata fields to the existing index"""
        try:
            print("🔧 Adding client metadata fields to index...")
            
            # Get current index
            index = self.index_client.get_index(self.index_name)
            
            # Add new fields
            new_fields = [
                SearchableField(
                    name="client_name",
                    type=SearchFieldDataType.String,
                    filterable=True,
                    facetable=True,
                    searchable=True
                ),
                SimpleField(
                    name="pm_initial", 
                    type=SearchFieldDataType.String,
                    filterable=True,
                    facetable=True
                ),
                SearchableField(
                    name="document_category",
                    type=SearchFieldDataType.String, 
                    filterable=True,
                    facetable=True,
                    searchable=True
                ),
                SimpleField(
                    name="is_client_specific",
                    type=SearchFieldDataType.Boolean,
                    filterable=True,
                    facetable=True
                ),
                SimpleField(
                    name="metadata_updated_timestamp",
                    type=SearchFieldDataType.DateTimeOffset,
                    filterable=True,
                    sortable=True
                )
            ]
            
            # Check which fields need to be added
            existing_field_names = {field.name for field in index.fields}
            fields_to_add = [field for field in new_fields if field.name not in existing_field_names]
            
            if not fields_to_add:
                print("✅ All metadata fields already exist in index")
                return True
            
            # Add new fields to existing fields
            updated_fields = list(index.fields) + fields_to_add
            index.fields = updated_fields
            
            # Update the index
            result = self.index_client.create_or_update_index(index)
            
            print(f"✅ Successfully added {len(fields_to_add)} metadata fields to index")
            for field in fields_to_add:
                print(f"   + {field.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error adding metadata fields: {str(e)}")
            return False
    
    def get_all_documents(self) -> List[Dict[str, Any]]:
        """Retrieve all documents from the index"""
        try:
            print("📥 Retrieving all documents from index...")
            
            # Get total count first
            count_result = self.search_client.search("*", include_total_count=True, top=1)
            total_count = count_result.get_count()
            print(f"   Found {total_count} documents total")
            
            # Retrieve all documents in batches
            all_documents = []
            batch_size = 1000
            skip = 0
            
            while skip < total_count:
                batch = list(self.search_client.search(
                    "*",
                    top=batch_size,
                    skip=skip,
                    select=["chunk_id", "document_path", "filename", "metadata", "client_name", "pm_initial"]
                ))
                
                all_documents.extend(batch)
                skip += batch_size
                print(f"   Retrieved {len(all_documents)}/{total_count} documents...")
            
            print(f"✅ Retrieved all {len(all_documents)} documents")
            return all_documents
            
        except Exception as e:
            print(f"❌ Error retrieving documents: {str(e)}")
            return []
    
    def update_documents_with_metadata(self, documents: List[Dict[str, Any]]) -> Dict[str, int]:
        """Update documents with client metadata"""
        print("🏷️  Extracting and adding client metadata...")
        
        updated_docs = []
        stats = {"updated": 0, "skipped": 0, "errors": 0}
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        for i, doc in enumerate(documents, 1):
            try:
                # Skip if already has client metadata (and it's recent)
                if doc.get('client_name') and doc.get('metadata_updated_timestamp'):
                    stats["skipped"] += 1
                    continue
                
                # Extract client info from document path
                document_path = doc.get('document_path', '')
                client_info = self.extractor.extract_client_info(document_path)
                
                # Create update document
                update_doc = {
                    "chunk_id": doc['chunk_id'],
                    "client_name": client_info.client_name,
                    "pm_initial": client_info.pm_initial,
                    "document_category": client_info.document_category,
                    "is_client_specific": client_info.is_client_specific,
                    "metadata_updated_timestamp": timestamp
                }
                
                updated_docs.append(update_doc)
                stats["updated"] += 1
                
                # Progress update
                if i % 50 == 0 or i == len(documents):
                    print(f"   Processed {i}/{len(documents)} documents...")
                
            except Exception as e:
                print(f"   ❌ Error processing document {doc.get('chunk_id', 'unknown')}: {str(e)}")
                stats["errors"] += 1
        
        # Batch update documents
        if updated_docs:
            print(f"📤 Uploading metadata for {len(updated_docs)} documents...")
            try:
                # Update in smaller batches to avoid timeout
                batch_size = 100
                for i in range(0, len(updated_docs), batch_size):
                    batch = updated_docs[i:i+batch_size]
                    result = self.search_client.merge_documents(batch)
                    
                    # Check for errors
                    failed = [r for r in result if not r.succeeded]
                    if failed:
                        print(f"   ⚠️  {len(failed)} documents failed to update in batch {i//batch_size + 1}")
                        for fail in failed[:3]:  # Show first 3 errors
                            print(f"      Error: {fail.error}")
                    
                    print(f"   Updated batch {i//batch_size + 1}/{(len(updated_docs) + batch_size - 1)//batch_size}")
                
                print("✅ Metadata update completed")
                
            except Exception as e:
                print(f"❌ Error updating documents: {str(e)}")
                stats["errors"] += len(updated_docs)
        
        return stats
    
    def generate_client_statistics(self) -> None:
        """Generate and display client statistics"""
        try:
            print("📊 Generating client statistics...")
            
            # Search for documents with client metadata
            results = list(self.search_client.search(
                "*",
                select=["client_name", "pm_initial", "document_category", "is_client_specific"],
                top=1000
            ))
            
            # Extract client info objects for statistics
            client_infos = []
            for result in results:
                from utils.client_metadata_extractor import ClientInfo
                client_info = ClientInfo(
                    client_name=result.get('client_name', 'Unknown'),
                    pm_initial=result.get('pm_initial', 'N/A'),
                    folder_path="",  # Not needed for stats
                    is_client_specific=result.get('is_client_specific', False),
                    document_category=result.get('document_category', 'general')
                )
                client_infos.append(client_info)
            
            # Get statistics
            stats = self.extractor.get_client_statistics(client_infos)
            
            print("\n📈 CLIENT METADATA STATISTICS")
            print("=" * 50)
            print(f"📄 Total documents: {stats['total_documents']}")
            print(f"🏢 Client-specific: {stats['client_specific']}")
            print(f"🔧 Internal documents: {stats['internal_documents']}")
            
            print(f"\n👥 Clients ({len(stats['clients'])} total):")
            for client, count in sorted(stats['clients'].items(), key=lambda x: x[1], reverse=True):
                print(f"   {client}: {count} documents")
            
            print(f"\n📋 Categories ({len(stats['categories'])} total):")
            for category, count in sorted(stats['categories'].items(), key=lambda x: x[1], reverse=True):
                print(f"   {category}: {count} documents")
            
            if stats['pms']:
                print(f"\n👨‍💼 Project Managers ({len(stats['pms'])} total):")
                for pm, count in sorted(stats['pms'].items()):
                    print(f"   PM-{pm}: {count} client documents")
            
        except Exception as e:
            print(f"❌ Error generating statistics: {str(e)}")

async def main():
    """Main execution function"""
    print("🤖 AOAI-RAG Client Metadata Updater")
    print("=" * 50)
    print("This will add client metadata to your existing Azure Search index")
    print("")
    
    try:
        # Initialize updater
        updater = ClientMetadataIndexUpdater()
        print("✅ Connected to Azure Search successfully")
        print(f"   Service: {updater.endpoint}")
        print(f"   Index: {updater.index_name}")
        print("")
        
        # Step 1: Check if metadata fields exist
        print("🔍 Step 1: Checking index schema...")
        field_status = updater.check_index_fields()
        
        # Step 2: Add fields if needed
        if not all(field_status.values()):
            print("\n🔧 Step 2: Adding missing metadata fields to index...")
            if not updater.add_metadata_fields_to_index():
                print("❌ Failed to add metadata fields. Exiting.")
                return
            print("⏳ Waiting 30 seconds for index update to propagate...")
            await asyncio.sleep(30)
        else:
            print("\n✅ Step 2: All metadata fields already exist")
        
        # Step 3: Get all documents
        print("\n📥 Step 3: Retrieving existing documents...")
        documents = updater.get_all_documents()
        if not documents:
            print("❌ No documents found. Exiting.")
            return
        
        # Step 4: Update with metadata
        print(f"\n🏷️  Step 4: Adding client metadata to {len(documents)} documents...")
        stats = updater.update_documents_with_metadata(documents)
        
        print(f"\n📊 UPDATE SUMMARY:")
        print(f"   ✅ Updated: {stats['updated']} documents")
        print(f"   ⏭️  Skipped: {stats['skipped']} documents")
        print(f"   ❌ Errors: {stats['errors']} documents")
        
        # Step 5: Generate statistics
        if stats['updated'] > 0:
            print("\n⏳ Waiting 10 seconds for index to update...")
            await asyncio.sleep(10)
            
            print("\n📈 Step 5: Generating client statistics...")
            updater.generate_client_statistics()
        
        print("\n🎉 Client metadata update completed successfully!")
        print("\nNext steps:")
        print("   1. Test the updated index with client filtering")
        print("   2. Update your RAG engine to use client metadata")
        print("   3. Create client-specific search interfaces")
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())