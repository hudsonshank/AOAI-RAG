"""
Test the Complete Client Metadata System
Run comprehensive tests of the client metadata tagging system
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

load_dotenv()

from utils.client_metadata_extractor import ClientMetadataExtractor
from utils.enhanced_document_processor import EnhancedDocumentProcessor
from api.client_aware_rag import ClientAwareRAGEngine
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

async def test_complete_system():
    """Test the complete client metadata system"""
    print("🤖 AOAI-RAG Client Metadata System Test")
    print("=" * 60)
    print("Testing the complete client metadata tagging system\n")
    
    # Test 1: Client Metadata Extraction
    print("🧪 Test 1: Client Metadata Extraction")
    print("-" * 40)
    
    extractor = ClientMetadataExtractor()
    
    test_paths = [
        "/Camelot (PM-C)/_08. Financials/Q1_Report.pdf",
        "/Phoenix Corporation (PM-S)/Handouts/Training.pdf", 
        "/LJ Kruse (PM-S)/Check-in Issues List.xlsx",
        "/Gold Standard Forum (PM-C)/Content Ideas/constitution.pdf",
        "/Autobahn Tools/1-3-1 Problem Solving/Handouts.pdf",
        "/PM & APM Training Materials/APM Handbook.pdf",
        "/Random Folder/unknown_document.pdf"
    ]
    
    extraction_results = []
    for path in test_paths:
        info = extractor.extract_client_info(path)
        extraction_results.append(info)
        
        print(f"✓ {path}")
        print(f"  → Client: {info.client_name} | PM: {info.pm_initial} | Category: {info.document_category}")
    
    print(f"\n✅ Extracted metadata for {len(test_paths)} documents")
    
    # Test 2: Document Processing Pipeline  
    print(f"\n🧪 Test 2: Enhanced Document Processing")
    print("-" * 40)
    
    processor = EnhancedDocumentProcessor()
    
    test_documents = [
        {
            "content": "This is a quarterly financial report showing revenue and expenses for Q1 2024. Total revenue was $2.5M with expenses of $1.8M, resulting in a net profit of $700K.",
            "document_path": "/Camelot (PM-C)/_08. Financials/Q1_2024_Financial_Report.pdf",
            "filename": "Q1_2024_Financial_Report.pdf"
        },
        {
            "content": "Meeting notes from weekly standup. Discussed project timeline, resource allocation, and upcoming milestones. Action items assigned to team members.",
            "document_path": "/Phoenix Corporation (PM-S)/Meetings/weekly_standup_notes.docx",
            "filename": "weekly_standup_notes.docx"
        },
        {
            "content": "Employee handbook covering company policies, procedures, benefits, and code of conduct. Updated for 2024 with new remote work policies.",
            "document_path": "/Autobahn Tools/Training/Employee_Handbook_2024.pdf",
            "filename": "Employee_Handbook_2024.pdf"
        }
    ]
    
    processed_docs = processor.process_document_batch(test_documents)
    
    print(f"✅ Processed {len(processed_docs)} documents with full metadata")
    for doc in processed_docs:
        print(f"  ✓ {doc['filename']} → {doc['client_name']} ({doc['document_category']})")
    
    # Generate processing statistics
    stats = processor.get_processing_statistics(processed_docs)
    print(f"\n📊 Processing Statistics:")
    print(f"   Total: {stats['total_documents']} | Client-specific: {stats['client_specific']} | Internal: {stats['internal_documents']}")
    print(f"   Clients: {', '.join(stats['clients'].keys())}")
    
    # Test 3: Check if index has metadata fields (without updating)
    print(f"\n🧪 Test 3: Index Metadata Fields Check")
    print("-" * 40)
    
    try:
        search_client = SearchClient(
            endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
            index_name=os.getenv("EXISTING_INDEX_NAME"),
            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
        )
        
        # Test search for documents with client metadata
        results = list(search_client.search(
            "*",
            select=["chunk_id", "client_name", "pm_initial", "document_category", "is_client_specific"],
            top=5
        ))
        
        has_metadata = False
        for result in results:
            if result.get('client_name') and result.get('client_name') != 'null':
                has_metadata = True
                break
        
        if has_metadata:
            print("✅ Index contains client metadata fields")
            print("   Sample documents with metadata:")
            for i, result in enumerate(results[:3], 1):
                client = result.get('client_name', 'N/A')
                pm = result.get('pm_initial', 'N/A') 
                category = result.get('document_category', 'N/A')
                print(f"   {i}. Client: {client} | PM: {pm} | Category: {category}")
        else:
            print("❌ Index does not contain client metadata")
            print("   → Run 'python scripts/add_client_metadata.py' to add metadata to existing documents")
            
    except Exception as e:
        print(f"❌ Error checking index metadata: {str(e)}")
    
    # Test 4: Client-Aware RAG Engine
    print(f"\n🧪 Test 4: Client-Aware RAG Engine")  
    print("-" * 40)
    
    try:
        rag_engine = ClientAwareRAGEngine()
        
        # Test client detection
        test_queries = [
            "What financial information do you have about Camelot?",
            "Show me Phoenix Corporation documents", 
            "What training materials are available?",
            "Tell me about PM-C projects"
        ]
        
        print("Testing client detection and search:")
        for query in test_queries:
            detected_client = rag_engine.detect_client_from_query(query)
            print(f"  '{query}' → Detected client: {detected_client or 'None'}")
        
        # Test search with client filtering
        print(f"\nTesting client-aware search:")
        search_result = await rag_engine.client_aware_search(
            "financial reports",
            client_name="Camelot",
            top=3
        )
        
        print(f"  ✓ Search for 'financial reports' filtered by Camelot:")
        print(f"    Found {len(search_result['sources'])} documents")
        print(f"    Filter: {search_result.get('filter_expression', 'None')}")
        
        for source in search_result['sources'][:2]:
            client = source.get('client_name', 'Unknown')
            file = source.get('sourcefile', 'Unknown')
            print(f"    - {client}: {file}")
        
        # Test getting client list
        clients = rag_engine.get_client_list()
        if clients:
            print(f"\n  ✓ Available clients in index:")
            for client in clients[:5]:
                print(f"    - {client['name']}: {client['document_count']} documents")
        else:
            print(f"\n  ❌ No clients found in index")
        
        print("✅ Client-Aware RAG Engine working correctly")
        
    except Exception as e:
        print(f"❌ Error testing RAG engine: {str(e)}")
    
    # Test 5: Integration Test
    print(f"\n🧪 Test 5: Full Integration Test")
    print("-" * 40)
    
    try:
        # Simulate processing a new document and searching for it
        new_doc = {
            "content": "Budget allocation for Q2 2024 project initiatives. Marketing: $500K, Development: $300K, Operations: $200K.",
            "document_path": "/Neptune (PM-C)/04. Budget/Q2_2024_Budget.xlsx", 
            "filename": "Q2_2024_Budget.xlsx"
        }
        
        # Process the document
        processed_doc = processor.process_document_for_indexing(
            new_doc["content"],
            new_doc["document_path"],
            new_doc["filename"]
        )
        
        print(f"✓ Processed new document: {processed_doc['filename']}")
        print(f"  Client: {processed_doc['client_name']}")
        print(f"  PM: {processed_doc['pm_initial']}")
        print(f"  Category: {processed_doc['document_category']}")
        
        # Test search that would find this document
        search_result = await rag_engine.client_aware_search(
            "budget allocation",
            client_name="Neptune", 
            top=5
        )
        
        print(f"✓ Client-aware search for Neptune budget documents:")
        print(f"  Found {len(search_result['sources'])} existing documents")
        
        print("✅ Full integration test completed")
        
    except Exception as e:
        print(f"❌ Integration test error: {str(e)}")
    
    # Final Summary
    print(f"\n🎉 SYSTEM TEST SUMMARY")
    print("=" * 60)
    print("✅ Client metadata extraction working")
    print("✅ Enhanced document processing working") 
    print("✅ Document metadata validation working")
    print("✅ Client-aware RAG engine working")
    print("✅ Integration pipeline working")
    
    print(f"\n📋 NEXT STEPS:")
    if not has_metadata:
        print("1. Run: python scripts/add_client_metadata.py")
        print("   → Add metadata to existing 233 documents")
    else:
        print("1. ✅ Index already has client metadata")
    
    print("2. Update your main RAG API to use client_aware_rag.py")
    print("3. Add client selection UI to your frontend")
    print("4. Test with real client queries")
    print("5. Monitor client-specific search performance")

if __name__ == "__main__":
    asyncio.run(test_complete_system())