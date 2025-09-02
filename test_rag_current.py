#!/usr/bin/env python3
"""
Test Current RAG System Performance
"""

import os
import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from src.api.client_aware_rag import ClientAwareRAGEngine

async def test_current_rag():
    """Test the current state of the RAG system"""
    
    print("🔍 Testing Current RAG System")
    print("=" * 60)
    
    engine = ClientAwareRAGEngine()
    
    # Test 1: Check available clients
    print("\n📊 TEST 1: Checking Available Clients")
    print("-" * 40)
    clients = engine.get_client_list()
    print(f"Total clients found: {len(clients)}")
    if clients:
        print("\nTop 10 clients by document count:")
        for i, client in enumerate(clients[:10], 1):
            print(f"  {i}. {client['name']}: {client['document_count']} documents")
        if len(clients) > 10:
            print(f"  ... and {len(clients) - 10} more clients")
    
    # Test 2: Query about Autobahn team
    print("\n\n🏢 TEST 2: Query about Autobahn Team")
    print("-" * 40)
    autobahn_queries = [
        "Who is on the Autobahn team?",
        "Tell me about Autobahn employees",
        "What is Autobahn's organizational structure?"
    ]
    
    for query in autobahn_queries:
        print(f"\nQuery: '{query}'")
        search_results = await engine.client_aware_search(query, top=3)
        
        print(f"Found {len(search_results['sources'])} results")
        if search_results['sources']:
            print("Top results:")
            for i, source in enumerate(search_results['sources'][:2], 1):
                print(f"  {i}. Client: {source['client_name']}")
                print(f"     File: {source['sourcefile']}")
                print(f"     Category: {source['document_category']}")
                print(f"     Preview: {source['content_preview'][:100]}...")
        
        # Get chat response
        messages = [{"role": "user", "content": query}]
        response = await engine.client_aware_chat(messages)
        print(f"\nRAG Response:")
        print(response['message']['content'][:500] + "..." if len(response['message']['content']) > 500 else response['message']['content'])
        break  # Just test the first query for brevity
    
    # Test 3: Query about JTL team
    print("\n\n🏢 TEST 3: Query about JTL Team")
    print("-" * 40)
    jtl_query = "Who is on the JTL team?"
    print(f"Query: '{jtl_query}'")
    
    search_results = await engine.client_aware_search(jtl_query, top=3)
    print(f"Found {len(search_results['sources'])} results")
    if search_results['sources']:
        print("Top results:")
        for i, source in enumerate(search_results['sources'][:2], 1):
            print(f"  {i}. Client: {source['client_name']}")
            print(f"     File: {source['sourcefile']}")
            print(f"     Category: {source['document_category']}")
    
    # Test 4: Query about specific clients
    print("\n\n💼 TEST 4: Query about Specific Clients")
    print("-" * 40)
    test_clients = ["Camelot", "Phoenix", "LJ Kruse"]
    
    for client_name in test_clients:
        query = f"What information do you have about {client_name}?"
        print(f"\nQuery: '{query}'")
        
        # Test with client detection
        detected = engine.detect_client_from_query(query)
        print(f"Auto-detected client: {detected}")
        
        # Search with client filter
        search_results = await engine.client_aware_search(
            query=query,
            client_name=detected,
            top=2
        )
        
        print(f"Documents found: {len(search_results['sources'])}")
        if search_results['sources']:
            categories = set(s['document_category'] for s in search_results['sources'])
            print(f"Document categories: {', '.join(categories)}")
    
    # Test 5: Check metadata extraction
    print("\n\n🔬 TEST 5: Metadata Extraction Check")
    print("-" * 40)
    
    # Do a broad search to check metadata
    all_results = await engine.client_aware_search("*", top=20)
    
    # Analyze metadata quality
    metadata_stats = {
        "has_client_name": 0,
        "has_pm_initial": 0,
        "has_category": 0,
        "is_client_specific": 0,
        "unique_clients": set(),
        "unique_pms": set(),
        "unique_categories": set()
    }
    
    for source in all_results['sources']:
        if source.get('client_name') and source['client_name'] != 'Unknown':
            metadata_stats['has_client_name'] += 1
            metadata_stats['unique_clients'].add(source['client_name'])
        
        if source.get('pm_initial') and source['pm_initial'] != 'N/A':
            metadata_stats['has_pm_initial'] += 1
            metadata_stats['unique_pms'].add(source['pm_initial'])
        
        if source.get('document_category') and source['document_category'] != 'general':
            metadata_stats['has_category'] += 1
            metadata_stats['unique_categories'].add(source['document_category'])
        
        if source.get('is_client_specific'):
            metadata_stats['is_client_specific'] += 1
    
    total_checked = len(all_results['sources'])
    print(f"Checked {total_checked} documents:")
    print(f"  - With client_name: {metadata_stats['has_client_name']}/{total_checked} ({metadata_stats['has_client_name']*100/total_checked:.1f}%)")
    print(f"  - With pm_initial: {metadata_stats['has_pm_initial']}/{total_checked} ({metadata_stats['has_pm_initial']*100/total_checked:.1f}%)")
    print(f"  - With category: {metadata_stats['has_category']}/{total_checked} ({metadata_stats['has_category']*100/total_checked:.1f}%)")
    print(f"  - Client-specific: {metadata_stats['is_client_specific']}/{total_checked} ({metadata_stats['is_client_specific']*100/total_checked:.1f}%)")
    
    print(f"\nUnique values found:")
    print(f"  - Clients: {len(metadata_stats['unique_clients'])} ({', '.join(list(metadata_stats['unique_clients'])[:5])}...)")
    print(f"  - PMs: {len(metadata_stats['unique_pms'])} ({', '.join(metadata_stats['unique_pms'])})")
    print(f"  - Categories: {len(metadata_stats['unique_categories'])} ({', '.join(list(metadata_stats['unique_categories'])[:5])}...)")
    
    print("\n" + "=" * 60)
    print("✅ RAG System Test Complete")
    
    return {
        "client_count": len(clients),
        "metadata_quality": metadata_stats,
        "autobahn_found": any("autobahn" in s['client_name'].lower() for s in all_results['sources'] if s.get('client_name'))
    }

if __name__ == "__main__":
    results = asyncio.run(test_current_rag())