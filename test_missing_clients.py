#!/usr/bin/env python3
"""
Test RAG System for Missing Clients
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

async def test_missing_clients():
    """Test the RAG system for clients that should be in the index but might be missing"""
    
    print("🔍 Testing RAG System for Missing Clients")
    print("=" * 70)
    
    engine = ClientAwareRAGEngine()
    
    # List of clients to test that we suspect are missing
    test_clients = [
        "Las Colinas Pharmacy",
        "Indium",
        "Gold Standard",
        "CE Floyd", 
        "Tendit",
        "Twining",
        "SECS",
        "JTL",
        "Autobahn"
    ]
    
    for client_name in test_clients:
        print(f"\n{'='*70}")
        print(f"🏢 Testing Client: {client_name}")
        print(f"{'='*70}")
        
        # Test different query patterns
        queries = [
            f"What information do you have about {client_name}?",
            f"Tell me about {client_name}",
            f"Show me {client_name} documents",
            f"{client_name} financial information",
            f"{client_name} meeting notes"
        ]
        
        for i, query in enumerate(queries[:2], 1):  # Test first 2 queries for each client
            print(f"\n📝 Query {i}: '{query}'")
            print("-" * 50)
            
            # Check if client is auto-detected
            detected = engine.detect_client_from_query(query)
            print(f"Auto-detected client: {detected if detected else 'None'}")
            
            # Perform search
            search_results = await engine.client_aware_search(
                query=query,
                client_name=detected,
                top=5
            )
            
            print(f"Documents found: {len(search_results['sources'])}")
            
            if search_results['sources']:
                # Analyze the results
                client_names = {}
                categories = {}
                
                for source in search_results['sources']:
                    client = source.get('client_name', 'Unknown')
                    category = source.get('document_category', 'general')
                    
                    client_names[client] = client_names.get(client, 0) + 1
                    categories[category] = categories.get(category, 0) + 1
                
                print(f"\nClient distribution in results:")
                for client, count in client_names.items():
                    print(f"  - {client}: {count} documents")
                
                print(f"\nCategory distribution:")
                for category, count in categories.items():
                    print(f"  - {category}: {count} documents")
                
                print(f"\nTop 3 results:")
                for j, source in enumerate(search_results['sources'][:3], 1):
                    print(f"\n  Result {j}:")
                    print(f"    Client: {source.get('client_name', 'Unknown')}")
                    print(f"    File: {source.get('sourcefile', 'N/A')}")
                    print(f"    Category: {source.get('document_category', 'general')}")
                    print(f"    PM: {source.get('pm_initial', 'N/A')}")
                    print(f"    Score: {source.get('score', 0):.3f}")
                    preview = source.get('content_preview', '')[:150]
                    print(f"    Preview: {preview}...")
            else:
                print("❌ No documents found!")
            
            # Get chat response for first query only
            if i == 1:
                print(f"\n💬 RAG Chat Response:")
                print("-" * 50)
                messages = [{"role": "user", "content": query}]
                response = await engine.client_aware_chat(messages)
                response_text = response['message']['content']
                
                # Print first 500 chars of response
                if len(response_text) > 500:
                    print(response_text[:500] + "...")
                else:
                    print(response_text)
                
                # Check if response indicates missing data
                missing_indicators = [
                    "no information",
                    "don't have",
                    "not found",
                    "no documents",
                    "unable to find",
                    "no specific information"
                ]
                
                response_lower = response_text.lower()
                has_missing_indicator = any(indicator in response_lower for indicator in missing_indicators)
                
                if has_missing_indicator:
                    print("\n⚠️ Response suggests client data is missing from index!")
                elif client_name.lower() in response_lower:
                    print(f"\n✅ Response mentions {client_name}")
                else:
                    print(f"\n⚠️ Response doesn't specifically mention {client_name}")
    
    # Summary
    print("\n" + "="*70)
    print("📊 SUMMARY")
    print("="*70)
    
    # Get current client list
    all_clients = engine.get_client_list()
    print(f"\nTotal clients in index: {len(all_clients)}")
    print("Clients found:")
    for client in all_clients:
        print(f"  - {client['name']}: {client['document_count']} documents")
    
    print(f"\n🔴 Missing clients (not in index):")
    indexed_client_names = [c['name'].lower() for c in all_clients]
    for test_client in test_clients:
        if test_client.lower() not in indexed_client_names:
            print(f"  - {test_client}")
    
    print("\n✅ Test Complete")

if __name__ == "__main__":
    asyncio.run(test_missing_clients())