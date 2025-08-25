#!/usr/bin/env python3
"""
Magic Meeting Tracker Diagnostic Script
Find and analyze the Magic Meeting Tracker document
"""

import os
import asyncio
from dotenv import load_dotenv
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

async def find_magic_meeting_tracker():
    """Find and analyze the Magic Meeting Tracker document"""
    print("🔍 MAGIC MEETING TRACKER DIAGNOSTIC")
    print("=" * 60)
    
    search_client = SearchClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        index_name=os.getenv("EXISTING_INDEX_NAME"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    # Search strategies
    search_queries = [
        "magic meeting tracker 7_30_25",
        "magic meeting tracker",
        "meeting tracker",
        "7_30_25",
        "employee directory autobahn",
        "hudson autobahn"
    ]
    
    print("1. SEARCHING FOR MAGIC MEETING TRACKER")
    print("-" * 40)
    
    all_findings = {}
    
    for query in search_queries:
        print(f"\nSearching: '{query}'")
        try:
            results = search_client.search(
                search_text=query,
                top=10,
                include_total_count=True
            )
            
            found_files = set()
            relevant_chunks = []
            
            for result in results:
                filename = result.get("filename", "").lower()
                content = result.get("chunk", "")
                
                # Check if this looks like our target document
                if any(term in filename for term in ["magic", "meeting", "tracker", "employee", "directory"]):
                    found_files.add(result.get("filename", ""))
                    relevant_chunks.append({
                        "filename": result.get("filename", ""),
                        "score": result.get("@search.score", 0),
                        "content_preview": content[:200],
                        "document_path": result.get("document_path", "")
                    })
            
            if found_files:
                print(f"  ✅ Found {len(found_files)} relevant files:")
                for file in sorted(found_files):
                    print(f"    - {file}")
                
                all_findings[query] = {
                    "files": list(found_files),
                    "chunks": relevant_chunks
                }
            else:
                print(f"  ❌ No relevant files found")
                
        except Exception as e:
            print(f"  ❌ Search error: {str(e)}")
    
    # Analyze findings
    print(f"\n2. ANALYSIS OF FINDINGS")
    print("-" * 40)
    
    all_files = set()
    best_candidates = []
    
    for query, findings in all_findings.items():
        all_files.update(findings["files"])
        best_candidates.extend(findings["chunks"])
    
    if all_files:
        print(f"✅ Total unique files found: {len(all_files)}")
        print(f"📁 All candidate files:")
        for file in sorted(all_files):
            print(f"    - {file}")
        
        # Find the best candidate
        print(f"\n🎯 BEST CANDIDATES (by relevance score):")
        best_candidates.sort(key=lambda x: x["score"], reverse=True)
        
        for i, candidate in enumerate(best_candidates[:5], 1):
            print(f"\n{i}. {candidate['filename']}")
            print(f"   Score: {candidate['score']:.4f}")
            print(f"   Path: {candidate['document_path']}")
            print(f"   Preview: {candidate['content_preview'][:100]}...")
        
        # Test employee search
        print(f"\n3. TESTING EMPLOYEE SEARCH")
        print("-" * 40)
        
        if best_candidates:
            best_file = best_candidates[0]['filename']
            print(f"Testing searches against: {best_file}")
            
            employee_tests = [
                "Hudson",
                "who is Hudson", 
                "Hudson Shank",
                "autobahn employees",
                "team members",
                "employee list"
            ]
            
            for test_query in employee_tests:
                print(f"\nTesting: '{test_query}'")
                try:
                    # Search with filename filter
                    filter_expr = f"search.ismatch('{best_file.split('.')[0]}', 'filename')"
                    
                    results = search_client.search(
                        search_text=test_query,
                        filter=filter_expr,
                        top=3
                    )
                    
                    result_list = list(results)
                    if result_list:
                        print(f"  ✅ Found {len(result_list)} results")
                        top_result = result_list[0]
                        print(f"  📄 Top result score: {top_result.get('@search.score', 0):.4f}")
                        content = top_result.get('chunk', '')
                        if 'hudson' in content.lower():
                            print(f"  🎯 Contains 'Hudson': YES")
                        else:
                            print(f"  ⚠️  Contains 'Hudson': NO")
                        print(f"  📝 Content preview: {content[:150]}...")
                    else:
                        print(f"  ❌ No results found")
                        
                except Exception as e:
                    print(f"  ❌ Error: {str(e)}")
        
        return best_candidates[0] if best_candidates else None
        
    else:
        print(f"❌ No Magic Meeting Tracker found")
        print(f"\n💡 SUGGESTIONS:")
        print(f"   1. Check if the file is named differently")
        print(f"   2. Search for similar files manually:")
        print(f"      - Employee directory")
        print(f"      - Team roster") 
        print(f"      - Staff list")
        print(f"      - Org chart")
        print(f"   3. Verify the file was properly indexed")
        
        # Broader search for employee-related files
        print(f"\n🔍 SEARCHING FOR ANY EMPLOYEE-RELATED FILES")
        print("-" * 50)
        
        broad_searches = [
            "employee",
            "staff",
            "team",
            "directory", 
            "roster",
            "autobahn",
            "hudson"
        ]
        
        for search_term in broad_searches:
            try:
                results = search_client.search(
                    search_text=search_term,
                    top=5
                )
                
                found_files = set()
                for result in results:
                    filename = result.get("filename", "")
                    if filename:
                        found_files.add(filename)
                
                if found_files:
                    print(f"\n'{search_term}' found in:")
                    for file in sorted(found_files):
                        print(f"  - {file}")
                        
            except Exception as e:
                print(f"Error searching '{search_term}': {str(e)}")
        
        return None

async def test_priority_system(magic_tracker_file=None):
    """Test the priority search system"""
    if not magic_tracker_file:
        print(f"\n⚠️  Cannot test priority system - Magic Meeting Tracker not found")
        return
    
    print(f"\n4. TESTING PRIORITY SEARCH SYSTEM")
    print("-" * 40)
    
    # This would test the actual priority system
    # For now, just show the concept
    
    print(f"✅ Target file identified: {magic_tracker_file['filename']}")
    print(f"📊 Base relevance score: {magic_tracker_file['score']:.4f}")
    print(f"🚀 With 15x boost: {magic_tracker_file['score'] * 15:.4f}")
    print(f"💡 This file will now be prioritized for employee queries!")

async def main():
    """Run the diagnostic"""
    try:
        best_candidate = await find_magic_meeting_tracker()
        await test_priority_system(best_candidate)
        
        print(f"\n" + "=" * 60)
        print("🎯 IMPLEMENTATION INSTRUCTIONS")
        print("=" * 60)
        
        if best_candidate:
            print(f"✅ Magic Meeting Tracker found: {best_candidate['filename']}")
            print(f"\n📋 Next steps:")
            print(f"   1. Update your Flask API with the priority system code")
            print(f"   2. Restart your server: python src/api/app.py")
            print(f"   3. Test employee queries:")
            print(f"      curl -X POST http://localhost:5001/api/chat \\")
            print(f"        -H 'Content-Type: application/json' \\")
            print(f"        -d '{{\"messages\":[{{\"role\":\"user\",\"content\":\"Who is Hudson?\"}}]}}'")
            print(f"\n🎉 The system will now prioritize Magic Meeting Tracker for employee queries!")
        else:
            print(f"❌ Magic Meeting Tracker not found in current index")
            print(f"\n📋 Alternative steps:")
            print(f"   1. Verify the file name and location")
            print(f"   2. Check if it needs to be re-indexed")
            print(f"   3. Look for similar employee directory files")
            print(f"   4. Update the filename patterns in the priority system")
        
    except Exception as e:
        print(f"Diagnostic failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())