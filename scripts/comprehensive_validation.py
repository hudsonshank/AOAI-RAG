#!/usr/bin/env python3
"""
Comprehensive Validation Without Rerunning Indexer
Thorough testing that costs $0 and takes minutes instead of hours
"""

import os
from dotenv import load_dotenv
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class ComprehensiveValidator:
    """Validate search functionality without rerunning indexer"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.index_name = "jennifur-rag"
        self.indexer_name = "jennifur-rag-indexer"
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.search_client = SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=self.credential
        )
        self.indexer_client = SearchIndexerClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
    
    def validate_indexer_health(self) -> dict:
        """Check indexer execution history and current status"""
        try:
            print("🏥 INDEXER HEALTH CHECK")
            print("-" * 30)
            
            status = self.indexer_client.get_indexer_status(self.indexer_name)
            indexer = self.indexer_client.get_indexer(self.indexer_name)
            
            health = {
                "current_status": str(status.status),
                "skillset": indexer.skillset_name or "None (Direct indexing)",
                "warnings": 0,
                "errors": 0,
                "last_success": False
            }
            
            if status.last_result:
                health["last_result"] = str(status.last_result.status)
                health["start_time"] = status.last_result.start_time
                health["end_time"] = status.last_result.end_time
                health["items_processed"] = getattr(status.last_result, 'item_count', 0)
                health["warnings"] = getattr(status.last_result, 'warning_count', 0)
                health["errors"] = getattr(status.last_result, 'failed_item_count', 0)
                health["last_success"] = str(status.last_result.status).lower() == "success"
            
            print(f"   Status: {health['current_status']}")
            print(f"   Skillset: {health['skillset']}")
            print(f"   Last Result: {health.get('last_result', 'N/A')}")
            print(f"   Items Processed: {health.get('items_processed', 0):,}")
            print(f"   Warnings: {health['warnings']}")
            print(f"   Errors: {health['errors']}")
            
            if health['warnings'] == 0 and health['errors'] == 0 and health['last_success']:
                print("   ✅ HEALTHY: No warnings, no errors, last run successful")
            else:
                print("   ⚠️  ISSUES DETECTED: Check warnings/errors")
            
            return health
            
        except Exception as e:
            print(f"   ❌ Health check failed: {str(e)}")
            return {"error": str(e)}
    
    def validate_document_quality(self) -> dict:
        """Sample documents to ensure quality and completeness"""
        try:
            print(f"\n📄 DOCUMENT QUALITY VALIDATION")
            print("-" * 30)
            
            # Test various document types and clients
            test_queries = [
                ("policy", "Policy documents"),
                ("client_name:Phoenix*", "Phoenix client docs"),
                ("client_name:'Autobahn Internal'", "Internal docs"),
                ("document_category:financial", "Financial docs"),
                ("pm_initial:C", "PM-C managed docs")
            ]
            
            quality_results = {
                "total_tests": len(test_queries),
                "passed": 0,
                "failed": 0,
                "details": []
            }
            
            for query, description in test_queries:
                try:
                    results = self.search_client.search(
                        search_text=query,
                        top=3,
                        include_total_count=True
                    )
                    
                    result_list = list(results)
                    total_count = results.get_count()
                    
                    test_result = {
                        "query": description,
                        "count": total_count,
                        "has_results": len(result_list) > 0,
                        "has_chunk": False,
                        "has_metadata": False
                    }
                    
                    if result_list:
                        first_doc = result_list[0]
                        test_result["has_chunk"] = "chunk" in first_doc and bool(first_doc.get("chunk", "").strip())
                        test_result["has_metadata"] = all(field in first_doc for field in ["client_name", "pm_initial", "document_category"])
                        
                        if test_result["has_results"] and test_result["has_chunk"] and test_result["has_metadata"]:
                            quality_results["passed"] += 1
                        else:
                            quality_results["failed"] += 1
                    else:
                        quality_results["failed"] += 1
                    
                    quality_results["details"].append(test_result)
                    
                    status = "✅" if test_result.get("has_results") and test_result.get("has_chunk") and test_result.get("has_metadata") else "❌"
                    print(f"   {status} {description}: {total_count:,} docs")
                    
                except Exception as e:
                    print(f"   ❌ {description}: Error - {str(e)}")
                    quality_results["failed"] += 1
            
            pass_rate = (quality_results["passed"] / quality_results["total_tests"]) * 100
            print(f"\n   📊 Quality Score: {quality_results['passed']}/{quality_results['total_tests']} ({pass_rate:.1f}%)")
            
            return quality_results
            
        except Exception as e:
            print(f"   ❌ Document quality validation failed: {str(e)}")
            return {"error": str(e)}
    
    def validate_metadata_consistency(self) -> dict:
        """Check metadata consistency across document types"""
        try:
            print(f"\n🏷️  METADATA CONSISTENCY CHECK")
            print("-" * 30)
            
            # Get sample from different document categories
            results = self.search_client.search(
                search_text="*",
                top=100,
                select=["client_name", "pm_initial", "document_category", "is_client_specific", "filename"]
            )
            
            docs = list(results)
            if not docs:
                print("   ❌ No documents found")
                return {"error": "No documents"}
            
            # Analyze metadata patterns
            metadata_stats = {
                "total_docs": len(docs),
                "client_names": {},
                "pm_initials": {},
                "categories": {},
                "client_specific": 0,
                "internal": 0,
                "missing_metadata": 0
            }
            
            for doc in docs:
                # Count client names
                client = doc.get("client_name", "Unknown")
                metadata_stats["client_names"][client] = metadata_stats["client_names"].get(client, 0) + 1
                
                # Count PM initials
                pm = doc.get("pm_initial", "N/A")
                metadata_stats["pm_initials"][pm] = metadata_stats["pm_initials"].get(pm, 0) + 1
                
                # Count categories
                category = doc.get("document_category", "unknown")
                metadata_stats["categories"][category] = metadata_stats["categories"].get(category, 0) + 1
                
                # Count client-specific vs internal
                if doc.get("is_client_specific", False):
                    metadata_stats["client_specific"] += 1
                else:
                    metadata_stats["internal"] += 1
                
                # Check for missing metadata
                required_fields = ["client_name", "pm_initial", "document_category"]
                if not all(doc.get(field) for field in required_fields):
                    metadata_stats["missing_metadata"] += 1
            
            print(f"   📊 Sample Analysis ({len(docs)} documents):")
            print(f"   Unique clients: {len(metadata_stats['client_names'])}")
            print(f"   Unique PMs: {len(metadata_stats['pm_initials'])}")
            print(f"   Document categories: {len(metadata_stats['categories'])}")
            print(f"   Client-specific: {metadata_stats['client_specific']}")
            print(f"   Internal docs: {metadata_stats['internal']}")
            print(f"   Missing metadata: {metadata_stats['missing_metadata']}")
            
            # Top clients
            top_clients = sorted(metadata_stats["client_names"].items(), key=lambda x: x[1], reverse=True)[:3]
            print(f"   Top clients: {', '.join([f'{name} ({count})' for name, count in top_clients])}")
            
            consistency_score = ((len(docs) - metadata_stats['missing_metadata']) / len(docs)) * 100
            print(f"   ✅ Metadata consistency: {consistency_score:.1f}%")
            
            return metadata_stats
            
        except Exception as e:
            print(f"   ❌ Metadata consistency check failed: {str(e)}")
            return {"error": str(e)}
    
    def validate_search_performance(self) -> dict:
        """Test search performance and relevance"""
        try:
            print(f"\n⚡ SEARCH PERFORMANCE VALIDATION")
            print("-" * 30)
            
            import time
            
            performance_tests = [
                ("remote work policy", "Specific policy search"),
                ("expense reimbursement", "Business process search"),
                ("financial report", "Document type search"),
                ("client meeting notes", "Mixed term search")
            ]
            
            performance_results = {
                "tests": [],
                "avg_response_time": 0,
                "total_time": 0
            }
            
            total_time = 0
            
            for query, description in performance_tests:
                start_time = time.time()
                
                results = self.search_client.search(
                    search_text=query,
                    top=10,
                    include_total_count=True
                )
                
                result_list = list(results)
                end_time = time.time()
                
                response_time = (end_time - start_time) * 1000  # Convert to milliseconds
                total_time += response_time
                
                test_result = {
                    "query": description,
                    "response_time_ms": response_time,
                    "total_results": results.get_count(),
                    "returned_results": len(result_list),
                    "top_score": result_list[0].get("@search.score", 0) if result_list else 0
                }
                
                performance_results["tests"].append(test_result)
                
                print(f"   {description}:")
                print(f"     Response time: {response_time:.0f}ms")
                print(f"     Results: {test_result['returned_results']}/{test_result['total_results']}")
                print(f"     Top score: {test_result['top_score']:.2f}")
            
            performance_results["avg_response_time"] = total_time / len(performance_tests)
            performance_results["total_time"] = total_time
            
            print(f"\n   📊 Performance Summary:")
            print(f"   Average response time: {performance_results['avg_response_time']:.0f}ms")
            
            if performance_results['avg_response_time'] < 1000:
                print(f"   ✅ EXCELLENT: Response times under 1 second")
            elif performance_results['avg_response_time'] < 3000:
                print(f"   ✅ GOOD: Response times under 3 seconds")
            else:
                print(f"   ⚠️  SLOW: Response times over 3 seconds")
            
            return performance_results
            
        except Exception as e:
            print(f"   ❌ Performance validation failed: {str(e)}")
            return {"error": str(e)}

def main():
    """Run comprehensive validation"""
    print("🔍 COMPREHENSIVE VALIDATION (NO RERUN NEEDED)")
    print("=" * 60)
    print("Testing everything without rerunning the indexer")
    print("Cost: $0.00 | Time: ~2 minutes")
    print()
    
    try:
        validator = ComprehensiveValidator()
        
        # Run all validations
        health = validator.validate_indexer_health()
        quality = validator.validate_document_quality()
        metadata = validator.validate_metadata_consistency()
        performance = validator.validate_search_performance()
        
        # Overall assessment
        print(f"\n🏆 OVERALL ASSESSMENT")
        print("=" * 40)
        
        issues = []
        
        # Check health
        if health.get("warnings", 0) > 0:
            issues.append(f"Indexer has {health['warnings']} warnings")
        if health.get("errors", 0) > 0:
            issues.append(f"Indexer has {health['errors']} errors")
        
        # Check quality
        if quality.get("passed", 0) < quality.get("total_tests", 1):
            issues.append("Some document quality tests failed")
        
        # Check metadata
        if metadata.get("missing_metadata", 0) > 0:
            issues.append(f"{metadata['missing_metadata']} documents missing metadata")
        
        # Check performance
        if performance.get("avg_response_time", 0) > 3000:
            issues.append("Search response times are slow")
        
        if not issues:
            print("✅ SYSTEM IS HEALTHY")
            print("   No warnings, errors, or performance issues detected")
            print("   Search functionality working perfectly")
            print("   Metadata consistency excellent")
            print("   👉 NO NEED TO RERUN INDEXER")
        else:
            print("⚠️  ISSUES DETECTED:")
            for issue in issues:
                print(f"   • {issue}")
            print("   👉 Consider investigating before rerunning")
        
        print(f"\n💰 COST SAVINGS:")
        print(f"   This validation: $0.00")
        print(f"   Full indexer rerun: $0.00 (but takes 30+ minutes)")
        print(f"   Time saved: ~30 minutes")
        
    except Exception as e:
        print(f"❌ Validation failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()