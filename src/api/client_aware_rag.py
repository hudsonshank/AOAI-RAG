"""
Client-Aware RAG Engine with Metadata Filtering
Enhanced RAG engine that uses client metadata for intelligent document filtering
"""

import os
import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from openai import AsyncAzureOpenAI
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from core.query_optimizer import AdvancedQueryOptimizer

class ClientAwareRAGEngine:
    """Enhanced RAG engine with client metadata awareness"""
    
    def __init__(self):
        # Initialize Azure services
        self.search_client = SearchClient(
            endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
            index_name=os.getenv("EXISTING_INDEX_NAME"),
            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
        )
        
        self.openai_client = AsyncAzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        self.chat_model = os.getenv("AZURE_OPENAI_CHAT_MODEL")
        self.query_optimizer = AdvancedQueryOptimizer()
        
        # Client detection patterns
        self.client_keywords = {
            'camelot': ['camelot', 'camelot corp'],
            'phoenix': ['phoenix', 'phoenix corporation', 'phoenix corp'],
            'lj kruse': ['lj kruse', 'kruse', 'ljk'],
            'gold standard': ['gold standard', 'gold standard forum'],
            'corridor': ['corridor', 'corridor title'],
            'ce floyd': ['ce floyd', 'floyd'],
            'tendit': ['tendit'],
            'twining': ['twining'],
            'neptune': ['neptune'],
            'secs': ['secs']
        }
        
        print("✅ Client-Aware RAG Engine initialized")
    
    def detect_client_from_query(self, query: str) -> Optional[str]:
        """
        Detect if a query is asking about a specific client
        
        Args:
            query: User query text
            
        Returns:
            Client name if detected, None otherwise
        """
        query_lower = query.lower()
        
        # Check for exact client mentions
        for client_name, keywords in self.client_keywords.items():
            for keyword in keywords:
                if keyword in query_lower:
                    return client_name.title()
        
        # Check for PM mentions (e.g., "PM-C documents")
        if 'pm-c' in query_lower:
            return 'Camelot'  # PM-C is Camelot
        elif 'pm-s' in query_lower:
            # Multiple clients with PM-S, can't determine specific client
            return None
        elif 'pm-k' in query_lower:
            return 'CE Floyd'  # PM-K is CE Floyd
        
        return None
    
    def build_client_filter(self, 
                          client_name: Optional[str] = None,
                          pm_initial: Optional[str] = None,
                          include_internal: bool = True,
                          document_category: Optional[str] = None) -> Optional[str]:
        """
        Build OData filter expression for client-specific search
        
        Args:
            client_name: Specific client to filter by
            pm_initial: PM initial to filter by (C, S, K, etc.)
            include_internal: Whether to include internal Autobahn documents
            document_category: Document category to filter by
            
        Returns:
            OData filter string or None
        """
        filters = []
        
        # Client name filter
        if client_name:
            filters.append(f"client_name eq '{client_name}'")
        
        # PM initial filter
        if pm_initial:
            filters.append(f"pm_initial eq '{pm_initial.upper()}'")
        
        # Include internal documents
        if include_internal and not client_name and not pm_initial:
            filters.append("(is_client_specific eq false)")
        elif client_name or pm_initial:
            # If filtering by client, optionally include relevant internal docs
            if include_internal:
                client_filter = " or ".join([f for f in filters])
                filters = [f"({client_filter}) or (is_client_specific eq false)"]
        
        # Document category filter
        if document_category:
            filters.append(f"document_category eq '{document_category}'")
        
        if not filters:
            return None
        
        return " and ".join(filters)
    
    async def client_aware_search(self, 
                                query: str,
                                client_name: Optional[str] = None,
                                pm_initial: Optional[str] = None,
                                document_category: Optional[str] = None,
                                top: int = 5,
                                include_internal: bool = True) -> Dict[str, Any]:
        """
        Perform client-aware search with metadata filtering
        
        Args:
            query: Search query
            client_name: Specific client to search for
            pm_initial: PM initial filter
            document_category: Document category filter
            top: Number of results to return
            include_internal: Whether to include internal documents
            
        Returns:
            Search results with client context
        """
        try:
            # Auto-detect client from query if not specified
            if not client_name:
                detected_client = self.detect_client_from_query(query)
                if detected_client:
                    client_name = detected_client
            
            # Build filter
            filter_expression = self.build_client_filter(
                client_name=client_name,
                pm_initial=pm_initial,
                include_internal=include_internal,
                document_category=document_category
            )
            
            # Perform search
            search_params = {
                "search_text": query,
                "top": top * 2,  # Get extra results for better filtering
                "search_mode": "any"
            }
            
            if filter_expression:
                search_params["filter"] = filter_expression
            
            results = self.search_client.search(**search_params)
            
            # Process results
            sources = []
            for result in results:
                if len(sources) >= top:
                    break
                
                source = {
                    "content": result.get("chunk", ""),
                    "content_preview": result.get("chunk", "")[:200] + "..." if len(result.get("chunk", "")) > 200 else result.get("chunk", ""),
                    "sourcefile": result.get("filename", ""),
                    "sourcepage": result.get("document_path", ""),
                    "title": result.get("title", ""),
                    "chunk_id": result.get("chunk_id", ""),
                    "score": float(result.get("@search.score", 0)),
                    
                    # Client metadata
                    "client_name": result.get("client_name", "Unknown"),
                    "pm_initial": result.get("pm_initial", "N/A"),
                    "document_category": result.get("document_category", "general"),
                    "is_client_specific": result.get("is_client_specific", False),
                    
                    "source_type": "client_filtered" if filter_expression else "general"
                }
                sources.append(source)
            
            return {
                "sources": sources,
                "total_found": len(sources),
                "client_filter_applied": client_name,
                "pm_filter_applied": pm_initial,
                "category_filter_applied": document_category,
                "filter_expression": filter_expression,
                "search_query": query
            }
            
        except Exception as e:
            print(f"Client-aware search error: {str(e)}")
            return {
                "sources": [],
                "error": str(e),
                "search_query": query
            }
    
    async def client_aware_chat(self, 
                              messages: List[Dict],
                              client_context: Optional[str] = None,
                              pm_context: Optional[str] = None,
                              session_id: str = "default") -> Dict[str, Any]:
        """
        Enhanced chat with client-aware context
        
        Args:
            messages: Chat messages
            client_context: Specific client to focus on
            pm_context: PM to focus on
            session_id: Session identifier
            
        Returns:
            Chat response with client-aware context
        """
        thoughts = []
        start_time = datetime.utcnow()
        
        try:
            user_query = messages[-1]["content"]
            
            # Step 1: Query analysis and client detection
            thoughts.append({
                "title": "Client Context Analysis",
                "description": "Analyzing query for client-specific context",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Auto-detect client if not specified
            detected_client = self.detect_client_from_query(user_query)
            active_client = client_context or detected_client
            
            context_info = f"Client context: {active_client or 'General'}"
            if pm_context:
                context_info += f", PM: {pm_context}"
            
            thoughts.append({
                "title": "Context Determined",
                "description": context_info,
                "details": f"Detected from query: {detected_client}" if detected_client else "No client detected",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Step 2: Query optimization
            optimization_result = await self.query_optimizer.optimize_query(user_query, [])
            optimized_query = optimization_result["optimized_query"]
            
            thoughts.append({
                "title": "Query Optimized",
                "description": f"'{user_query}' → '{optimized_query}'",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Step 3: Client-aware search
            thoughts.append({
                "title": "Client-Aware Search",
                "description": f"Searching with client context: {active_client or 'All clients'}" +
                             (f", PM-{pm_context}" if pm_context else ""),
                "timestamp": datetime.utcnow().isoformat()
            })
            
            search_results = await self.client_aware_search(
                query=optimized_query,
                client_name=active_client,
                pm_initial=pm_context,
                top=5,
                include_internal=True
            )
            
            # Client distribution in results
            client_distribution = {}
            for source in search_results["sources"]:
                client = source["client_name"]
                client_distribution[client] = client_distribution.get(client, 0) + 1
            
            dist_text = ", ".join([f"{client}: {count}" for client, count in client_distribution.items()])
            
            thoughts.append({
                "title": "Sources Retrieved", 
                "description": f"Found {len(search_results['sources'])} documents",
                "details": f"Client distribution: {dist_text}",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Step 4: Generate response
            thoughts.append({
                "title": "Client-Aware Response Generation",
                "description": "Creating response with client context awareness",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            response = await self.generate_client_aware_response(
                user_query, 
                search_results["sources"],
                active_client,
                pm_context
            )
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            return {
                "message": response,
                "context": {
                    "thoughts": thoughts,
                    "data_points": search_results["sources"],
                    "client_context": active_client,
                    "pm_context": pm_context,
                    "client_distribution": client_distribution,
                    "filter_applied": search_results.get("filter_expression"),
                    "optimization": optimization_result,
                    "session_id": session_id,
                    "processing_time": f"{processing_time:.2f}s"
                }
            }
            
        except Exception as e:
            return {
                "message": {"content": f"Error in client-aware chat: {str(e)}", "role": "assistant"},
                "context": {"thoughts": thoughts, "error": str(e)}
            }
    
    async def generate_client_aware_response(self, 
                                           user_query: str,
                                           sources: List[Dict],
                                           client_context: Optional[str],
                                           pm_context: Optional[str]) -> Dict[str, Any]:
        """Generate response with client context awareness"""
        try:
            # Build context with client awareness
            context_text = ""
            client_sources = {}
            
            for i, source in enumerate(sources[:5], 1):
                content = source.get("content", "")
                client = source.get("client_name", "Unknown")
                category = source.get("document_category", "general")
                
                # Group sources by client
                if client not in client_sources:
                    client_sources[client] = []
                client_sources[client].append(source)
                
                context_text += f"\n\nSource {i} - {client} ({category}):\n{content}"
            
            # Build system prompt with client awareness
            system_prompt = f"""You are an AI assistant for Autobahn Consultants with access to client-specific documentation.

CURRENT CONTEXT:
- Client Focus: {client_context or 'General inquiry'}
- PM Context: {pm_context or 'Not specified'}
- Sources from: {', '.join(client_sources.keys())}

INSTRUCTIONS:
1. If the query is about a specific client, prioritize information from that client's documents
2. Clearly indicate which client information comes from in your response
3. Include relevant context from internal Autobahn tools when appropriate
4. Use specific citations [Client: filename] for key claims
5. If discussing multiple clients, clearly separate the information

CLIENT-SPECIFIC GUIDELINES:
- Camelot (PM-C): Focus on their specific projects, financials, and meeting notes
- Phoenix Corporation (PM-S): Emphasize their handouts and project materials  
- LJ Kruse (PM-S): Include their onboarding and check-in materials
- Other clients: Treat each client's information as confidential to that client

SOURCE DOCUMENTS:{context_text}

Provide a comprehensive answer that respects client confidentiality while being helpful."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ]
            
            response = await self.openai_client.chat.completions.create(
                model=self.chat_model,
                messages=messages,
                temperature=0.3,
                max_tokens=800
            )
            
            return {"content": response.choices[0].message.content, "role": "assistant"}
            
        except Exception as e:
            return {"content": f"Found relevant client information but couldn't generate response: {str(e)}", "role": "assistant"}
    
    def get_client_list(self) -> List[Dict[str, Any]]:
        """Get list of available clients in the index"""
        try:
            # Use faceting to get client distribution
            results = self.search_client.search(
                "*", 
                facets=["client_name", "pm_initial"],
                top=0
            )
            
            facets = results.get_facets()
            
            clients = []
            if "client_name" in facets:
                for facet in facets["client_name"]:
                    if facet["value"] not in ["Uncategorized", "Processing Error", "Autobahn Internal"]:
                        clients.append({
                            "name": facet["value"],
                            "document_count": facet["count"]
                        })
            
            return sorted(clients, key=lambda x: x["document_count"], reverse=True)
            
        except Exception as e:
            print(f"Error getting client list: {str(e)}")
            return []

# Test function
async def test_client_aware_rag():
    """Test the client-aware RAG functionality"""
    engine = ClientAwareRAGEngine()
    
    test_queries = [
        "What financial information do you have about Camelot?",
        "Show me Phoenix Corporation handouts",
        "What training materials does Autobahn have?",
        "Tell me about LJ Kruse onboarding process"
    ]
    
    print("🧪 Testing Client-Aware RAG Engine")
    print("=" * 50)
    
    for query in test_queries:
        print(f"\n🔍 Query: {query}")
        
        # Test search
        search_results = await engine.client_aware_search(query, top=3)
        
        print(f"  📄 Found {len(search_results['sources'])} documents")
        if search_results.get('client_filter_applied'):
            print(f"  🏢 Client filter: {search_results['client_filter_applied']}")
        
        for source in search_results['sources'][:2]:
            print(f"    - {source['client_name']}: {source['sourcefile']}")

if __name__ == "__main__":
    asyncio.run(test_client_aware_rag())