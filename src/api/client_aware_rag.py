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
            'allbrite': ['allbrite'],
            'camelot': ['camelot', 'camelot corp'],
            'phoenix': ['phoenix', 'phoenix corporation', 'phoenix corp'],
            'lj kruse': ['lj kruse', 'kruse', 'ljk'],
            'gold standard': ['gold standard', 'gold standard forum'],
            'corridor': ['corridor', 'corridor title'],
            'ce floyd': ['ce floyd', 'floyd'],
            'tendit': ['tendit'],
            'twining': ['twining'],
            'neptune': ['neptune', 'neptune plumbing'],
            'secs': ['secs', 'southeast concrete systems'],
            'brets electric': ['brets', 'brets electric'],            
            'cmr': ['cmr'],
            'curexa pharmacy': ['curexa pharmacy', 'curexa'],
            'desert de oro': ['ddo', 'desert de oro'],
            'eckart': ['eckart', 'eckart supply'],
            'eden': ['eden health', 'eden'],
            'gideon': ['gideon'],
            'i3': ['i3'],
            'indium': ['indium'],
            'inpwr': ['inpower', 'inpwr'],
            'rxharmony': ['rxharmony', 'rx harmony', 'joi'],
            'jtl': ['jtl construction', 'jtl'],
            'las colinas pharmacy': ['las colinas pharmacy', 'las colinas'],
            'park square homes': ['park square homes', 'psh'],
            'peak': ['peak', 'bellwether enterprises', 'bellwether'],
            'prc': ['prc'],
            'revelation pharma': ['revelation pharma', 'rev', 'rev pharma'],
            'skybeck': ['skybeck'],
            'talent groups': ['talent groups'],
            'town & country': ['town & country', 'town and country'],
            'wellbore': ['wellbore'],
            'woodward': ['woodward'],
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
                    # Return proper client name - use uppercase for known acronyms
                    if client_name.lower() in ['jtl', 'cmr', 'prc', 'i3', 'psh', 'ddo']:
                        return client_name.upper()
                    else:
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
        # Build client/PM filter first
        client_filters = []
        
        # Client name filter
        if client_name:
            client_filters.append(f"client_name eq '{client_name}'")
        
        # PM initial filter
        if pm_initial:
            client_filters.append(f"pm_initial eq '{pm_initial.upper()}'")
        
        # Combine client/PM filters
        client_filter_expr = None
        if client_filters:
            client_base = " and ".join(client_filters)
            if include_internal:
                # Include both client-specific AND internal documents
                client_filter_expr = f"({client_base}) or (is_client_specific eq false)"
            else:
                # Only client-specific documents
                client_filter_expr = client_base
        elif include_internal:
            # No client specified, include internal documents
            client_filter_expr = "(is_client_specific eq false)"
        
        # Build final filter combining client and category filters
        final_filters = []
        
        if client_filter_expr:
            final_filters.append(f"({client_filter_expr})")
        
        # Document category filter
        if document_category:
            final_filters.append(f"document_category eq '{document_category}'")
        
        if not final_filters:
            return None
        
        return " and ".join(final_filters)
    
    async def _search_magic_meeting_tracker(self, query: str, client_name: Optional[str], top: int) -> List[Dict]:
        """Search MAGIC MEETING TRACKER first - most up-to-date client data"""
        magic_sources = []
        
        # Search MAGIC MEETING TRACKER specifically
        magic_tracker_queries = [
            f"MAGIC MEETING TRACKER {client_name}" if client_name else "MAGIC MEETING TRACKER",
            f"MAGIC MEETING TRACKER {query}",
            "MAGIC MEETING TRACKER"
        ]
        
        for search_query in magic_tracker_queries[:2]:  # Limit to most relevant queries
            try:
                results = self.search_client.search(
                    search_text=search_query,
                    search_fields=["filename", "chunk"],
                    search_mode="any",
                    top=5
                )
                
                for result in results:
                    filename = result.get("filename", "")
                    
                    # Only include actual MAGIC MEETING TRACKER documents
                    if 'MAGIC MEETING TRACKER' in filename.upper():
                        chunk = result.get("chunk", "")
                        source = {
                            "content": chunk,
                            "content_preview": chunk[:200] + "..." if len(chunk) > 200 else chunk,
                            "sourcefile": filename,
                            "sourcepage": result.get("document_path", ""),
                            "title": result.get("title", ""),
                            "chunk_id": result.get("chunk_id", ""),
                            "score": float(result.get("@search.score", 0)) + 2.0,  # BIG score boost for MAGIC TRACKER
                            
                            # Client metadata
                            "client_name": result.get("client_name", "Unknown"),
                            "pm_initial": result.get("pm_initial", "N/A"),
                            "document_category": result.get("document_category", "current_data"),
                            "is_client_specific": result.get("is_client_specific", False),
                            
                            "source_type": "magic_tracker_prioritized"
                        }
                        
                        # Avoid duplicates
                        if not any(s["chunk_id"] == source["chunk_id"] for s in magic_sources):
                            magic_sources.append(source)
                        
                        if len(magic_sources) >= top // 2:  # Get up to half results from MAGIC TRACKER
                            break
            except Exception as e:
                print(f"Error searching MAGIC MEETING TRACKER: {str(e)}")
                continue
        
        return magic_sources[:top // 2]
    
    async def _search_additional_contact_info(self, query: str, client_name: Optional[str], top: int, existing_sources: List[Dict]) -> List[Dict]:
        """Search for additional contact information beyond MAGIC MEETING TRACKER"""
        contact_sources = []
        
        # Search for other contact-related documents
        other_contact_queries = [
            f"{client_name} contact" if client_name else "contact",
            f"{client_name} tracker" if client_name else "tracker",
            "weekly tracker contact email phone",
            "contact list directory"
        ]
        
        existing_chunk_ids = {s["chunk_id"] for s in existing_sources}
        
        for search_query in other_contact_queries:
            try:
                results = self.search_client.search(
                    search_text=search_query,
                    search_fields=["filename", "chunk"],
                    search_mode="any",
                    top=3
                )
                
                for result in results:
                    chunk_id = result.get("chunk_id", "")
                    if chunk_id in existing_chunk_ids:
                        continue  # Skip duplicates
                    
                    chunk = result.get("chunk", "")
                    filename = result.get("filename", "")
                    
                    # Check if this document contains contact information
                    chunk_lower = chunk.lower()
                    has_contact_info = any(indicator in chunk_lower for indicator in 
                                         ['email', 'phone', 'cell', 'contact', '@', 'preferred contact'])
                    
                    # Prioritize documents with contact info
                    if has_contact_info or 'tracker' in filename.lower() or 'contact' in filename.lower():
                        source = {
                            "content": chunk,
                            "content_preview": chunk[:200] + "..." if len(chunk) > 200 else chunk,
                            "sourcefile": filename,
                            "sourcepage": result.get("document_path", ""),
                            "title": result.get("title", ""),
                            "chunk_id": chunk_id,
                            "score": float(result.get("@search.score", 0)) + 0.8,  # Moderate boost for other contact docs
                            
                            # Client metadata
                            "client_name": result.get("client_name", "Unknown"),
                            "pm_initial": result.get("pm_initial", "N/A"),
                            "document_category": result.get("document_category", "contact"),
                            "is_client_specific": result.get("is_client_specific", False),
                            
                            "source_type": "contact_supplementary"
                        }
                        
                        contact_sources.append(source)
                        existing_chunk_ids.add(chunk_id)
                        
                        if len(contact_sources) >= (top // 4):  # Limit additional contact sources
                            break
            except Exception as e:
                print(f"Error searching additional contact info: {str(e)}")
                continue
        
        return contact_sources[:(top // 4)]
    
    def is_contact_information_query(self, query: str) -> bool:
        """Detect if query is asking for contact information"""
        contact_keywords = [
            'contact', 'phone', 'email', 'cell', 'number', 'reach', 'call',
            'meeting tracker', 'directory', 'who is', 'contact info',
            'how to reach', 'phone number', 'email address', 'contact details'
        ]
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in contact_keywords)
    
    async def client_aware_search(self, 
                                query: str,
                                client_name: Optional[str] = None,
                                pm_initial: Optional[str] = None,
                                document_category: Optional[str] = None,
                                top: int = 5,
                                include_internal: bool = True,
                                prioritize_contact_info: bool = None) -> Dict[str, Any]:
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
            
            # Auto-detect if this is a contact information query
            if prioritize_contact_info is None:
                prioritize_contact_info = self.is_contact_information_query(query)
            
            sources = []
            
            # ALWAYS search MAGIC MEETING TRACKER first (most up-to-date data)
            magic_tracker_sources = await self._search_magic_meeting_tracker(
                query, client_name, top
            )
            sources.extend(magic_tracker_sources)
            
            # For contact queries, also search other contact-related documents
            if prioritize_contact_info:
                additional_contact_sources = await self._search_additional_contact_info(
                    query, client_name, top, sources
                )
                sources.extend(additional_contact_sources)
            
            # Build filter for general document search
            filter_expression = self.build_client_filter(
                client_name=client_name,
                pm_initial=pm_initial,
                include_internal=include_internal,
                document_category=document_category
            )
            
            # If we haven't reached our target with contact sources, get general results
            remaining_needed = top - len(sources)
            if remaining_needed > 0:
                # Perform general search
                search_params = {
                    "search_text": query,
                    "top": remaining_needed * 2,  # Get extra results for better filtering
                    "search_mode": "any"
                }
                
                if filter_expression:
                    search_params["filter"] = filter_expression
                
                results = self.search_client.search(**search_params)
                
                # Process general search results
                for result in results:
                    if len(sources) >= top:
                        break
                    
                    chunk_id = result.get("chunk_id", "")
                    # Avoid duplicates from contact search
                    if any(s["chunk_id"] == chunk_id for s in sources):
                        continue
                    
                    source = {
                        "content": result.get("chunk", ""),
                        "content_preview": result.get("chunk", "")[:200] + "..." if len(result.get("chunk", "")) > 200 else result.get("chunk", ""),
                        "sourcefile": result.get("filename", ""),
                        "sourcepage": result.get("document_path", ""),
                        "title": result.get("title", ""),
                        "chunk_id": chunk_id,
                        "score": float(result.get("@search.score", 0)),
                        
                        # Client metadata
                        "client_name": result.get("client_name", "Unknown"),
                        "pm_initial": result.get("pm_initial", "N/A"),
                        "document_category": result.get("document_category", "general"),
                        "is_client_specific": result.get("is_client_specific", False),
                        
                        "source_type": "client_filtered" if filter_expression else "general"
                    }
                    sources.append(source)
            
            # Sort all sources by score (contact sources already have boosted scores)
            sources.sort(key=lambda x: x["score"], reverse=True)
            
            # Count different source types
            magic_tracker_count = sum(1 for s in sources if s.get("source_type") == "magic_tracker_prioritized")
            contact_sources_count = sum(1 for s in sources if s.get("source_type") in ["contact_supplementary", "magic_tracker_prioritized"])
            
            return {
                "sources": sources,
                "total_found": len(sources),
                "client_filter_applied": client_name,
                "pm_filter_applied": pm_initial,
                "category_filter_applied": document_category,
                "filter_expression": filter_expression,
                "search_query": query,
                "contact_prioritized": prioritize_contact_info,
                "contact_sources_found": contact_sources_count,
                "magic_tracker_sources_found": magic_tracker_count
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
                facets=["client_name,count:50", "pm_initial"],  # Get up to 50 client facets
                top=0
            )
            
            facets = results.get_facets()
            
            clients = []
            if "client_name" in facets:
                for facet in facets["client_name"]:
                    # Include all clients, but can optionally exclude certain categories
                    if facet["value"] not in ["Processing Error"]:  # Only exclude genuine errors
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