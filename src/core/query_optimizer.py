"""
Simple Query Optimizer for Enhanced RAG
"""

import os
import asyncio
from typing import List, Dict, Any
from openai import AsyncAzureOpenAI
from dotenv import load_dotenv

load_dotenv()

class AdvancedQueryOptimizer:
    def __init__(self):
        self.client = AsyncAzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.chat_model = os.getenv("AZURE_OPENAI_CHAT_MODEL", "gpt-4.1")
        
    async def optimize_query(self, user_query: str, conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """Optimize user query for better search results"""
        try:
            system_prompt = """You are a search query optimizer. Transform conversational questions into effective search terms.

RULES:
1. Extract key business terms
2. Add relevant synonyms
3. Remove conversational words
4. Add domain terms

EXAMPLES:
"What's our remote work policy?" → "remote work policy telecommuting work from home WFH guidelines procedures"
"How do expense approvals work?" → "expense approval process reimbursement procedures spending authority"

Return only the optimized search terms."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Optimize: {user_query}"}
            ]

            response = await self.client.chat.completions.create(
                model=self.chat_model,
                messages=messages,
                temperature=0.1,
                max_tokens=100
            )
            
            optimized_query = response.choices[0].message.content.strip()
            
            return {
                "optimized_query": optimized_query,
                "reasoning": "AI-optimized for better search relevance",
                "original_query": user_query
            }
            
        except Exception as e:
            # Fallback to basic optimization
            return {
                "optimized_query": self.basic_optimize(user_query),
                "reasoning": f"Basic optimization (AI failed: {str(e)})",
                "original_query": user_query
            }
    
    def basic_optimize(self, query: str) -> str:
        """Basic keyword optimization"""
        # Remove common words
        stop_words = ['what', 'how', 'is', 'our', 'the', 'can', 'you']
        words = [w for w in query.lower().split() if w not in stop_words]
        
        # Add domain terms
        expansions = {
            'remote': 'remote telecommuting work from home WFH',
            'expense': 'expense reimbursement spending cost',
            'policy': 'policy procedure guideline rule',
            'vacation': 'vacation PTO leave time off'
        }
        
        expanded = []
        for word in words:
            expanded.append(word)
            if word in expansions:
                expanded.extend(expansions[word].split())
        
        return ' '.join(expanded)

# Test function
async def test_optimizer():
    optimizer = AdvancedQueryOptimizer()
    result = await optimizer.optimize_query("What is our remote work policy?")
    print("Test result:", result)

if __name__ == "__main__":
    asyncio.run(test_optimizer())
