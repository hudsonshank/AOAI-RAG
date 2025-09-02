import json
import logging
import azure.functions as func
import os
# Removed asyncio - using synchronous OpenAI client
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from openai import AzureOpenAI

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Simplified Azure Function for RAG chat endpoint
    """
    logging.info('RAG chat function triggered')
    
    # Set CORS headers
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }
    
    # Handle preflight OPTIONS request
    if req.method == 'OPTIONS':
        return func.HttpResponse(
            "",
            status_code=200,
            headers=headers
        )
    
    try:
        # Parse request
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json",
                headers=headers
            )
        
        messages = req_body.get('messages', [])
        if not messages:
            return func.HttpResponse(
                json.dumps({"error": "Messages array is required"}),
                status_code=400,
                mimetype="application/json",
                headers=headers
            )
        
        # Get user message
        user_message = messages[-1].get('content', '') if messages else ''
        
        # Initialize clients
        try:
            search_client = SearchClient(
                endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
                index_name=os.getenv("EXISTING_INDEX_NAME"),
                credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
            )
            
            openai_client = AzureOpenAI(
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
            )
        except Exception as e:
            logging.error(f'Failed to initialize clients: {str(e)}')
            return func.HttpResponse(
                json.dumps({"error": f"Client initialization failed: {str(e)}"}),
                status_code=500,
                mimetype="application/json",
                headers=headers
            )
        
        # Simple search
        try:
            search_results = search_client.search(
                search_text=user_message,
                top=3
            )
            
            # Get search context
            context = []
            for result in search_results:
                if hasattr(result, 'content'):
                    context.append(result['content'][:500])
            
            context_text = "\n\n".join(context) if context else "No relevant information found."
            
        except Exception as e:
            logging.error(f'Search failed: {str(e)}')
            context_text = "Search temporarily unavailable."
        
        # Generate response
        try:
            system_prompt = f"""You are Jennifur, a helpful AI assistant with access to business knowledge.
            
Context information:
{context_text}

Based on this context, provide a helpful response to the user's question. If the context doesn't contain relevant information, say so politely."""

            completion = openai_client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_CHAT_MODEL"),
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=800,
                temperature=0.7
            )
            
            response_content = completion.choices[0].message.content
            
        except Exception as e:
            logging.error(f'OpenAI completion failed: {str(e)}')
            response_content = "I'm having trouble generating a response right now. Please try again."
        
        # Return response in expected format
        response = {
            "message": {
                "role": "assistant",
                "content": response_content
            }
        }
        
        return func.HttpResponse(
            json.dumps(response, indent=2),
            status_code=200,
            mimetype="application/json",
            headers=headers
        )
        
    except Exception as e:
        logging.error(f'Error in RAG chat function: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": f"Internal server error: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers=headers
        )