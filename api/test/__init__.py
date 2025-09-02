import json
import logging
import azure.functions as func
import os
import sys

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Simple test endpoint to debug imports and basic functionality
    """
    logging.info('Test endpoint triggered')
    
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
        # Test 1: Basic request handling
        req_body = req.get_json()
        messages = req_body.get('messages', []) if req_body else []
        
        result = {
            "test": "basic_functionality",
            "status": "✅ Basic request handling works",
            "messages_received": len(messages),
            "python_version": sys.version,
            "path": sys.path[:3]  # First 3 path entries
        }
        
        # Test 2: Try importing required modules
        try:
            from azure.search.documents import SearchClient
            result["azure_search_import"] = "✅ Success"
        except Exception as e:
            result["azure_search_import"] = f"❌ Failed: {str(e)}"
        
        try:
            from openai import AsyncAzureOpenAI
            result["openai_import"] = "✅ Success"
        except Exception as e:
            result["openai_import"] = f"❌ Failed: {str(e)}"
        
        # Test 3: Try importing our RAG engine
        try:
            sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
            from api.client_aware_rag import ClientAwareRAGEngine
            result["rag_engine_import"] = "✅ Success"
        except Exception as e:
            result["rag_engine_import"] = f"❌ Failed: {str(e)}"
        
        # Test 4: Try initializing RAG engine
        try:
            rag_engine = ClientAwareRAGEngine()
            result["rag_engine_init"] = "✅ Success"
        except Exception as e:
            result["rag_engine_init"] = f"❌ Failed: {str(e)}"
        
        return func.HttpResponse(
            json.dumps(result, indent=2),
            status_code=200,
            mimetype="application/json",
            headers=headers
        )
        
    except Exception as e:
        logging.error(f'Test endpoint error: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": f"Test failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers=headers
        )