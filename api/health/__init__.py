import json
import logging
import azure.functions as func
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint for debugging
    """
    logging.info('Health check triggered')
    
    # Set CORS headers
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
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
        # Check environment variables
        required_vars = [
            'AZURE_SEARCH_ENDPOINT',
            'AZURE_SEARCH_ADMIN_KEY',
            'AZURE_OPENAI_API_KEY',
            'AZURE_OPENAI_ENDPOINT',
            'EXISTING_INDEX_NAME'
        ]
        
        env_status = {}
        for var in required_vars:
            env_status[var] = "✅ Set" if os.getenv(var) else "❌ Missing"
        
        response_data = {
            "status": "healthy",
            "environment_variables": env_status,
            "function_app": "Azure Static Web App API",
            "version": "jennifur-v1.0"
        }
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            mimetype="application/json",
            headers=headers
        )
        
    except Exception as e:
        logging.error(f'Health check error: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": f"Health check failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers=headers
        )