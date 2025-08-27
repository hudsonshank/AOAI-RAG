import azure.functions as func
import json
import logging
import datetime
from ..process_single_document import EnhancedDocumentProcessor

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Manual cleanup endpoint for on-demand orphaned document removal
    
    Expected JSON body:
    {
        "site_name": "Clients",
        "base_path": "Documents", 
        "target_subfolders": [
            "08. Financials",
            "04. Roadmaps & Org Charts",
            "05. Behavioral Profiles",
            "15. Meeting Agendas",
            "09. Notes"
        ],
        "dry_run": false  // Optional: set to true to see what would be cleaned without actually doing it
    }
    """
    logging.info('🧹 Manual cleanup triggered')
    
    try:
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "JSON body required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Extract parameters
        site_name = req_body.get('site_name', 'Clients')
        base_path = req_body.get('base_path', 'Documents')
        target_subfolders = req_body.get('target_subfolders', [
            "08. Financials",
            "04. Roadmaps & Org Charts", 
            "05. Behavioral Profiles",
            "15. Meeting Agendas",
            "09. Notes"
        ])
        dry_run = req_body.get('dry_run', False)
        
        logging.info(f'🔍 Manual cleanup for site: {site_name}')
        logging.info(f'📁 Target subfolders: {", ".join(target_subfolders)}')
        logging.info(f'🎭 Dry run mode: {dry_run}')
        
        # Initialize processor
        processor = EnhancedDocumentProcessor()
        
        if dry_run:
            # For dry run, we'll use a modified cleanup that doesn't actually delete
            cleanup_results = processor.preview_cleanup_orphaned_documents(
                site_name=site_name,
                base_path=base_path,
                target_subfolders=target_subfolders
            )
        else:
            # Run actual cleanup
            cleanup_results = processor.cleanup_orphaned_documents(
                site_name=site_name,
                base_path=base_path,
                target_subfolders=target_subfolders
            )
        
        # Return results
        response_data = {
            "cleanup_type": "dry_run" if dry_run else "actual",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "configuration": {
                "site_name": site_name,
                "base_path": base_path,
                "target_subfolders": target_subfolders
            },
            "results": cleanup_results
        }
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f'❌ Error in manual cleanup: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )