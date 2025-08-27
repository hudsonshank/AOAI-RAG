import azure.functions as func
import json
import logging
import datetime
from ..process_single_document import EnhancedDocumentProcessor

def main(timer: func.TimerRequest) -> None:
    """
    Scheduled cleanup function that runs weekly to remove orphaned documents
    
    Schedule: Every Sunday at 2:00 AM UTC
    Cron expression: "0 0 2 * * 0"
    """
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ).isoformat()

    if timer.past_due:
        logging.info('⏰ The timer is past due!')

    logging.info(f'🧹 Scheduled cleanup started at {utc_timestamp}')
    
    try:
        # Initialize the document processor
        processor = EnhancedDocumentProcessor()
        
        # Configuration for cleanup
        site_name = "Clients"
        base_path = "Documents"
        target_subfolders = [
            "08. Financials",
            "04. Roadmaps & Org Charts", 
            "05. Behavioral Profiles",
            "15. Meeting Agendas",
            "09. Notes"
        ]
        
        logging.info(f'🔍 Starting cleanup for site: {site_name}')
        logging.info(f'📁 Target subfolders: {", ".join(target_subfolders)}')
        
        # Run the cleanup
        cleanup_results = processor.cleanup_orphaned_documents(
            site_name=site_name,
            base_path=base_path, 
            target_subfolders=target_subfolders
        )
        
        # Log detailed results
        logging.info('✅ Scheduled cleanup completed successfully!')
        logging.info(f'📊 Cleanup Results:')
        logging.info(f'   - Documents checked: {cleanup_results["documents_checked"]}')
        logging.info(f'   - Documents removed: {cleanup_results["documents_removed"]}')
        logging.info(f'   - Errors encountered: {len(cleanup_results["errors"])}')
        
        if cleanup_results["errors"]:
            logging.warning('⚠️ Errors during cleanup:')
            for error in cleanup_results["errors"]:
                logging.warning(f'   - {error}')
        
        # Optional: Send notification if significant cleanup occurred
        if cleanup_results["documents_removed"] > 10:
            logging.info(f'🚨 Significant cleanup: {cleanup_results["documents_removed"]} documents removed')
            # Here you could add email/Teams notification logic if needed
        
        logging.info(f'🧹 Scheduled cleanup finished at {datetime.datetime.utcnow().isoformat()}')
        
    except Exception as e:
        logging.error(f'❌ Error during scheduled cleanup: {str(e)}')
        # The function will still complete, but the error is logged
        raise