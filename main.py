import os
import sys
import logging
from src.database import DatabaseManager
from src.ingestion import FileIngestor
import generate_data

def setup_logger():
    """
    Configures the root logger. 
    Ensures the logs directory exists and routes all output to etl_process.log.
    """
    os.makedirs('logs', exist_ok=True)

    # Configure the logging format to include timestamps and severity levels
    logging.basicConfig(
        filename='logs/etl_process.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S'
    )

def check_for_files():
    """Checks for CSVs. Prompts the user to generate dummy data if empty."""
    incoming_dir = "data/incoming"
    os.makedirs(incoming_dir, exist_ok=True)
    
    files = [f for f in os.listdir(incoming_dir) if f.endswith('.csv')]
    
    if not files:
        print("\n--- No CSV files detected in data/incoming/ ---")
        user_input = input("Would you like to generate a messy dummy CSV file to test the pipeline? (y/n): ").strip().lower()
        
        if user_input == 'y':
            generate_data.create_dummy_csv()
        else:
            print("Exiting program. Please place a CSV file in data/incoming/ to run the ETL.")
            sys.exit(0)

if __name__ == "__main__":
    # 1. Initialize logging before doing anything else
    setup_logger()
    logger = logging.getLogger(__name__)

    try:
        # Step 0: Check for files and prompt user interactively in the terminal
        check_for_files()
        
        logger.info("==========================================")
        logger.info("=== Insurance ETL Pipeline Initiated   ===")
        
        # 1. Setup the Database Schema (Staging, Master, Views)
        logger.info("Phase 1: Verifying database schema...")
        with DatabaseManager() as db:
            db.execute_schema("report.sql")
            
        # 2. Trigger the Ingestion Workflow
        logger.info("Phase 2: Starting file ingestion workflow...")
        ingestor = FileIngestor()
        ingestor.process_files()

        # Step 3: Transform and Load (Moves data to Master, clears Staging)
        logger.info("Phase 3: Moving data to Master and cleaning Staging...")
        with DatabaseManager() as db:
            db.execute_schema("transform.sql")
        
        logger.info("=== Insurance ETL Pipeline Completed   ===")
        logger.info("==========================================")

    except Exception as e:
        # Catch any catastrophic failure that slipped past the lower levels
        logger.critical(f"FATAL ERROR: Pipeline terminated unexpectedly: {e}", exc_info=True)
        sys.exit(1)