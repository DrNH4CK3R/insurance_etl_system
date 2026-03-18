import os
import csv
import shutil
import logging
from src.validation import (
    PolicyValidator, NameValidator, RegionValidator, 
    PremiumValidator, ClaimValidator, DateValidator
)
from src.database import DatabaseManager

logger = logging.getLogger(__name__)

class FileIngestor:
    """Orchestrates the reading of CSV files, validation of data, and database insertion."""
    
    def __init__(self):
        self.incoming_dir = "data/incoming"
        self.processed_dir = "data/processed"

        # Instantiate the suite of validators we built using OOP
        self.validators = [
            PolicyValidator(),
            NameValidator(),
            RegionValidator(),
            PremiumValidator(),
            ClaimValidator(),
            DateValidator()
        ]

        # Ensure directories exist so the script doesn't crash on the first run
        os.makedirs(self.incoming_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

    def process_files(self):
        """Scans the incoming directory and processes each CSV found."""

        files = [f for f in os.listdir(self.incoming_dir) if f.endswith('.csv')]

        if not files:
            logger.info("No incoming CSV files detected in the incoming directory.")
            return

        for filename in files:
            self._process_single_file(filename)

    def _process_single_file(self, filename: str):
        filepath = os.path.join(self.incoming_dir, filename)
        logger.info(f"Started processing file: {filename}")
        valid_records = []

        try:
            # utf-8-sig automatically removes the hidden BOM character at the start of some CSVs
            with open(filepath, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)

                for row_num, row in enumerate(reader, start=1):
                    # all() ensures the row passes every single validation rule in our list
                    if all(validator.validate(row) for validator in self.validators):
                        try:
                            # Cast data to exact types and create a tuple for MySQL insertion
                            record = (
                                row['policy_id'].strip(),
                                row['customer_name'].strip(),
                                row['region'].strip().capitalize(),
                                float(row['premium_amount']),
                                float(row['claim_amount']),
                                row['policy_date'].strip()
                            )
                            valid_records.append(record)
                        except ValueError as ve:
                            logger.warning(f"Validation Skipped: Type conversion error on row {row_num}: {ve}")
                    else:
                        # We don't need to log the specific error here, because the Validator classes 
                        # already logged exactly *why* it failed.
                        logger.info(f"Row {row_num} failed validation and was discarded.")

            # Load the cleaned data into the database
            if valid_records:
                self._load_to_database(valid_records)
            else:
                logger.warning(f"No valid records found in {filename} to insert.")

            # File cleanup: safely move the processed file so we don't process it again tomorrow
            processed_filepath = os.path.join(self.processed_dir, filename)
            shutil.move(filepath, processed_filepath)
            logger.info(f"File handling complete. Moved {filename} to processed/.")

        except Exception as e:
            logger.error(f"Critical failure while processing {filename}: {e}")

    def _load_to_database(self, records: list):
        """Handles the Database Manager context manager for bulk insertion."""
        
        try:
            with DatabaseManager() as db:
                # MySQL uses %s for placeholders
                query = """
                    INSERT INTO staging_insurance 
                    (policy_id, customer_name, region, premium_amount, claim_amount, policy_date) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                db.cursor.executemany(query, records)
                logger.info(f"Successfully loaded {len(records)} rows into staging_insurance.")

        except Exception as e:
            logger.error(f"Critical DB failure: {e}")
            raise  # Re-raise so the outer try-except block catches it and doesn't move the file