import os
import csv
import random

def create_dummy_csv():
    """Generates a CSV file with a mix of valid and intentionally messy data."""
    
    incoming_dir = "data/incoming"
    os.makedirs(incoming_dir, exist_ok=True)
    filepath = os.path.join(incoming_dir, "messy_dummy_data.csv")

    # A mix of perfect rows and rows that violate our business rules
    dummy_data = [
        # Valid row
        {"policy_id": "POL001", "customer_name": "Alice Smith", "region": "North", "premium_amount": 150.50, "claim_amount": 0.00, "policy_date": "2023-01-15"},
        # Valid row
        {"policy_id": "POL002", "customer_name": "Bob Johnson", "region": "South", "premium_amount": 200.00, "claim_amount": 500.00, "policy_date": "2023-02-20"},
        # INVALID: Duplicate Policy ID (Will be skipped by PolicyValidator)
        {"policy_id": "POL001", "customer_name": "Alice Smith Copy", "region": "North", "premium_amount": 150.50, "claim_amount": 0.00, "policy_date": "2023-01-15"},
        # INVALID: Missing Policy ID
        {"policy_id": "", "customer_name": "Charlie Brown", "region": "East", "premium_amount": 300.00, "claim_amount": 0.00, "policy_date": "2023-03-10"},
        # INVALID: Negative Premium Amount
        {"policy_id": "POL003", "customer_name": "Diana Prince", "region": "West", "premium_amount": -50.00, "claim_amount": 100.00, "policy_date": "2023-04-05"},
        # INVALID: Invalid Region ('Central' is not in our allowed list)
        {"policy_id": "POL004", "customer_name": "Eve Adams", "region": "Central", "premium_amount": 120.00, "claim_amount": 0.00, "policy_date": "2023-05-12"},
        # INVALID: Bad Date Format
        {"policy_id": "POL005", "customer_name": "Frank Castle", "region": "East", "premium_amount": 400.00, "claim_amount": 2000.00, "policy_date": "15-06-2023"},
        # Valid row to finish
        {"policy_id": "POL006", "customer_name": "Grace Hopper", "region": "North", "premium_amount": 800.00, "claim_amount": 0.00, "policy_date": "2023-07-01"}
    ]

    # Shuffle the data so the errors happen randomly during processing
    random.shuffle(dummy_data)

    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["policy_id", "customer_name", "region", "premium_amount", "claim_amount", "policy_date"])
        writer.writeheader()
        writer.writerows(dummy_data)
        
    print(f"\n[+] Success: Generated 8 rows of test data at {filepath}")
    print("[+] This file contains deliberate errors to test the ETL logging system.\n")