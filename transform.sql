-- Move Validated Data from Staging to Master
INSERT IGNORE INTO master_insurance (
    policy_id, customer_name, region, premium_amount, claim_amount, policy_date
)
SELECT 
    policy_id, customer_name, region, premium_amount, claim_amount, policy_date 
FROM 
    staging_insurance;

-- Clear the Staging Table for the next run
TRUNCATE TABLE staging_insurance;