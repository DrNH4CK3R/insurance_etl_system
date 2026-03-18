-- 1. Create Staging Table (The Landing Zone for Raw ETL Data)
-- We use DECIMAL(10,2) for exact precision on financial amounts.
CREATE TABLE IF NOT EXISTS staging_insurance (
    policy_id VARCHAR(50),
    customer_name VARCHAR(100),
    region VARCHAR(50),
    premium_amount DECIMAL(10, 2),
    claim_amount DECIMAL(10, 2),
    policy_date DATE
);

-- 2. Create Master Table (The Source of Truth)
-- The primary key ensures absolute uniqueness at the database level.
CREATE TABLE IF NOT EXISTS master_insurance (
    policy_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    region VARCHAR(50),
    premium_amount DECIMAL(10, 2),
    claim_amount DECIMAL(10, 2),
    policy_date DATE
);

-- 3. Move Validated Data from Staging to Master
-- INSERT IGNORE tells MySQL to simply skip rows that violate the PRIMARY KEY constraint 
-- (i.e., if the policy_id already exists in the master table from a previous run).
INSERT IGNORE INTO master_insurance (
    policy_id, 
    customer_name, 
    region, 
    premium_amount, 
    claim_amount, 
    policy_date
)
SELECT 
    policy_id, 
    customer_name, 
    region, 
    premium_amount, 
    claim_amount, 
    policy_date 
FROM 
    staging_insurance;

-- 4. Clear the Staging Table
-- TRUNCATE is much faster than DELETE and resets the table for the next ETL batch.
TRUNCATE TABLE staging_insurance;

-- 5. Analytical Reporting View
-- Using DENSE_RANK() to find the highest claims within each specific region.
CREATE OR REPLACE VIEW vw_top_claims_by_region AS
SELECT 
    region,
    policy_id,
    customer_name,
    claim_amount,
    DENSE_RANK() OVER (
        PARTITION BY region 
        ORDER BY claim_amount DESC
    ) AS regional_claim_rank
FROM 
    master_insurance;