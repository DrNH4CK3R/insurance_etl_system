import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Validator:
    """
    Base Class establishing the validation contract.
    All subclasses must implement the validate method.
    """
    def validate(self, row: dict) -> bool:
        raise NotImplementedError("Subclasses must implement validate()")


class PolicyValidator(Validator):
    """Checks for missing policy IDs and enforces uniqueness across the dataset."""
    def __init__(self):
        self.seen_policies = set()

    def validate(self, row: dict) -> bool:
        policy_id = row.get('policy_id', '').strip()
        
        if not policy_id:
            logger.warning(f"Validation Skipped: Empty policy_id. Row data: {row}")
            return False
            
        if policy_id in self.seen_policies:
            logger.warning(f"Validation Skipped: Duplicate policy_id '{policy_id}'. Row data: {row}")
            return False
            
        self.seen_policies.add(policy_id)
        return True


class NameValidator(Validator):
    """Ensures the customer name is present and is a valid string."""
    def validate(self, row: dict) -> bool:
        name = row.get('customer_name', '').strip()
        
        if not name:
            logger.warning(f"Validation Skipped: Empty customer_name. Row data: {row}")
            return False
            
        return True


class RegionValidator(Validator):
    """Ensures the region strictly matches expected categories."""
    def validate(self, row: dict) -> bool:
        valid_regions = {'North', 'South', 'East', 'West'}
        region = row.get('region', '').strip().capitalize()
        
        if region not in valid_regions:
            logger.warning(f"Validation Skipped: Invalid region '{region}'. Expected {valid_regions}. Row data: {row}")
            return False
            
        return True


class PremiumValidator(Validator):
    """Checks that the premium amount is a valid number and strictly greater than zero."""
    def validate(self, row: dict) -> bool:
        try:
            premium = float(row.get('premium_amount', 0))
            if premium <= 0:
                logger.warning(f"Validation Skipped: Premium amount ({premium}) must be > 0. Row data: {row}")
                return False
            return True
        except (ValueError, TypeError):
            logger.warning(f"Validation Skipped: Non-numeric premium_amount. Row data: {row}")
            return False


class ClaimValidator(Validator):
    """Checks that the claim amount is a valid number and not negative."""
    def validate(self, row: dict) -> bool:
        try:
            # A claim can be 0 if they haven't made one, but it cannot be negative
            claim = float(row.get('claim_amount', 0))
            if claim < 0:
                logger.warning(f"Validation Skipped: Claim amount ({claim}) cannot be negative. Row data: {row}")
                return False
            return True
        except (ValueError, TypeError):
            logger.warning(f"Validation Skipped: Non-numeric claim_amount. Row data: {row}")
            return False


class DateValidator(Validator):
    """Ensures the policy date adheres exactly to the YYYY-MM-DD format."""
    def validate(self, row: dict) -> bool:
        date_str = row.get('policy_date', '').strip()
        
        if not date_str:
            logger.warning(f"Validation Skipped: Empty policy_date. Row data: {row}")
            return False
            
        try:
            # This will throw a ValueError if the format doesn't match
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            logger.warning(f"Validation Skipped: Invalid policy_date '{date_str}'. Expected YYY-MM-DD. Row data: {row}")
            return False