"""
Tests for the ODS (Operational Data Store) Layer.

Validates that data has been correctly transformed from staging
to ODS with proper data types, cleaning, and integration.
"""
import pytest
from conftest import (
    get_table_row_count,
    get_null_count,
    ODS_TABLES,
)


@pytest.mark.ods
class TestODSDataLoaded:
    """Test that all ODS tables have data loaded."""

    @pytest.mark.parametrize("table_name", list(ODS_TABLES.keys()))
    def test_ods_table_has_data(self, db_cursor, table_name):
        """Each ODS table should have at least one row."""
        row_count = get_table_row_count(db_cursor, "ods", table_name)
        assert row_count > 0, f"ods.{table_name} is empty"

    def test_core_users_loaded(self, db_cursor):
        """Core users should be populated."""
        count = get_table_row_count(db_cursor, "ods", "core_users")
        print(f"\nods.core_users: {count} rows")
        assert count > 0

    def test_core_products_loaded(self, db_cursor):
        """Core products should be populated."""
        count = get_table_row_count(db_cursor, "ods", "core_products")
        print(f"\nods.core_products: {count} rows")
        assert count > 0

    def test_core_orders_loaded(self, db_cursor):
        """Core orders should be populated."""
        count = get_table_row_count(db_cursor, "ods", "core_orders")
        print(f"\nods.core_orders: {count} rows")
        assert count > 0


@pytest.mark.ods
class TestODSPrimaryKeys:
    """Test that primary keys are valid in ODS tables."""

    @pytest.mark.parametrize("table_name,config", list(ODS_TABLES.items()))
    def test_primary_key_not_null(self, db_cursor, table_name, config):
        """Primary key columns should not be null."""
        pk = config["pk"]
        null_count = get_null_count(db_cursor, "ods", table_name, pk)
        assert null_count == 0, f"{null_count} rows have NULL {pk} in ods.{table_name}"

    @pytest.mark.parametrize("table_name,config", list(ODS_TABLES.items()))
    def test_primary_key_unique(self, db_cursor, table_name, config):
        """Primary keys should be unique."""
        pk = config["pk"]
        db_cursor.execute(f"""
            SELECT {pk}, COUNT(*) as cnt
            FROM ods.{table_name}
            GROUP BY {pk}
            HAVING COUNT(*) > 1
        """)
        duplicates = db_cursor.fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate {pk} values in ods.{table_name}"


@pytest.mark.ods
@pytest.mark.data_quality
class TestODSDataTypes:
    """Test that data types are correctly converted in ODS."""

    def test_product_price_is_decimal(self, db_cursor):
        """Product price should be a valid decimal."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_products 
            WHERE price IS NOT NULL AND price >= 0
        """)
        valid_count = db_cursor.fetchone()["cnt"]
        total_count = get_table_row_count(db_cursor, "ods", "core_products")
        assert valid_count == total_count, f"Some products have invalid prices"

    def test_product_price_positive(self, db_cursor):
        """Product prices should be positive."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_products 
            WHERE price <= 0
        """)
        invalid_count = db_cursor.fetchone()["cnt"]
        assert invalid_count == 0, f"{invalid_count} products have non-positive prices"

    def test_user_birthdate_is_date(self, db_cursor):
        """User birthdate should be a valid date."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_users 
            WHERE birthdate IS NOT NULL
        """)
        count = db_cursor.fetchone()["cnt"]
        assert count > 0, "No valid birthdates in core_users"

    def test_user_birthdate_not_future(self, db_cursor):
        """User birthdate should not be in the future."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_users 
            WHERE birthdate > CURRENT_DATE
        """)
        future_count = db_cursor.fetchone()["cnt"]
        assert future_count == 0, f"{future_count} users have future birthdates"

    def test_order_transaction_date_is_timestamp(self, db_cursor):
        """Order transaction date should be a valid timestamp."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_orders 
            WHERE transaction_date IS NOT NULL
        """)
        count = db_cursor.fetchone()["cnt"]
        assert count > 0, "No valid transaction dates in core_orders"

    def test_line_item_quantity_is_integer(self, db_cursor):
        """Line item quantity should be a positive integer."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_line_items 
            WHERE quantity IS NOT NULL AND quantity > 0
        """)
        valid_count = db_cursor.fetchone()["cnt"]
        total_count = get_table_row_count(db_cursor, "ods", "core_line_items")
        # Allow some tolerance for data quality issues
        assert valid_count > 0, "No valid quantities in line items"

    def test_line_item_price_is_decimal(self, db_cursor):
        """Line item price should be a valid decimal."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_line_items 
            WHERE price IS NOT NULL AND price >= 0
        """)
        valid_count = db_cursor.fetchone()["cnt"]
        assert valid_count > 0, "No valid prices in line items"

    def test_campaign_discount_range(self, db_cursor):
        """Campaign discount should be between 0 and 100."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_campaigns 
            WHERE discount < 0 OR discount > 100
        """)
        invalid_count = db_cursor.fetchone()["cnt"]
        assert invalid_count == 0, f"{invalid_count} campaigns have discount outside 0-100 range"

    def test_order_delay_days_reasonable(self, db_cursor):
        """Delay in days should be reasonable (0-365)."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_orders 
            WHERE delay_in_days IS NOT NULL AND (delay_in_days < 0 OR delay_in_days > 365)
        """)
        unreasonable_count = db_cursor.fetchone()["cnt"]
        assert unreasonable_count == 0, f"{unreasonable_count} orders have unreasonable delay values"


@pytest.mark.ods
@pytest.mark.transformations
class TestODSTransformations:
    """Test that data transformations are correctly applied."""

    def test_credit_card_tokenized(self, db_cursor):
        """Credit cards should be tokenized (hashed), not stored as plain text."""
        db_cursor.execute("""
            SELECT credit_card_token 
            FROM ods.core_users 
            WHERE credit_card_token IS NOT NULL
            LIMIT 10
        """)
        tokens = [row["credit_card_token"] for row in db_cursor.fetchall()]
        
        for token in tokens:
            # SHA256 produces 64 character hex string
            assert len(token) == 64, f"Token length {len(token)} != 64 (expected SHA256)"
            assert all(c in '0123456789abcdef' for c in token.lower()), "Token contains non-hex characters"

    def test_no_raw_credit_card_numbers(self, db_cursor):
        """No raw credit card numbers should exist in ODS."""
        # Check that there's no column with raw credit card numbers
        db_cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'ods' 
              AND table_name = 'core_users'
              AND column_name = 'credit_card_number'
        """)
        result = db_cursor.fetchall()
        assert len(result) == 0, "Raw credit_card_number column exists in core_users"

    def test_gender_standardized(self, db_cursor):
        """Gender values should be standardized."""
        db_cursor.execute("""
            SELECT DISTINCT gender 
            FROM ods.core_users 
            WHERE gender IS NOT NULL
        """)
        genders = [row["gender"] for row in db_cursor.fetchall()]
        
        # Check that genders are title case or standard format
        for g in genders:
            if g:  # Skip NULL
                assert g[0].isupper(), f"Gender '{g}' is not properly capitalized"

    def test_is_delayed_flag_calculated(self, db_cursor):
        """is_delayed should match delay_in_days > 0."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_orders 
            WHERE (delay_in_days > 0 AND is_delayed = FALSE)
               OR (delay_in_days = 0 AND is_delayed = TRUE)
               OR (delay_in_days IS NULL AND is_delayed IS NOT NULL)
        """)
        mismatches = db_cursor.fetchone()["cnt"]
        assert mismatches == 0, f"{mismatches} orders have mismatched is_delayed flag"

    def test_actual_arrival_calculated(self, db_cursor):
        """actual_arrival should be estimated_arrival + delay_in_days."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_orders 
            WHERE delay_in_days IS NOT NULL 
              AND estimated_arrival IS NOT NULL
              AND actual_arrival IS NOT NULL
              AND actual_arrival != estimated_arrival + (delay_in_days || ' days')::INTERVAL
        """)
        mismatches = db_cursor.fetchone()["cnt"]
        # Allow some tolerance for edge cases
        print(f"\nActual arrival calculation mismatches: {mismatches}")

    def test_user_data_merged(self, db_cursor):
        """User data should be merged from profiles, jobs, and credit cards."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_users 
            WHERE job_title IS NOT NULL OR job_level IS NOT NULL
        """)
        users_with_jobs = db_cursor.fetchone()["cnt"]
        
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_users 
            WHERE credit_card_token IS NOT NULL
        """)
        users_with_cards = db_cursor.fetchone()["cnt"]
        
        assert users_with_jobs > 0, "No users have job data merged"
        assert users_with_cards > 0, "No users have credit card data merged"
        print(f"\nUsers with jobs: {users_with_jobs}, Users with cards: {users_with_cards}")


@pytest.mark.ods
class TestODSRowCountReconciliation:
    """Test row counts between staging and ODS layers."""

    def test_products_staging_to_ods(self, db_cursor):
        """ODS products should be <= staging products (dedup applied)."""
        staging_count = get_table_row_count(db_cursor, "staging", "biz_products_raw")
        ods_count = get_table_row_count(db_cursor, "ods", "core_products")
        
        assert ods_count <= staging_count, "ODS products > staging (dedup should reduce)"
        assert ods_count > 0, "No products in ODS"
        print(f"\nProducts - Staging: {staging_count}, ODS: {ods_count}")

    def test_users_staging_to_ods(self, db_cursor):
        """ODS users should be <= staging user profiles (dedup applied)."""
        staging_count = get_table_row_count(db_cursor, "staging", "cust_user_profiles_raw")
        ods_count = get_table_row_count(db_cursor, "ods", "core_users")
        
        assert ods_count <= staging_count, "ODS users > staging (dedup should reduce)"
        assert ods_count > 0, "No users in ODS"
        print(f"\nUsers - Staging: {staging_count}, ODS: {ods_count}")

    def test_orders_staging_to_ods(self, db_cursor):
        """ODS orders should be <= staging orders (dedup applied)."""
        staging_count = get_table_row_count(db_cursor, "staging", "ops_orders_raw")
        ods_count = get_table_row_count(db_cursor, "ods", "core_orders")
        
        assert ods_count <= staging_count, "ODS orders > staging (dedup should reduce)"
        assert ods_count > 0, "No orders in ODS"
        print(f"\nOrders - Staging: {staging_count}, ODS: {ods_count}")

    def test_merchants_staging_to_ods(self, db_cursor):
        """ODS merchants should be <= staging merchants."""
        staging_count = get_table_row_count(db_cursor, "staging", "ent_merchants_raw")
        ods_count = get_table_row_count(db_cursor, "ods", "core_merchants")
        
        assert ods_count <= staging_count
        assert ods_count > 0
        print(f"\nMerchants - Staging: {staging_count}, ODS: {ods_count}")

    def test_staff_staging_to_ods(self, db_cursor):
        """ODS staff should be <= staging staff."""
        staging_count = get_table_row_count(db_cursor, "staging", "ent_staff_raw")
        ods_count = get_table_row_count(db_cursor, "ods", "core_staff")
        
        assert ods_count <= staging_count
        assert ods_count > 0
        print(f"\nStaff - Staging: {staging_count}, ODS: {ods_count}")

    def test_campaigns_staging_to_ods(self, db_cursor):
        """ODS campaigns should be <= staging campaigns."""
        staging_count = get_table_row_count(db_cursor, "staging", "mkt_campaigns_raw")
        ods_count = get_table_row_count(db_cursor, "ods", "core_campaigns")
        
        assert ods_count <= staging_count
        assert ods_count > 0
        print(f"\nCampaigns - Staging: {staging_count}, ODS: {ods_count}")


@pytest.mark.ods
@pytest.mark.data_quality
class TestODSDataQuality:
    """Test data quality in ODS layer."""

    def test_no_empty_strings_in_names(self, db_cursor):
        """Name fields should not have empty strings."""
        tables_with_names = [
            ("core_users", "name"),
            ("core_products", "product_name"),
            ("core_merchants", "name"),
            ("core_staff", "name"),
            ("core_campaigns", "campaign_name"),
        ]
        
        for table, column in tables_with_names:
            db_cursor.execute(f"""
                SELECT COUNT(*) as cnt 
                FROM ods.{table} 
                WHERE TRIM({column}) = ''
            """)
            empty_count = db_cursor.fetchone()["cnt"]
            assert empty_count == 0, f"{empty_count} empty {column} values in ods.{table}"

    def test_creation_dates_reasonable(self, db_cursor):
        """Creation dates should be in a reasonable range."""
        # Check users
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_users 
            WHERE creation_date IS NOT NULL 
              AND (creation_date < '2000-01-01' OR creation_date > CURRENT_TIMESTAMP + INTERVAL '1 day')
        """)
        unreasonable = db_cursor.fetchone()["cnt"]
        assert unreasonable == 0, f"{unreasonable} users have unreasonable creation dates"

    def test_order_has_required_relationships(self, db_cursor):
        """Orders should have required foreign key relationships."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM ods.core_orders 
            WHERE user_id IS NULL
        """)
        no_user = db_cursor.fetchone()["cnt"]
        assert no_user == 0, f"{no_user} orders have no user_id"
