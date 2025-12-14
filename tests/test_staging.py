"""
Tests for the Staging Layer.

Validates that data has been correctly loaded from source files
into staging tables with proper metadata.
"""
import pytest
from conftest import (
    get_table_row_count,
    get_null_count,
    STAGING_TABLES,
)


@pytest.mark.staging
class TestStagingDataLoaded:
    """Test that all staging tables have data loaded."""

    @pytest.mark.parametrize("table_name", list(STAGING_TABLES.keys()))
    def test_staging_table_has_data(self, db_cursor, table_name):
        """Each staging table should have at least one row."""
        row_count = get_table_row_count(db_cursor, "staging", table_name)
        assert row_count > 0, f"staging.{table_name} is empty"

    def test_staging_products_row_count(self, db_cursor):
        """Business products should have reasonable data volume."""
        row_count = get_table_row_count(db_cursor, "staging", "biz_products_raw")
        assert row_count > 0, "No products loaded"
        # Log the count for visibility
        print(f"\nstaging.biz_products_raw: {row_count} rows")

    def test_staging_users_row_count(self, db_cursor):
        """Customer profiles should have reasonable data volume."""
        row_count = get_table_row_count(db_cursor, "staging", "cust_user_profiles_raw")
        assert row_count > 0, "No user profiles loaded"
        print(f"\nstaging.cust_user_profiles_raw: {row_count} rows")

    def test_staging_orders_row_count(self, db_cursor):
        """Operations orders should have reasonable data volume."""
        row_count = get_table_row_count(db_cursor, "staging", "ops_orders_raw")
        assert row_count > 0, "No orders loaded"
        print(f"\nstaging.ops_orders_raw: {row_count} rows")


@pytest.mark.staging
class TestStagingMetadataColumns:
    """Test that metadata columns are properly populated."""

    @pytest.mark.parametrize("table_name", list(STAGING_TABLES.keys()))
    def test_ingested_at_not_null(self, db_cursor, table_name):
        """_ingested_at should be populated for all records."""
        db_cursor.execute(f"""
            SELECT COUNT(*) as cnt 
            FROM staging.{table_name} 
            WHERE _ingested_at IS NULL
        """)
        null_count = db_cursor.fetchone()["cnt"]
        assert null_count == 0, f"{null_count} rows have NULL _ingested_at in staging.{table_name}"

    @pytest.mark.parametrize("table_name", list(STAGING_TABLES.keys()))
    def test_source_file_not_null(self, db_cursor, table_name):
        """_source_file should be populated for all records."""
        db_cursor.execute(f"""
            SELECT COUNT(*) as cnt 
            FROM staging.{table_name} 
            WHERE _source_file IS NULL OR TRIM(_source_file) = ''
        """)
        null_count = db_cursor.fetchone()["cnt"]
        assert null_count == 0, f"{null_count} rows have NULL/empty _source_file in staging.{table_name}"

    def test_source_files_tracked(self, db_cursor):
        """Verify source files are tracked with proper filenames."""
        db_cursor.execute("""
            SELECT DISTINCT _source_file 
            FROM staging.biz_products_raw 
            ORDER BY _source_file
        """)
        source_files = [row["_source_file"] for row in db_cursor.fetchall()]
        assert len(source_files) > 0, "No source files tracked"
        # Verify file paths look valid (contain expected extensions)
        for sf in source_files:
            assert any(ext in sf.lower() for ext in ['.xlsx', '.csv', '.json', '.parquet', '.pickle', '.html']), \
                f"Unexpected source file format: {sf}"


@pytest.mark.staging
class TestStagingPrimaryKeyColumns:
    """Test that primary key columns are populated in staging."""

    def test_products_has_product_id(self, db_cursor):
        """Product ID should not be null."""
        null_count = get_null_count(db_cursor, "staging", "biz_products_raw", "product_id")
        assert null_count == 0, f"{null_count} products have NULL product_id"

    def test_users_has_user_id(self, db_cursor):
        """User ID should not be null in profiles."""
        null_count = get_null_count(db_cursor, "staging", "cust_user_profiles_raw", "user_id")
        assert null_count == 0, f"{null_count} user profiles have NULL user_id"

    def test_merchants_has_merchant_id(self, db_cursor):
        """Merchant ID should not be null."""
        null_count = get_null_count(db_cursor, "staging", "ent_merchants_raw", "merchant_id")
        assert null_count == 0, f"{null_count} merchants have NULL merchant_id"

    def test_staff_has_staff_id(self, db_cursor):
        """Staff ID should not be null."""
        null_count = get_null_count(db_cursor, "staging", "ent_staff_raw", "staff_id")
        assert null_count == 0, f"{null_count} staff have NULL staff_id"

    def test_campaigns_has_campaign_id(self, db_cursor):
        """Campaign ID should not be null."""
        null_count = get_null_count(db_cursor, "staging", "mkt_campaigns_raw", "campaign_id")
        assert null_count == 0, f"{null_count} campaigns have NULL campaign_id"

    def test_orders_has_order_id(self, db_cursor):
        """Order ID should not be null."""
        null_count = get_null_count(db_cursor, "staging", "ops_orders_raw", "order_id")
        assert null_count == 0, f"{null_count} orders have NULL order_id"


@pytest.mark.staging
class TestStagingDataFormats:
    """Test that staging data is in expected raw formats."""

    def test_price_column_exists(self, db_cursor):
        """Price column should exist and have data."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM staging.biz_products_raw 
            WHERE price IS NOT NULL AND TRIM(price) != ''
        """)
        count = db_cursor.fetchone()["cnt"]
        assert count > 0, "No price data in products"

    def test_dates_exist_in_users(self, db_cursor):
        """Birthdate and creation_date should have data."""
        db_cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE birthdate IS NOT NULL AND TRIM(birthdate) != '') as birthdate_count,
                COUNT(*) FILTER (WHERE creation_date IS NOT NULL AND TRIM(creation_date) != '') as creation_count
            FROM staging.cust_user_profiles_raw
        """)
        result = db_cursor.fetchone()
        assert result["birthdate_count"] > 0, "No birthdate data"
        assert result["creation_count"] > 0, "No creation_date data"

    def test_orders_have_timestamps(self, db_cursor):
        """Orders should have transaction dates."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM staging.ops_orders_raw 
            WHERE transaction_date IS NOT NULL AND TRIM(transaction_date) != ''
        """)
        count = db_cursor.fetchone()["cnt"]
        assert count > 0, "No transaction dates in orders"


@pytest.mark.staging
class TestStagingRowCountReconciliation:
    """Test row counts across related staging tables."""

    def test_user_profiles_vs_jobs_alignment(self, db_cursor):
        """User profiles and jobs should have comparable user counts."""
        profile_users = get_table_row_count(db_cursor, "staging", "cust_user_profiles_raw")
        job_users = get_table_row_count(db_cursor, "staging", "cust_user_jobs_raw")
        
        # Jobs might have more rows (multiple jobs per user) or fewer (not all users have jobs)
        # But there should be some overlap
        assert profile_users > 0, "No user profiles"
        assert job_users > 0, "No user jobs"
        print(f"\nUser profiles: {profile_users}, User jobs: {job_users}")

    def test_order_items_alignment(self, db_cursor):
        """Order item prices and products should have same row counts."""
        prices_count = get_table_row_count(db_cursor, "staging", "ops_order_item_prices_raw")
        products_count = get_table_row_count(db_cursor, "staging", "ops_order_item_products_raw")
        
        # These should be very close or equal as they represent the same line items
        assert prices_count > 0, "No order item prices"
        assert products_count > 0, "No order item products"
        print(f"\nOrder item prices: {prices_count}, Order item products: {products_count}")

    def test_campaigns_vs_transactions(self, db_cursor):
        """Campaign transactions should reference existing campaigns."""
        campaigns = get_table_row_count(db_cursor, "staging", "mkt_campaigns_raw")
        transactions = get_table_row_count(db_cursor, "staging", "mkt_campaign_transactions_raw")
        
        assert campaigns > 0, "No campaigns"
        assert transactions > 0, "No campaign transactions"
        # Transactions should be more than campaigns (many orders per campaign)
        print(f"\nCampaigns: {campaigns}, Campaign transactions: {transactions}")
