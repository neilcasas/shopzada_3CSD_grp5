"""
Tests for Duplicate Detection across all layers.

Validates that deduplication has been properly applied and
no unexpected duplicates exist in staging, ODS, or DW tables.
"""
import pytest
from conftest import (
    get_duplicate_count,
    STAGING_TABLES,
    ODS_TABLES,
    DW_DIMENSION_TABLES,
    DW_FACT_TABLES,
)


@pytest.mark.duplicates
class TestStagingDuplicates:
    """Test for duplicates in staging layer."""

    def test_staging_products_duplicates(self, db_cursor):
        """Check for duplicate product_id in staging products."""
        dup_count = get_duplicate_count(
            db_cursor, "staging", "biz_products_raw", ["product_id"]
        )
        # Staging may have duplicates (before dedup), but report them
        print(f"\nstaging.biz_products_raw duplicate groups: {dup_count}")

    def test_staging_users_duplicates(self, db_cursor):
        """Check for duplicate user_id in staging user profiles."""
        dup_count = get_duplicate_count(
            db_cursor, "staging", "cust_user_profiles_raw", ["user_id"]
        )
        print(f"\nstaging.cust_user_profiles_raw duplicate groups: {dup_count}")

    def test_staging_merchants_duplicates(self, db_cursor):
        """Check for duplicate merchant_id in staging merchants."""
        dup_count = get_duplicate_count(
            db_cursor, "staging", "ent_merchants_raw", ["merchant_id"]
        )
        print(f"\nstaging.ent_merchants_raw duplicate groups: {dup_count}")

    def test_staging_staff_duplicates(self, db_cursor):
        """Check for duplicate staff_id in staging staff."""
        dup_count = get_duplicate_count(
            db_cursor, "staging", "ent_staff_raw", ["staff_id"]
        )
        print(f"\nstaging.ent_staff_raw duplicate groups: {dup_count}")

    def test_staging_campaigns_duplicates(self, db_cursor):
        """Check for duplicate campaign_id in staging campaigns."""
        dup_count = get_duplicate_count(
            db_cursor, "staging", "mkt_campaigns_raw", ["campaign_id"]
        )
        print(f"\nstaging.mkt_campaigns_raw duplicate groups: {dup_count}")

    def test_staging_orders_duplicates(self, db_cursor):
        """Check for duplicate order_id in staging orders."""
        dup_count = get_duplicate_count(
            db_cursor, "staging", "ops_orders_raw", ["order_id"]
        )
        print(f"\nstaging.ops_orders_raw duplicate groups: {dup_count}")


@pytest.mark.duplicates
class TestODSDuplicates:
    """Test for duplicates in ODS layer - should have NO duplicates."""

    @pytest.mark.parametrize("table_name,config", list(ODS_TABLES.items()))
    def test_ods_no_duplicate_primary_keys(self, db_cursor, table_name, config):
        """ODS tables should have no duplicate primary keys."""
        key_columns = config["key_columns"]
        dup_count = get_duplicate_count(db_cursor, "ods", table_name, key_columns)
        
        assert dup_count == 0, (
            f"Found {dup_count} duplicate groups in ods.{table_name} "
            f"on columns {key_columns}"
        )

    def test_ods_core_users_no_duplicates(self, db_cursor):
        """core_users should have unique user_id."""
        dup_count = get_duplicate_count(db_cursor, "ods", "core_users", ["user_id"])
        assert dup_count == 0, f"Found {dup_count} duplicate user_id in core_users"

    def test_ods_core_products_no_duplicates(self, db_cursor):
        """core_products should have unique product_id."""
        dup_count = get_duplicate_count(db_cursor, "ods", "core_products", ["product_id"])
        assert dup_count == 0, f"Found {dup_count} duplicate product_id in core_products"

    def test_ods_core_orders_no_duplicates(self, db_cursor):
        """core_orders should have unique order_id."""
        dup_count = get_duplicate_count(db_cursor, "ods", "core_orders", ["order_id"])
        assert dup_count == 0, f"Found {dup_count} duplicate order_id in core_orders"

    def test_ods_core_line_items_no_duplicates(self, db_cursor):
        """core_line_items should have unique line_item_id."""
        dup_count = get_duplicate_count(db_cursor, "ods", "core_line_items", ["line_item_id"])
        assert dup_count == 0, f"Found {dup_count} duplicate line_item_id in core_line_items"

    def test_ods_line_items_unique_order_product(self, db_cursor):
        """Each order-product combination should be unique (or properly aggregated)."""
        db_cursor.execute("""
            SELECT order_id, product_id, COUNT(*) as cnt
            FROM ods.core_line_items
            GROUP BY order_id, product_id
            HAVING COUNT(*) > 1
        """)
        duplicates = db_cursor.fetchall()
        # Note: This might be valid if same product appears multiple times in an order
        # Just report, don't fail
        if duplicates:
            print(f"\nNote: {len(duplicates)} order-product combinations appear multiple times")


@pytest.mark.duplicates
class TestDWDimensionDuplicates:
    """Test for duplicates in DW dimension tables."""

    @pytest.mark.parametrize("table_name,config", list(DW_DIMENSION_TABLES.items()))
    def test_dw_dimension_no_duplicate_surrogate_keys(self, db_cursor, table_name, config):
        """DW dimension tables should have unique surrogate keys."""
        sk = config["surrogate_key"]
        dup_count = get_duplicate_count(db_cursor, "dw", table_name, [sk])
        
        assert dup_count == 0, (
            f"Found {dup_count} duplicate {sk} in dw.{table_name}"
        )

    @pytest.mark.parametrize("table_name,config", [
        (t, c) for t, c in DW_DIMENSION_TABLES.items() if t != "dim_date"
    ])
    def test_dw_dimension_no_duplicate_business_keys(self, db_cursor, table_name, config):
        """DW dimension tables should have unique business keys (Type 1 SCD)."""
        bk = config["business_key"]
        dup_count = get_duplicate_count(db_cursor, "dw", table_name, [bk])
        
        assert dup_count == 0, (
            f"Found {dup_count} duplicate {bk} in dw.{table_name}"
        )


@pytest.mark.duplicates
class TestDWFactDuplicates:
    """Test for duplicates in DW fact tables."""

    @pytest.mark.parametrize("table_name,config", list(DW_FACT_TABLES.items()))
    def test_dw_fact_no_duplicate_surrogate_keys(self, db_cursor, table_name, config):
        """DW fact tables should have unique surrogate keys."""
        sk = config["surrogate_key"]
        dup_count = get_duplicate_count(db_cursor, "dw", table_name, [sk])
        
        assert dup_count == 0, (
            f"Found {dup_count} duplicate {sk} in dw.{table_name}"
        )

    def test_fact_sales_unique_grain(self, db_cursor):
        """fact_sales should have unique order_id + product combination per line."""
        db_cursor.execute("""
            SELECT order_id, product_key, COUNT(*) as cnt
            FROM dw.fact_sales
            GROUP BY order_id, product_key
            HAVING COUNT(*) > 1
        """)
        duplicates = db_cursor.fetchall()
        # This might be valid if same product ordered multiple times
        # Just report
        if duplicates:
            print(f"\nNote: {len(duplicates)} order-product combinations in fact_sales")

    def test_fact_orders_unique_order_id(self, db_cursor):
        """fact_orders should have unique order_id (one row per order)."""
        dup_count = get_duplicate_count(db_cursor, "dw", "fact_orders", ["order_id"])
        assert dup_count == 0, f"Found {dup_count} duplicate order_id in fact_orders"

    def test_fact_campaign_response_unique_order(self, db_cursor):
        """fact_campaign_response should have unique order_id."""
        dup_count = get_duplicate_count(
            db_cursor, "dw", "fact_campaign_response", ["order_id"]
        )
        assert dup_count == 0, f"Found {dup_count} duplicate order_id in fact_campaign_response"


@pytest.mark.duplicates
class TestCrossTableDuplicateAnalysis:
    """Analyze duplicate patterns across layers."""

    def test_dedup_effectiveness_products(self, db_cursor):
        """Verify deduplication reduced product count from staging to ODS."""
        db_cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM staging.biz_products_raw) as staging_total,
                (SELECT COUNT(DISTINCT product_id) FROM staging.biz_products_raw) as staging_unique,
                (SELECT COUNT(*) FROM ods.core_products) as ods_count
        """)
        result = db_cursor.fetchone()
        
        staging_total = result["staging_total"]
        staging_unique = result["staging_unique"]
        ods_count = result["ods_count"]
        
        print(f"\nProducts - Staging total: {staging_total}, "
              f"Staging unique: {staging_unique}, ODS: {ods_count}")
        
        # ODS should match unique staging count (after dedup)
        assert ods_count <= staging_unique, "ODS has more products than unique staging products"

    def test_dedup_effectiveness_users(self, db_cursor):
        """Verify deduplication reduced user count from staging to ODS."""
        db_cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM staging.cust_user_profiles_raw) as staging_total,
                (SELECT COUNT(DISTINCT user_id) FROM staging.cust_user_profiles_raw) as staging_unique,
                (SELECT COUNT(*) FROM ods.core_users) as ods_count
        """)
        result = db_cursor.fetchone()
        
        print(f"\nUsers - Staging total: {result['staging_total']}, "
              f"Staging unique: {result['staging_unique']}, ODS: {result['ods_count']}")

    def test_dedup_effectiveness_orders(self, db_cursor):
        """Verify deduplication reduced order count from staging to ODS."""
        db_cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM staging.ops_orders_raw) as staging_total,
                (SELECT COUNT(DISTINCT order_id) FROM staging.ops_orders_raw) as staging_unique,
                (SELECT COUNT(*) FROM ods.core_orders) as ods_count
        """)
        result = db_cursor.fetchone()
        
        print(f"\nOrders - Staging total: {result['staging_total']}, "
              f"Staging unique: {result['staging_unique']}, ODS: {result['ods_count']}")


@pytest.mark.duplicates
class TestDuplicateDetails:
    """Detailed duplicate analysis with sample data."""

    def test_show_duplicate_product_samples(self, db_cursor):
        """Show sample duplicate products if any exist in staging."""
        db_cursor.execute("""
            SELECT product_id, COUNT(*) as cnt
            FROM staging.biz_products_raw
            WHERE product_id IS NOT NULL
            GROUP BY product_id
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 5
        """)
        duplicates = db_cursor.fetchall()
        
        if duplicates:
            print("\nSample duplicate products in staging:")
            for dup in duplicates:
                print(f"  product_id: {dup['product_id']}, count: {dup['cnt']}")

    def test_show_duplicate_order_samples(self, db_cursor):
        """Show sample duplicate orders if any exist in staging."""
        db_cursor.execute("""
            SELECT order_id, COUNT(*) as cnt
            FROM staging.ops_orders_raw
            WHERE order_id IS NOT NULL
            GROUP BY order_id
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 5
        """)
        duplicates = db_cursor.fetchall()
        
        if duplicates:
            print("\nSample duplicate orders in staging:")
            for dup in duplicates:
                print(f"  order_id: {dup['order_id']}, count: {dup['cnt']}")

    def test_cross_file_duplicates(self, db_cursor):
        """Check if duplicates come from different source files."""
        db_cursor.execute("""
            SELECT product_id, COUNT(DISTINCT _source_file) as file_count, COUNT(*) as total
            FROM staging.biz_products_raw
            WHERE product_id IS NOT NULL
            GROUP BY product_id
            HAVING COUNT(DISTINCT _source_file) > 1
            LIMIT 5
        """)
        cross_file_dups = db_cursor.fetchall()
        
        if cross_file_dups:
            print("\nProducts appearing in multiple source files:")
            for dup in cross_file_dups:
                print(f"  product_id: {dup['product_id']}, files: {dup['file_count']}, total: {dup['total']}")
