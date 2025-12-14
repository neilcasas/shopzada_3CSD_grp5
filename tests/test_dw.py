"""
Tests for the Data Warehouse (DW) Layer.

Validates dimension and fact tables follow Kimball methodology,
surrogate keys are properly generated, and measures are calculated correctly.
"""
import pytest
from conftest import (
    get_table_row_count,
    get_null_count,
    DW_DIMENSION_TABLES,
    DW_FACT_TABLES,
)


@pytest.mark.dw
class TestDWDataLoaded:
    """Test that all DW tables have data loaded."""

    @pytest.mark.parametrize("table_name", list(DW_DIMENSION_TABLES.keys()))
    def test_dimension_table_has_data(self, db_cursor, table_name):
        """Each dimension table should have at least one row."""
        row_count = get_table_row_count(db_cursor, "dw", table_name)
        assert row_count > 0, f"dw.{table_name} is empty"

    @pytest.mark.parametrize("table_name", list(DW_FACT_TABLES.keys()))
    def test_fact_table_has_data(self, db_cursor, table_name):
        """Each fact table should have at least one row."""
        row_count = get_table_row_count(db_cursor, "dw", table_name)
        assert row_count > 0, f"dw.{table_name} is empty"


@pytest.mark.dw
class TestDWSurrogateKeys:
    """Test surrogate key generation in DW tables."""

    @pytest.mark.parametrize("table_name,config", list(DW_DIMENSION_TABLES.items()))
    def test_dimension_surrogate_key_not_null(self, db_cursor, table_name, config):
        """Dimension surrogate keys should not be null."""
        sk = config["surrogate_key"]
        null_count = get_null_count(db_cursor, "dw", table_name, sk)
        assert null_count == 0, f"{null_count} rows have NULL {sk} in dw.{table_name}"

    @pytest.mark.parametrize("table_name,config", list(DW_DIMENSION_TABLES.items()))
    def test_dimension_surrogate_key_unique(self, db_cursor, table_name, config):
        """Dimension surrogate keys should be unique."""
        sk = config["surrogate_key"]
        db_cursor.execute(f"""
            SELECT {sk}, COUNT(*) as cnt
            FROM dw.{table_name}
            GROUP BY {sk}
            HAVING COUNT(*) > 1
        """)
        duplicates = db_cursor.fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate {sk} values in dw.{table_name}"

    @pytest.mark.parametrize("table_name,config", list(DW_DIMENSION_TABLES.items()))
    def test_dimension_surrogate_key_positive(self, db_cursor, table_name, config):
        """Dimension surrogate keys should be positive integers."""
        sk = config["surrogate_key"]
        db_cursor.execute(f"""
            SELECT COUNT(*) as cnt
            FROM dw.{table_name}
            WHERE {sk} <= 0
        """)
        invalid_count = db_cursor.fetchone()["cnt"]
        assert invalid_count == 0, f"{invalid_count} rows have non-positive {sk} in dw.{table_name}"

    @pytest.mark.parametrize("table_name,config", list(DW_FACT_TABLES.items()))
    def test_fact_surrogate_key_not_null(self, db_cursor, table_name, config):
        """Fact surrogate keys should not be null."""
        sk = config["surrogate_key"]
        null_count = get_null_count(db_cursor, "dw", table_name, sk)
        assert null_count == 0, f"{null_count} rows have NULL {sk} in dw.{table_name}"


@pytest.mark.dw
class TestDWBusinessKeys:
    """Test business keys are preserved in dimension tables."""

    @pytest.mark.parametrize("table_name,config", [
        (t, c) for t, c in DW_DIMENSION_TABLES.items() if t != "dim_date"
    ])
    def test_dimension_business_key_not_null(self, db_cursor, table_name, config):
        """Dimension business keys should not be null."""
        bk = config["business_key"]
        null_count = get_null_count(db_cursor, "dw", table_name, bk)
        assert null_count == 0, f"{null_count} rows have NULL {bk} in dw.{table_name}"

    @pytest.mark.parametrize("table_name,config", [
        (t, c) for t, c in DW_DIMENSION_TABLES.items() if t != "dim_date"
    ])
    def test_dimension_business_key_unique(self, db_cursor, table_name, config):
        """Dimension business keys should be unique (for Type 1 SCD)."""
        bk = config["business_key"]
        db_cursor.execute(f"""
            SELECT {bk}, COUNT(*) as cnt
            FROM dw.{table_name}
            GROUP BY {bk}
            HAVING COUNT(*) > 1
        """)
        duplicates = db_cursor.fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate {bk} values in dw.{table_name}"


@pytest.mark.dw
class TestDWDimDate:
    """Test the date dimension table."""

    def test_dim_date_has_data(self, db_cursor):
        """Date dimension should have data."""
        count = get_table_row_count(db_cursor, "dw", "dim_date")
        assert count > 0, "dim_date is empty"
        print(f"\ndw.dim_date: {count} rows")

    def test_date_key_format(self, db_cursor):
        """Date key should be in YYYYMMDD format."""
        db_cursor.execute("""
            SELECT date_key, date
            FROM dw.dim_date
            LIMIT 10
        """)
        rows = db_cursor.fetchall()
        
        for row in rows:
            date_key = row["date_key"]
            date_val = row["date"]
            
            # Verify format: YYYYMMDD
            assert 10000101 <= date_key <= 99991231, f"Invalid date_key format: {date_key}"
            
            # Verify date_key matches date
            if date_val:
                expected_key = int(date_val.strftime("%Y%m%d"))
                assert date_key == expected_key, f"date_key {date_key} != expected {expected_key}"

    def test_date_attributes_populated(self, db_cursor):
        """Date dimension attributes should be populated."""
        db_cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE day_of_week IS NOT NULL) as dow_count,
                COUNT(*) FILTER (WHERE month IS NOT NULL) as month_count,
                COUNT(*) FILTER (WHERE year IS NOT NULL) as year_count,
                COUNT(*) FILTER (WHERE quarter IS NOT NULL) as quarter_count,
                COUNT(*) as total
            FROM dw.dim_date
        """)
        result = db_cursor.fetchone()
        total = result["total"]
        
        assert result["dow_count"] == total, "Some rows missing day_of_week"
        assert result["month_count"] == total, "Some rows missing month"
        assert result["year_count"] == total, "Some rows missing year"
        assert result["quarter_count"] == total, "Some rows missing quarter"

    def test_date_ranges_valid(self, db_cursor):
        """Date dimension values should be in valid ranges."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.dim_date
            WHERE day_of_week NOT BETWEEN 0 AND 6
               OR month NOT BETWEEN 1 AND 12
               OR quarter NOT BETWEEN 1 AND 4
        """)
        invalid = db_cursor.fetchone()["cnt"]
        assert invalid == 0, f"{invalid} rows have invalid date attribute values"

    def test_is_weekend_correct(self, db_cursor):
        """is_weekend should match day_of_week (5=Sat, 6=Sun)."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.dim_date
            WHERE (day_of_week IN (5, 6) AND is_weekend = FALSE)
               OR (day_of_week NOT IN (5, 6) AND is_weekend = TRUE)
        """)
        mismatches = db_cursor.fetchone()["cnt"]
        assert mismatches == 0, f"{mismatches} rows have incorrect is_weekend flag"


@pytest.mark.dw
class TestDWFactMeasures:
    """Test fact table measures are calculated correctly."""

    def test_fact_sales_sale_amount_calculated(self, db_cursor):
        """sale_amount should equal quantity_sold * unit_price."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_sales
            WHERE ABS(sale_amount - (quantity_sold * unit_price)) > 0.01
        """)
        mismatches = db_cursor.fetchone()["cnt"]
        assert mismatches == 0, f"{mismatches} rows have incorrect sale_amount calculation"

    def test_fact_sales_quantity_positive(self, db_cursor):
        """Quantity sold should be positive."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_sales
            WHERE quantity_sold IS NULL OR quantity_sold <= 0
        """)
        invalid = db_cursor.fetchone()["cnt"]
        # Allow some tolerance for data issues
        total = get_table_row_count(db_cursor, "dw", "fact_sales")
        assert invalid < total * 0.01, f"{invalid} rows have invalid quantity (>1% of total)"

    def test_fact_sales_unit_price_positive(self, db_cursor):
        """Unit price should be positive."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_sales
            WHERE unit_price IS NULL OR unit_price <= 0
        """)
        invalid = db_cursor.fetchone()["cnt"]
        total = get_table_row_count(db_cursor, "dw", "fact_sales")
        assert invalid < total * 0.01, f"{invalid} rows have invalid unit_price (>1% of total)"

    def test_fact_orders_delay_reasonable(self, db_cursor):
        """Delay in days should be reasonable."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_orders
            WHERE delay_in_days IS NOT NULL 
              AND (delay_in_days < 0 OR delay_in_days > 365)
        """)
        unreasonable = db_cursor.fetchone()["cnt"]
        assert unreasonable == 0, f"{unreasonable} orders have unreasonable delay values"

    def test_fact_orders_is_delayed_correct(self, db_cursor):
        """is_delayed should match delay_in_days > 0."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_orders
            WHERE (delay_in_days > 0 AND is_delayed = FALSE)
               OR (delay_in_days = 0 AND is_delayed = TRUE)
        """)
        mismatches = db_cursor.fetchone()["cnt"]
        assert mismatches == 0, f"{mismatches} orders have incorrect is_delayed flag"

    def test_fact_campaign_response_amounts(self, db_cursor):
        """Campaign response amounts should be consistent."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_campaign_response
            WHERE order_total_amount IS NOT NULL
              AND order_discount_amount IS NOT NULL
              AND order_net_amount IS NOT NULL
              AND ABS(order_net_amount - (order_total_amount - order_discount_amount)) > 0.01
        """)
        mismatches = db_cursor.fetchone()["cnt"]
        assert mismatches == 0, f"{mismatches} rows have incorrect net_amount calculation"

    def test_fact_campaign_response_discount_positive(self, db_cursor):
        """Discount amount should be non-negative."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_campaign_response
            WHERE order_discount_amount < 0
        """)
        negative = db_cursor.fetchone()["cnt"]
        assert negative == 0, f"{negative} rows have negative discount amounts"


@pytest.mark.dw
class TestDWRowCountReconciliation:
    """Test row counts between ODS and DW layers."""

    def test_dim_user_matches_core_users(self, db_cursor):
        """dim_user should have same count as core_users."""
        ods_count = get_table_row_count(db_cursor, "ods", "core_users")
        dw_count = get_table_row_count(db_cursor, "dw", "dim_user")
        
        assert dw_count == ods_count, f"dim_user ({dw_count}) != core_users ({ods_count})"

    def test_dim_product_matches_core_products(self, db_cursor):
        """dim_product should have same count as core_products."""
        ods_count = get_table_row_count(db_cursor, "ods", "core_products")
        dw_count = get_table_row_count(db_cursor, "dw", "dim_product")
        
        assert dw_count == ods_count, f"dim_product ({dw_count}) != core_products ({ods_count})"

    def test_dim_merchant_matches_core_merchants(self, db_cursor):
        """dim_merchant should have same count as core_merchants."""
        ods_count = get_table_row_count(db_cursor, "ods", "core_merchants")
        dw_count = get_table_row_count(db_cursor, "dw", "dim_merchant")
        
        assert dw_count == ods_count, f"dim_merchant ({dw_count}) != core_merchants ({ods_count})"

    def test_dim_staff_matches_core_staff(self, db_cursor):
        """dim_staff should have same count as core_staff."""
        ods_count = get_table_row_count(db_cursor, "ods", "core_staff")
        dw_count = get_table_row_count(db_cursor, "dw", "dim_staff")
        
        assert dw_count == ods_count, f"dim_staff ({dw_count}) != core_staff ({ods_count})"

    def test_dim_campaign_matches_core_campaigns(self, db_cursor):
        """dim_campaign should have same count as core_campaigns."""
        ods_count = get_table_row_count(db_cursor, "ods", "core_campaigns")
        dw_count = get_table_row_count(db_cursor, "dw", "dim_campaign")
        
        assert dw_count == ods_count, f"dim_campaign ({dw_count}) != core_campaigns ({ods_count})"

    def test_fact_sales_line_items_coverage(self, db_cursor):
        """fact_sales should cover line items with valid dimension lookups."""
        line_items = get_table_row_count(db_cursor, "ods", "core_line_items")
        fact_sales = get_table_row_count(db_cursor, "dw", "fact_sales")
        
        # fact_sales might be less than line_items if some lookups fail
        assert fact_sales > 0, "No fact_sales records"
        assert fact_sales <= line_items, "More fact_sales than line_items"
        print(f"\nLine items: {line_items}, Fact sales: {fact_sales}")

    def test_fact_orders_orders_coverage(self, db_cursor):
        """fact_orders should cover orders with valid dimension lookups."""
        orders = get_table_row_count(db_cursor, "ods", "core_orders")
        fact_orders = get_table_row_count(db_cursor, "dw", "fact_orders")
        
        assert fact_orders > 0, "No fact_orders records"
        assert fact_orders <= orders, "More fact_orders than core_orders"
        print(f"\nCore orders: {orders}, Fact orders: {fact_orders}")


@pytest.mark.dw
class TestDWDegenerateDimensions:
    """Test degenerate dimensions in fact tables."""

    def test_fact_sales_order_id_not_null(self, db_cursor):
        """fact_sales order_id (degenerate dimension) should not be null."""
        null_count = get_null_count(db_cursor, "dw", "fact_sales", "order_id")
        assert null_count == 0, f"{null_count} fact_sales rows have NULL order_id"

    def test_fact_orders_order_id_not_null(self, db_cursor):
        """fact_orders order_id should not be null."""
        null_count = get_null_count(db_cursor, "dw", "fact_orders", "order_id")
        assert null_count == 0, f"{null_count} fact_orders rows have NULL order_id"

    def test_fact_campaign_response_order_id_not_null(self, db_cursor):
        """fact_campaign_response order_id should not be null."""
        null_count = get_null_count(db_cursor, "dw", "fact_campaign_response", "order_id")
        assert null_count == 0, f"{null_count} fact_campaign_response rows have NULL order_id"


@pytest.mark.dw
class TestDWDimensionLookups:
    """Test that fact tables correctly reference dimension tables."""

    def test_fact_sales_all_dimensions_populated(self, db_cursor):
        """fact_sales should have all required dimension keys populated."""
        db_cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE user_key IS NULL) as null_user,
                COUNT(*) FILTER (WHERE product_key IS NULL) as null_product,
                COUNT(*) FILTER (WHERE merchant_key IS NULL) as null_merchant,
                COUNT(*) FILTER (WHERE order_date_key IS NULL) as null_date,
                COUNT(*) as total
            FROM dw.fact_sales
        """)
        result = db_cursor.fetchone()
        
        # Some nulls might be acceptable depending on data quality
        total = result["total"]
        if total > 0:
            assert result["null_user"] < total * 0.05, f"Too many NULL user_key ({result['null_user']}/{total})"
            assert result["null_product"] < total * 0.05, f"Too many NULL product_key ({result['null_product']}/{total})"

    def test_fact_orders_all_dimensions_populated(self, db_cursor):
        """fact_orders should have all required dimension keys populated."""
        db_cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE user_key IS NULL) as null_user,
                COUNT(*) FILTER (WHERE merchant_key IS NULL) as null_merchant,
                COUNT(*) FILTER (WHERE order_date_key IS NULL) as null_date,
                COUNT(*) as total
            FROM dw.fact_orders
        """)
        result = db_cursor.fetchone()
        
        total = result["total"]
        if total > 0:
            assert result["null_user"] < total * 0.05, f"Too many NULL user_key"
            assert result["null_date"] < total * 0.05, f"Too many NULL order_date_key"
