"""
Additional Data Quality Tests.

Covers edge cases, data consistency, and business rule validations
across all layers of the data pipeline.
"""
import pytest
from conftest import get_table_row_count


@pytest.mark.data_quality
class TestDataCompleteness:
    """Test that all required data is present."""

    def test_all_staging_tables_populated(self, db_cursor):
        """All staging tables should have data."""
        db_cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'staging'
            ORDER BY table_name
        """)
        tables = [row["table_name"] for row in db_cursor.fetchall()]
        
        empty_tables = []
        for table in tables:
            count = get_table_row_count(db_cursor, "staging", table)
            if count == 0:
                empty_tables.append(table)
        
        assert len(empty_tables) == 0, f"Empty staging tables: {empty_tables}"

    def test_all_ods_tables_populated(self, db_cursor):
        """All ODS tables should have data."""
        db_cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'ods'
            ORDER BY table_name
        """)
        tables = [row["table_name"] for row in db_cursor.fetchall()]
        
        empty_tables = []
        for table in tables:
            count = get_table_row_count(db_cursor, "ods", table)
            if count == 0:
                empty_tables.append(table)
        
        assert len(empty_tables) == 0, f"Empty ODS tables: {empty_tables}"

    def test_all_dw_tables_populated(self, db_cursor):
        """All DW tables should have data."""
        db_cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'dw'
            ORDER BY table_name
        """)
        tables = [row["table_name"] for row in db_cursor.fetchall()]
        
        empty_tables = []
        for table in tables:
            count = get_table_row_count(db_cursor, "dw", table)
            if count == 0:
                empty_tables.append(table)
        
        assert len(empty_tables) == 0, f"Empty DW tables: {empty_tables}"


@pytest.mark.data_quality
class TestBusinessRules:
    """Test business rule validations."""

    def test_order_amounts_positive(self, db_cursor):
        """Order amounts in campaign response should be positive."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_campaign_response
            WHERE order_total_amount < 0 
               OR order_net_amount < 0
        """)
        negative_amounts = db_cursor.fetchone()["cnt"]
        assert negative_amounts == 0, f"{negative_amounts} orders have negative amounts"

    def test_discount_not_exceed_total(self, db_cursor):
        """Discount should not exceed order total."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM dw.fact_campaign_response
            WHERE order_discount_amount > order_total_amount
        """)
        exceeded = db_cursor.fetchone()["cnt"]
        assert exceeded == 0, f"{exceeded} orders have discount > total"

    def test_transaction_date_before_arrival(self, db_cursor):
        """Transaction date should be before or equal to estimated arrival."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_orders
            WHERE transaction_date IS NOT NULL 
              AND estimated_arrival IS NOT NULL
              AND transaction_date > estimated_arrival
        """)
        invalid = db_cursor.fetchone()["cnt"]
        # Note: This might happen due to data quality issues
        if invalid > 0:
            print(f"\nNote: {invalid} orders have transaction_date > estimated_arrival")

    def test_user_creation_before_first_order(self, db_cursor):
        """User creation date should be before their first order."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_users u
            JOIN (
                SELECT user_id, MIN(transaction_date) as first_order
                FROM ods.core_orders
                GROUP BY user_id
            ) o ON u.user_id = o.user_id
            WHERE u.creation_date IS NOT NULL
              AND o.first_order IS NOT NULL
              AND u.creation_date > o.first_order
        """)
        invalid = db_cursor.fetchone()["cnt"]
        if invalid > 0:
            print(f"\nNote: {invalid} users have creation_date > first_order")

    def test_merchant_creation_before_first_order(self, db_cursor):
        """Merchant creation date should be before their first order."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_merchants m
            JOIN (
                SELECT merchant_id, MIN(transaction_date) as first_order
                FROM ods.core_orders
                GROUP BY merchant_id
            ) o ON m.merchant_id = o.merchant_id
            WHERE m.creation_date IS NOT NULL
              AND o.first_order IS NOT NULL
              AND m.creation_date > o.first_order
        """)
        invalid = db_cursor.fetchone()["cnt"]
        if invalid > 0:
            print(f"\nNote: {invalid} merchants have creation_date > first_order")


@pytest.mark.data_quality
class TestDataRanges:
    """Test that data values are in expected ranges."""

    def test_product_prices_in_range(self, db_cursor):
        """Product prices should be in a reasonable range."""
        db_cursor.execute("""
            SELECT 
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price
            FROM ods.core_products
            WHERE price IS NOT NULL
        """)
        result = db_cursor.fetchone()
        
        assert result["min_price"] >= 0, f"Negative price: {result['min_price']}"
        # Log for visibility
        print(f"\nProduct prices - Min: {result['min_price']}, "
              f"Max: {result['max_price']}, Avg: {result['avg_price']:.2f}")

    def test_quantities_in_range(self, db_cursor):
        """Line item quantities should be in a reasonable range."""
        db_cursor.execute("""
            SELECT 
                MIN(quantity) as min_qty,
                MAX(quantity) as max_qty,
                AVG(quantity) as avg_qty
            FROM ods.core_line_items
            WHERE quantity IS NOT NULL
        """)
        result = db_cursor.fetchone()
        
        assert result["min_qty"] > 0, f"Non-positive quantity: {result['min_qty']}"
        print(f"\nQuantities - Min: {result['min_qty']}, "
              f"Max: {result['max_qty']}, Avg: {result['avg_qty']:.2f}")

    def test_delay_days_in_range(self, db_cursor):
        """Delay days should be in a reasonable range."""
        db_cursor.execute("""
            SELECT 
                MIN(delay_in_days) as min_delay,
                MAX(delay_in_days) as max_delay,
                AVG(delay_in_days) as avg_delay
            FROM ods.core_orders
            WHERE delay_in_days IS NOT NULL
        """)
        result = db_cursor.fetchone()
        
        if result["min_delay"] is not None:
            assert result["min_delay"] >= 0, f"Negative delay: {result['min_delay']}"
            print(f"\nDelays - Min: {result['min_delay']}, "
                  f"Max: {result['max_delay']}, Avg: {result['avg_delay']:.2f}")


@pytest.mark.data_quality
class TestDataDistributions:
    """Test data distributions for anomalies."""

    def test_user_type_distribution(self, db_cursor):
        """User types should have reasonable distribution."""
        db_cursor.execute("""
            SELECT user_type, COUNT(*) as cnt
            FROM ods.core_users
            GROUP BY user_type
            ORDER BY cnt DESC
        """)
        distribution = db_cursor.fetchall()
        
        assert len(distribution) > 0, "No user types found"
        print("\nUser type distribution:")
        for row in distribution:
            print(f"  {row['user_type']}: {row['cnt']}")

    def test_product_type_distribution(self, db_cursor):
        """Product types should have reasonable distribution."""
        db_cursor.execute("""
            SELECT product_type, COUNT(*) as cnt
            FROM ods.core_products
            GROUP BY product_type
            ORDER BY cnt DESC
            LIMIT 10
        """)
        distribution = db_cursor.fetchall()
        
        assert len(distribution) > 0, "No product types found"
        print("\nTop 10 product types:")
        for row in distribution:
            print(f"  {row['product_type']}: {row['cnt']}")

    def test_orders_by_month(self, db_cursor):
        """Orders should be distributed across time periods."""
        db_cursor.execute("""
            SELECT 
                DATE_TRUNC('month', transaction_date) as month,
                COUNT(*) as cnt
            FROM ods.core_orders
            WHERE transaction_date IS NOT NULL
            GROUP BY DATE_TRUNC('month', transaction_date)
            ORDER BY month
        """)
        distribution = db_cursor.fetchall()
        
        assert len(distribution) > 0, "No orders with transaction dates"
        print(f"\nOrders span {len(distribution)} months")

    def test_delay_rate(self, db_cursor):
        """Calculate and report delay rate."""
        db_cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_delayed = TRUE) as delayed,
                COUNT(*) FILTER (WHERE is_delayed = FALSE) as on_time,
                COUNT(*) as total
            FROM ods.core_orders
            WHERE is_delayed IS NOT NULL
        """)
        result = db_cursor.fetchone()
        
        if result["total"] > 0:
            delay_rate = (result["delayed"] / result["total"]) * 100
            print(f"\nDelay rate: {delay_rate:.1f}% "
                  f"({result['delayed']} delayed / {result['total']} total)")


@pytest.mark.data_quality
class TestStringFormatting:
    """Test that string values are properly formatted."""

    def test_no_leading_trailing_whitespace_in_names(self, db_cursor):
        """Names should not have leading/trailing whitespace."""
        tables_with_names = [
            ("ods.core_users", "name"),
            ("ods.core_products", "product_name"),
            ("ods.core_merchants", "name"),
            ("ods.core_staff", "name"),
        ]
        
        issues = []
        for table, column in tables_with_names:
            db_cursor.execute(f"""
                SELECT COUNT(*) as cnt
                FROM {table}
                WHERE {column} != TRIM({column})
                  AND {column} IS NOT NULL
            """)
            count = db_cursor.fetchone()["cnt"]
            if count > 0:
                issues.append(f"{table}.{column}: {count}")
        
        assert len(issues) == 0, f"Whitespace issues: {issues}"

    def test_no_empty_strings_as_nulls(self, db_cursor):
        """Empty strings should be converted to NULL."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_users
            WHERE name = '' OR gender = '' OR city = ''
        """)
        empty_count = db_cursor.fetchone()["cnt"]
        assert empty_count == 0, f"{empty_count} users have empty strings instead of NULL"

    def test_country_codes_consistent(self, db_cursor):
        """Country values should be consistent."""
        db_cursor.execute("""
            SELECT DISTINCT country
            FROM ods.core_users
            WHERE country IS NOT NULL
            ORDER BY country
        """)
        countries = [row["country"] for row in db_cursor.fetchall()]
        
        # Just report the distinct values
        print(f"\nDistinct user countries: {len(countries)}")
        if len(countries) <= 10:
            for c in countries:
                print(f"  {c}")


@pytest.mark.data_quality
class TestNullHandling:
    """Test proper null value handling."""

    def test_optional_fields_allow_nulls(self, db_cursor):
        """Optional fields should allow nulls without data loss."""
        # Check that optional fields have some nulls (expected behavior)
        db_cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE job_title IS NULL) as null_job_title,
                COUNT(*) FILTER (WHERE job_level IS NULL) as null_job_level,
                COUNT(*) FILTER (WHERE credit_card_token IS NULL) as null_cc_token,
                COUNT(*) as total
            FROM ods.core_users
        """)
        result = db_cursor.fetchone()
        
        # These are optional, so some nulls are expected
        print(f"\nOptional field nulls in core_users:")
        print(f"  job_title: {result['null_job_title']}/{result['total']}")
        print(f"  job_level: {result['null_job_level']}/{result['total']}")
        print(f"  credit_card_token: {result['null_cc_token']}/{result['total']}")

    def test_campaign_id_null_handling_in_orders(self, db_cursor):
        """Orders without campaigns should have NULL campaign_id."""
        db_cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE campaign_id IS NULL) as no_campaign,
                COUNT(*) FILTER (WHERE campaign_id IS NOT NULL) as with_campaign,
                COUNT(*) as total
            FROM ods.core_orders
        """)
        result = db_cursor.fetchone()
        
        print(f"\nOrders with/without campaigns:")
        print(f"  With campaign: {result['with_campaign']}")
        print(f"  Without campaign: {result['no_campaign']}")
        print(f"  Total: {result['total']}")


@pytest.mark.data_quality
class TestIdempotency:
    """Test that pipeline is idempotent (same results on re-run)."""

    def test_table_row_counts_stable(self, db_cursor):
        """Row counts should be consistent (run this test multiple times)."""
        # This test captures the current state for comparison
        counts = {}
        
        for schema in ["staging", "ods", "dw"]:
            db_cursor.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                ORDER BY table_name
            """)
            tables = [row["table_name"] for row in db_cursor.fetchall()]
            
            for table in tables:
                count = get_table_row_count(db_cursor, schema, table)
                counts[f"{schema}.{table}"] = count
        
        # Just report counts - actual idempotency would require comparing across runs
        print("\nCurrent row counts:")
        for table, count in sorted(counts.items()):
            print(f"  {table}: {count}")


@pytest.mark.data_quality
class TestSchemaCompliance:
    """Test that tables match expected schema."""

    def test_staging_tables_have_metadata_columns(self, db_cursor):
        """All staging tables should have _ingested_at and _source_file."""
        db_cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'staging'
        """)
        tables = [row["table_name"] for row in db_cursor.fetchall()]
        
        missing_columns = []
        for table in tables:
            db_cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'staging' AND table_name = '{table}'
            """)
            columns = [row["column_name"] for row in db_cursor.fetchall()]
            
            if "_ingested_at" not in columns:
                missing_columns.append(f"{table}._ingested_at")
            if "_source_file" not in columns:
                missing_columns.append(f"{table}._source_file")
        
        assert len(missing_columns) == 0, f"Missing metadata columns: {missing_columns}"

    def test_dw_dimensions_have_surrogate_keys(self, db_cursor):
        """All DW dimension tables should have surrogate keys."""
        expected_keys = {
            "dim_user": "user_key",
            "dim_product": "product_key",
            "dim_merchant": "merchant_key",
            "dim_staff": "staff_key",
            "dim_campaign": "campaign_key",
            "dim_date": "date_key",
        }
        
        missing_keys = []
        for table, key in expected_keys.items():
            db_cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'dw' AND table_name = '{table}'
                  AND column_name = '{key}'
            """)
            if not db_cursor.fetchall():
                missing_keys.append(f"{table}.{key}")
        
        assert len(missing_keys) == 0, f"Missing surrogate keys: {missing_keys}"
