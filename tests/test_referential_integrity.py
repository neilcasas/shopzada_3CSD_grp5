"""
Tests for Referential Integrity across all layers.

Validates that foreign key relationships are maintained
and no orphan records exist in ODS or DW layers.
"""
import pytest
from conftest import (
    get_orphan_count,
    REFERENTIAL_INTEGRITY_CHECKS,
)


@pytest.mark.referential_integrity
class TestODSReferentialIntegrity:
    """Test referential integrity in ODS layer."""

    def test_orders_reference_valid_users(self, db_cursor):
        """All orders should reference valid users."""
        orphan_count = get_orphan_count(
            db_cursor,
            "ods", "core_orders", "user_id",
            "ods", "core_users", "user_id"
        )
        assert orphan_count == 0, f"{orphan_count} orders reference non-existent users"

    def test_orders_reference_valid_merchants(self, db_cursor):
        """All orders should reference valid merchants."""
        orphan_count = get_orphan_count(
            db_cursor,
            "ods", "core_orders", "merchant_id",
            "ods", "core_merchants", "merchant_id"
        )
        assert orphan_count == 0, f"{orphan_count} orders reference non-existent merchants"

    def test_orders_reference_valid_staff(self, db_cursor):
        """All orders should reference valid staff."""
        orphan_count = get_orphan_count(
            db_cursor,
            "ods", "core_orders", "staff_id",
            "ods", "core_staff", "staff_id"
        )
        assert orphan_count == 0, f"{orphan_count} orders reference non-existent staff"

    def test_orders_reference_valid_campaigns(self, db_cursor):
        """All orders with campaigns should reference valid campaigns."""
        orphan_count = get_orphan_count(
            db_cursor,
            "ods", "core_orders", "campaign_id",
            "ods", "core_campaigns", "campaign_id"
        )
        assert orphan_count == 0, f"{orphan_count} orders reference non-existent campaigns"

    def test_line_items_reference_valid_orders(self, db_cursor):
        """All line items should reference valid orders."""
        orphan_count = get_orphan_count(
            db_cursor,
            "ods", "core_line_items", "order_id",
            "ods", "core_orders", "order_id"
        )
        assert orphan_count == 0, f"{orphan_count} line items reference non-existent orders"

    def test_line_items_reference_valid_products(self, db_cursor):
        """All line items should reference valid products."""
        orphan_count = get_orphan_count(
            db_cursor,
            "ods", "core_line_items", "product_id",
            "ods", "core_products", "product_id"
        )
        assert orphan_count == 0, f"{orphan_count} line items reference non-existent products"


@pytest.mark.referential_integrity
class TestDWFactSalesReferentialIntegrity:
    """Test referential integrity for fact_sales table."""

    def test_fact_sales_user_key_valid(self, db_cursor):
        """fact_sales user_key should reference valid dim_user."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_sales", "user_key",
            "dw", "dim_user", "user_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_sales rows reference non-existent users"

    def test_fact_sales_product_key_valid(self, db_cursor):
        """fact_sales product_key should reference valid dim_product."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_sales", "product_key",
            "dw", "dim_product", "product_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_sales rows reference non-existent products"

    def test_fact_sales_merchant_key_valid(self, db_cursor):
        """fact_sales merchant_key should reference valid dim_merchant."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_sales", "merchant_key",
            "dw", "dim_merchant", "merchant_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_sales rows reference non-existent merchants"

    def test_fact_sales_staff_key_valid(self, db_cursor):
        """fact_sales staff_key should reference valid dim_staff."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_sales", "staff_key",
            "dw", "dim_staff", "staff_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_sales rows reference non-existent staff"

    def test_fact_sales_campaign_key_valid(self, db_cursor):
        """fact_sales campaign_key should reference valid dim_campaign."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_sales", "campaign_key",
            "dw", "dim_campaign", "campaign_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_sales rows reference non-existent campaigns"

    def test_fact_sales_date_key_valid(self, db_cursor):
        """fact_sales order_date_key should reference valid dim_date."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_sales", "order_date_key",
            "dw", "dim_date", "date_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_sales rows reference non-existent dates"


@pytest.mark.referential_integrity
class TestDWFactOrdersReferentialIntegrity:
    """Test referential integrity for fact_orders table."""

    def test_fact_orders_user_key_valid(self, db_cursor):
        """fact_orders user_key should reference valid dim_user."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_orders", "user_key",
            "dw", "dim_user", "user_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_orders rows reference non-existent users"

    def test_fact_orders_merchant_key_valid(self, db_cursor):
        """fact_orders merchant_key should reference valid dim_merchant."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_orders", "merchant_key",
            "dw", "dim_merchant", "merchant_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_orders rows reference non-existent merchants"

    def test_fact_orders_staff_key_valid(self, db_cursor):
        """fact_orders staff_key should reference valid dim_staff."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_orders", "staff_key",
            "dw", "dim_staff", "staff_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_orders rows reference non-existent staff"

    def test_fact_orders_order_date_key_valid(self, db_cursor):
        """fact_orders order_date_key should reference valid dim_date."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_orders", "order_date_key",
            "dw", "dim_date", "date_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_orders rows reference non-existent order dates"

    def test_fact_orders_arrival_date_key_valid(self, db_cursor):
        """fact_orders estimated_arrival_date_key should reference valid dim_date."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_orders", "estimated_arrival_date_key",
            "dw", "dim_date", "date_key"
        )
        assert orphan_count == 0, f"{orphan_count} fact_orders rows reference non-existent arrival dates"


@pytest.mark.referential_integrity
class TestDWFactCampaignResponseReferentialIntegrity:
    """Test referential integrity for fact_campaign_response table."""

    def test_fact_campaign_response_user_key_valid(self, db_cursor):
        """fact_campaign_response user_key should reference valid dim_user."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_campaign_response", "user_key",
            "dw", "dim_user", "user_key"
        )
        assert orphan_count == 0, f"{orphan_count} campaign responses reference non-existent users"

    def test_fact_campaign_response_merchant_key_valid(self, db_cursor):
        """fact_campaign_response merchant_key should reference valid dim_merchant."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_campaign_response", "merchant_key",
            "dw", "dim_merchant", "merchant_key"
        )
        assert orphan_count == 0, f"{orphan_count} campaign responses reference non-existent merchants"

    def test_fact_campaign_response_campaign_key_valid(self, db_cursor):
        """fact_campaign_response campaign_key should reference valid dim_campaign."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_campaign_response", "campaign_key",
            "dw", "dim_campaign", "campaign_key"
        )
        assert orphan_count == 0, f"{orphan_count} campaign responses reference non-existent campaigns"

    def test_fact_campaign_response_date_key_valid(self, db_cursor):
        """fact_campaign_response transaction_date_key should reference valid dim_date."""
        orphan_count = get_orphan_count(
            db_cursor,
            "dw", "fact_campaign_response", "transaction_date_key",
            "dw", "dim_date", "date_key"
        )
        assert orphan_count == 0, f"{orphan_count} campaign responses reference non-existent dates"


@pytest.mark.referential_integrity
class TestParameterizedReferentialIntegrity:
    """Parameterized tests for all referential integrity checks."""

    @pytest.mark.parametrize("check", REFERENTIAL_INTEGRITY_CHECKS, ids=lambda c: c["name"])
    def test_referential_integrity(self, db_cursor, check):
        """Verify foreign key relationship has no orphans."""
        child_schema, child_table, child_col = check["child"]
        parent_schema, parent_table, parent_col = check["parent"]
        
        orphan_count = get_orphan_count(
            db_cursor,
            child_schema, child_table, child_col,
            parent_schema, parent_table, parent_col
        )
        
        assert orphan_count == 0, (
            f"{check['name']}: {orphan_count} orphan records found in "
            f"{child_schema}.{child_table}.{child_col}"
        )


@pytest.mark.referential_integrity
class TestOrphanDetails:
    """Detailed analysis of orphan records."""

    def test_show_orphan_order_users(self, db_cursor):
        """Show sample orders with missing user references."""
        db_cursor.execute("""
            SELECT o.order_id, o.user_id
            FROM ods.core_orders o
            LEFT JOIN ods.core_users u ON o.user_id = u.user_id
            WHERE o.user_id IS NOT NULL AND u.user_id IS NULL
            LIMIT 5
        """)
        orphans = db_cursor.fetchall()
        
        if orphans:
            print("\nSample orders with missing user references:")
            for row in orphans:
                print(f"  order_id: {row['order_id']}, user_id: {row['user_id']}")

    def test_show_orphan_line_items_orders(self, db_cursor):
        """Show sample line items with missing order references."""
        db_cursor.execute("""
            SELECT li.line_item_id, li.order_id
            FROM ods.core_line_items li
            LEFT JOIN ods.core_orders o ON li.order_id = o.order_id
            WHERE li.order_id IS NOT NULL AND o.order_id IS NULL
            LIMIT 5
        """)
        orphans = db_cursor.fetchall()
        
        if orphans:
            print("\nSample line items with missing order references:")
            for row in orphans:
                print(f"  line_item_id: {row['line_item_id']}, order_id: {row['order_id']}")

    def test_show_orphan_line_items_products(self, db_cursor):
        """Show sample line items with missing product references."""
        db_cursor.execute("""
            SELECT li.line_item_id, li.product_id
            FROM ods.core_line_items li
            LEFT JOIN ods.core_products p ON li.product_id = p.product_id
            WHERE li.product_id IS NOT NULL AND p.product_id IS NULL
            LIMIT 5
        """)
        orphans = db_cursor.fetchall()
        
        if orphans:
            print("\nSample line items with missing product references:")
            for row in orphans:
                print(f"  line_item_id: {row['line_item_id']}, product_id: {row['product_id']}")


@pytest.mark.referential_integrity
class TestCrossLayerConsistency:
    """Test consistency between ODS and DW layers."""

    def test_all_ods_users_in_dw(self, db_cursor):
        """All ODS users should have corresponding DW dimension records."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_users u
            LEFT JOIN dw.dim_user d ON u.user_id = d.user_id
            WHERE d.user_id IS NULL
        """)
        missing = db_cursor.fetchone()["cnt"]
        assert missing == 0, f"{missing} ODS users missing from DW dim_user"

    def test_all_ods_products_in_dw(self, db_cursor):
        """All ODS products should have corresponding DW dimension records."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_products p
            LEFT JOIN dw.dim_product d ON p.product_id = d.product_id
            WHERE d.product_id IS NULL
        """)
        missing = db_cursor.fetchone()["cnt"]
        assert missing == 0, f"{missing} ODS products missing from DW dim_product"

    def test_all_ods_merchants_in_dw(self, db_cursor):
        """All ODS merchants should have corresponding DW dimension records."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_merchants m
            LEFT JOIN dw.dim_merchant d ON m.merchant_id = d.merchant_id
            WHERE d.merchant_id IS NULL
        """)
        missing = db_cursor.fetchone()["cnt"]
        assert missing == 0, f"{missing} ODS merchants missing from DW dim_merchant"

    def test_all_ods_staff_in_dw(self, db_cursor):
        """All ODS staff should have corresponding DW dimension records."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_staff s
            LEFT JOIN dw.dim_staff d ON s.staff_id = d.staff_id
            WHERE d.staff_id IS NULL
        """)
        missing = db_cursor.fetchone()["cnt"]
        assert missing == 0, f"{missing} ODS staff missing from DW dim_staff"

    def test_all_ods_campaigns_in_dw(self, db_cursor):
        """All ODS campaigns should have corresponding DW dimension records."""
        db_cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM ods.core_campaigns c
            LEFT JOIN dw.dim_campaign d ON c.campaign_id = d.campaign_id
            WHERE d.campaign_id IS NULL
        """)
        missing = db_cursor.fetchone()["cnt"]
        assert missing == 0, f"{missing} ODS campaigns missing from DW dim_campaign"


@pytest.mark.referential_integrity
class TestDimensionCoverage:
    """Test that dimension tables cover all referenced keys in facts."""

    def test_dim_date_covers_all_fact_dates(self, db_cursor):
        """dim_date should have entries for all dates referenced in facts."""
        db_cursor.execute("""
            WITH all_date_keys AS (
                SELECT DISTINCT order_date_key as date_key FROM dw.fact_sales WHERE order_date_key IS NOT NULL
                UNION
                SELECT DISTINCT order_date_key FROM dw.fact_orders WHERE order_date_key IS NOT NULL
                UNION
                SELECT DISTINCT estimated_arrival_date_key FROM dw.fact_orders WHERE estimated_arrival_date_key IS NOT NULL
                UNION
                SELECT DISTINCT transaction_date_key FROM dw.fact_campaign_response WHERE transaction_date_key IS NOT NULL
            )
            SELECT COUNT(*) as cnt
            FROM all_date_keys adk
            LEFT JOIN dw.dim_date dd ON adk.date_key = dd.date_key
            WHERE dd.date_key IS NULL
        """)
        missing = db_cursor.fetchone()["cnt"]
        assert missing == 0, f"{missing} date keys referenced in facts but missing from dim_date"
