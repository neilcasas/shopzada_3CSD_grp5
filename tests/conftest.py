"""
Pytest configuration and shared fixtures for Shopzada data pipeline tests.
"""
import os
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor


# Database connection settings - uses the existing Docker PostgreSQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "shopzada"),
    "user": os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
}


@pytest.fixture(scope="session")
def db_connection():
    """
    Create a database connection for the entire test session.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """
    Create a cursor for each test function.
    """
    cursor = db_connection.cursor(cursor_factory=RealDictCursor)
    yield cursor
    cursor.close()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_table_row_count(cursor, schema: str, table: str) -> int:
    """Get the row count for a specific table."""
    cursor.execute(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
    result = cursor.fetchone()
    return result["cnt"] if result else 0


def get_duplicate_count(cursor, schema: str, table: str, key_columns: list) -> int:
    """
    Count duplicate records based on key columns.
    Returns the number of duplicate groups (not total duplicate rows).
    """
    key_cols = ", ".join(key_columns)
    query = f"""
        SELECT {key_cols}, COUNT(*) as cnt
        FROM {schema}.{table}
        GROUP BY {key_cols}
        HAVING COUNT(*) > 1
    """
    cursor.execute(query)
    return len(cursor.fetchall())


def get_null_count(cursor, schema: str, table: str, column: str) -> int:
    """Count null values in a specific column."""
    cursor.execute(f"""
        SELECT COUNT(*) as cnt 
        FROM {schema}.{table} 
        WHERE {column} IS NULL OR TRIM({column}::TEXT) = ''
    """)
    result = cursor.fetchone()
    return result["cnt"] if result else 0


def get_orphan_count(
    cursor,
    child_schema: str,
    child_table: str,
    child_column: str,
    parent_schema: str,
    parent_table: str,
    parent_column: str,
) -> int:
    """
    Count orphan records (child records without matching parent).
    """
    query = f"""
        SELECT COUNT(*) as cnt
        FROM {child_schema}.{child_table} c
        LEFT JOIN {parent_schema}.{parent_table} p 
            ON c.{child_column} = p.{parent_column}
        WHERE c.{child_column} IS NOT NULL 
          AND p.{parent_column} IS NULL
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return result["cnt"] if result else 0


def check_data_type_validity(cursor, schema: str, table: str, column: str, expected_type: str) -> dict:
    """
    Check if column values conform to expected data type.
    Returns dict with valid_count and invalid_count.
    """
    # For checking if VARCHAR values can be cast to expected types
    if expected_type == "integer":
        query = f"""
            SELECT 
                COUNT(*) FILTER (WHERE {column} ~ '^-?[0-9]+$') as valid_count,
                COUNT(*) FILTER (WHERE {column} !~ '^-?[0-9]+$' AND {column} IS NOT NULL) as invalid_count
            FROM {schema}.{table}
        """
    elif expected_type == "decimal":
        query = f"""
            SELECT 
                COUNT(*) FILTER (WHERE {column} ~ '^-?[0-9]+(\\.[0-9]+)?$') as valid_count,
                COUNT(*) FILTER (WHERE {column} !~ '^-?[0-9]+(\\.[0-9]+)?$' AND {column} IS NOT NULL) as invalid_count
            FROM {schema}.{table}
        """
    elif expected_type == "date":
        query = f"""
            SELECT 
                COUNT(*) FILTER (WHERE {column}::DATE IS NOT NULL) as valid_count,
                0 as invalid_count
            FROM {schema}.{table}
            WHERE {column} IS NOT NULL
        """
    else:
        return {"valid_count": 0, "invalid_count": 0}
    
    cursor.execute(query)
    result = cursor.fetchone()
    return dict(result) if result else {"valid_count": 0, "invalid_count": 0}


# =============================================================================
# TABLE DEFINITIONS FOR TESTS
# =============================================================================

STAGING_TABLES = {
    "biz_products_raw": {"key_columns": ["product_id"]},
    "cust_credit_cards_raw": {"key_columns": ["user_id"]},
    "cust_user_profiles_raw": {"key_columns": ["user_id"]},
    "cust_user_jobs_raw": {"key_columns": ["user_id"]},
    "ent_merchants_raw": {"key_columns": ["merchant_id"]},
    "ent_order_merchants_raw": {"key_columns": ["order_id"]},
    "ent_staff_raw": {"key_columns": ["staff_id"]},
    "mkt_campaigns_raw": {"key_columns": ["campaign_id"]},
    "mkt_campaign_transactions_raw": {"key_columns": ["order_id", "campaign_id"]},
    "ops_order_item_prices_raw": {"key_columns": ["order_id", "raw_index"]},
    "ops_order_item_products_raw": {"key_columns": ["order_id", "raw_index"]},
    "ops_orders_raw": {"key_columns": ["order_id"]},
    "ops_order_delays_raw": {"key_columns": ["order_id"]},
}

ODS_TABLES = {
    "core_users": {"key_columns": ["user_id"], "pk": "user_id"},
    "core_products": {"key_columns": ["product_id"], "pk": "product_id"},
    "core_merchants": {"key_columns": ["merchant_id"], "pk": "merchant_id"},
    "core_staff": {"key_columns": ["staff_id"], "pk": "staff_id"},
    "core_campaigns": {"key_columns": ["campaign_id"], "pk": "campaign_id"},
    "core_orders": {"key_columns": ["order_id"], "pk": "order_id"},
    "core_line_items": {"key_columns": ["line_item_id"], "pk": "line_item_id"},
}

DW_DIMENSION_TABLES = {
    "dim_user": {"surrogate_key": "user_key", "business_key": "user_id"},
    "dim_product": {"surrogate_key": "product_key", "business_key": "product_id"},
    "dim_merchant": {"surrogate_key": "merchant_key", "business_key": "merchant_id"},
    "dim_staff": {"surrogate_key": "staff_key", "business_key": "staff_id"},
    "dim_campaign": {"surrogate_key": "campaign_key", "business_key": "campaign_id"},
    "dim_date": {"surrogate_key": "date_key", "business_key": "date_key"},
}

DW_FACT_TABLES = {
    "fact_sales": {"surrogate_key": "sales_key", "degenerate_dim": "order_id"},
    "fact_orders": {"surrogate_key": "order_key", "degenerate_dim": "order_id"},
    "fact_campaign_response": {"surrogate_key": "response_key", "degenerate_dim": "order_id"},
}

# Foreign key relationships for referential integrity tests
REFERENTIAL_INTEGRITY_CHECKS = [
    # ODS Layer
    {
        "name": "core_orders -> core_users",
        "child": ("ods", "core_orders", "user_id"),
        "parent": ("ods", "core_users", "user_id"),
    },
    {
        "name": "core_orders -> core_merchants",
        "child": ("ods", "core_orders", "merchant_id"),
        "parent": ("ods", "core_merchants", "merchant_id"),
    },
    {
        "name": "core_orders -> core_staff",
        "child": ("ods", "core_orders", "staff_id"),
        "parent": ("ods", "core_staff", "staff_id"),
    },
    {
        "name": "core_orders -> core_campaigns",
        "child": ("ods", "core_orders", "campaign_id"),
        "parent": ("ods", "core_campaigns", "campaign_id"),
    },
    {
        "name": "core_line_items -> core_orders",
        "child": ("ods", "core_line_items", "order_id"),
        "parent": ("ods", "core_orders", "order_id"),
    },
    {
        "name": "core_line_items -> core_products",
        "child": ("ods", "core_line_items", "product_id"),
        "parent": ("ods", "core_products", "product_id"),
    },
    # DW Layer - Fact to Dimension
    {
        "name": "fact_sales -> dim_user",
        "child": ("dw", "fact_sales", "user_key"),
        "parent": ("dw", "dim_user", "user_key"),
    },
    {
        "name": "fact_sales -> dim_product",
        "child": ("dw", "fact_sales", "product_key"),
        "parent": ("dw", "dim_product", "product_key"),
    },
    {
        "name": "fact_sales -> dim_merchant",
        "child": ("dw", "fact_sales", "merchant_key"),
        "parent": ("dw", "dim_merchant", "merchant_key"),
    },
    {
        "name": "fact_sales -> dim_staff",
        "child": ("dw", "fact_sales", "staff_key"),
        "parent": ("dw", "dim_staff", "staff_key"),
    },
    {
        "name": "fact_sales -> dim_campaign",
        "child": ("dw", "fact_sales", "campaign_key"),
        "parent": ("dw", "dim_campaign", "campaign_key"),
    },
    {
        "name": "fact_sales -> dim_date",
        "child": ("dw", "fact_sales", "order_date_key"),
        "parent": ("dw", "dim_date", "date_key"),
    },
    {
        "name": "fact_orders -> dim_user",
        "child": ("dw", "fact_orders", "user_key"),
        "parent": ("dw", "dim_user", "user_key"),
    },
    {
        "name": "fact_orders -> dim_merchant",
        "child": ("dw", "fact_orders", "merchant_key"),
        "parent": ("dw", "dim_merchant", "merchant_key"),
    },
    {
        "name": "fact_orders -> dim_staff",
        "child": ("dw", "fact_orders", "staff_key"),
        "parent": ("dw", "dim_staff", "staff_key"),
    },
    {
        "name": "fact_orders -> dim_date (order)",
        "child": ("dw", "fact_orders", "order_date_key"),
        "parent": ("dw", "dim_date", "date_key"),
    },
    {
        "name": "fact_orders -> dim_date (arrival)",
        "child": ("dw", "fact_orders", "estimated_arrival_date_key"),
        "parent": ("dw", "dim_date", "date_key"),
    },
    {
        "name": "fact_campaign_response -> dim_user",
        "child": ("dw", "fact_campaign_response", "user_key"),
        "parent": ("dw", "dim_user", "user_key"),
    },
    {
        "name": "fact_campaign_response -> dim_merchant",
        "child": ("dw", "fact_campaign_response", "merchant_key"),
        "parent": ("dw", "dim_merchant", "merchant_key"),
    },
    {
        "name": "fact_campaign_response -> dim_campaign",
        "child": ("dw", "fact_campaign_response", "campaign_key"),
        "parent": ("dw", "dim_campaign", "campaign_key"),
    },
    {
        "name": "fact_campaign_response -> dim_date",
        "child": ("dw", "fact_campaign_response", "transaction_date_key"),
        "parent": ("dw", "dim_date", "date_key"),
    },
]
