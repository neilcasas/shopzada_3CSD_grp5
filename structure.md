# Project Dataset Structure

## Business Department

- **product_list.xlsx** (XLSX)
  - Columns:
    - Unnamed: 0 (exported row index)
    - product_id
    - product_name
    - product_type
    - price

## Customer Management Department

- **user_credit_card.pickle** (Pickle)

  - Columns:
    - user_id
    - name
    - credit_card_number
    - issuing_bank

- **user_data.json** (JSON)

  - Fields (column-oriented objects keyed by row index):
    - user_id
    - name
    - gender
    - birthdate
    - street
    - city
    - state
    - country
    - device_address
    - creation_date
    - user_type

- **user_job.csv** (CSV)
  - Columns:
    - Unnamed: 0 (index from export)
    - user_id
    - name
    - job_title
    - job_level

## Enterprise Department

- **merchant_data.html** (HTML table)

  - Columns:
    - Unnamed: 0 (table index)
    - merchant_id
    - creation_date
    - name
    - street
    - state
    - city
    - country
    - contact_number

- **order_with_merchant_data1.parquet** (Parquet)

  - Columns:
    - order_id
    - merchant_id
    - staff_id

- **order_with_merchant_data2.parquet** (Parquet)

  - Columns:
    - order_id
    - merchant_id
    - staff_id

- **order_with_merchant_data3.csv** (CSV)

  - Columns:
    - Unnamed: 0 (row index)
    - order_id
    - merchant_id
    - staff_id

- **staff_data.html** (HTML table)
  - Columns:
    - Unnamed: 0 (table index)
    - staff_id
    - name
    - job_level
    - street
    - state
    - city
    - country
    - contact_number
    - creation_date

## Marketing Department

- **campaign_data.csv** (CSV)

  - Columns:
    - Unnamed: 0 (row index)
    - campaign_id
    - campaign_name
    - campaign_description
    - discount

- **transactional_campaign_data.csv** (CSV)
  - Columns:
    - Unnamed: 0 (row index)
    - transaction_date
    - campaign_id
    - order_id
    - estimated arrival
    - availed

## Operations Department

- **line_item_data_prices1.csv** (CSV)

  - Columns:
    - Unnamed: 0 (row index)
    - order_id
    - price
    - quantity

- **line_item_data_prices2.csv** (CSV)

  - Columns:
    - Unnamed: 0 (row index)
    - order_id
    - price
    - quantity

- **line_item_data_prices3.parquet** (Parquet)

  - Columns:
    - Unnamed: 0
    - order_id
    - price
    - quantity

- **line_item_data_products1.csv** (CSV)

  - Columns:
    - Unnamed: 0 (row index)
    - order_id
    - product_name
    - product_id

- **line_item_data_products2.csv** (CSV)

  - Columns:
    - Unnamed: 0 (row index)
    - order_id
    - product_name
    - product_id

- **line_item_data_products3.parquet** (Parquet)

  - Columns:
    - Unnamed: 0
    - order_id
    - product_name
    - product_id

- **order_data_20200101-20200701.parquet** (Parquet)

  - Columns:
    - order_id
    - user_id
    - estimated arrival
    - transaction_date

- **order_data_20200701-20211001.pickle** (Pickle)

  - Columns:
    - order_id
    - user_id
    - estimated arrival
    - transaction_date

- **order_data_20211001-20220101.csv** (CSV)

  - Columns:
    - Unnamed: 0 (row index)
    - order_id
    - user_id
    - estimated arrival
    - transaction_date

- **order_data_20221201-20230601.json** (JSON)

  - Fields (column-oriented objects keyed by row index):
    - order_id
    - user_id
    - estimated arrival
    - transaction_date

- **order_data_20230601-20240101.html** (HTML table)

  - Columns:
    - Unnamed: 0 (table index)
    - order_id
    - user_id
    - estimated arrival
    - transaction_date

- **order_delays.html** (HTML table)
  - Columns:
    - Unnamed: 0 (table index)
    - order_id
    - delay in days
