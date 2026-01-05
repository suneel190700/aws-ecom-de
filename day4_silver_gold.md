Problem found: Bronze order items had duplicates

Bronze total 28 vs distinct 14

Solution: Silver de-duplicates line items

Silver outputs: customers/products/order_items as partitioned Parquet

Gold output: daily sales KPI table with Athena CTAS

Commands/queries used (the key ones)
