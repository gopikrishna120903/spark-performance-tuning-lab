-- Delta Lake table maintenance used alongside the experiments in this repo.

-- Compact small files and co-locate data on common filter columns
OPTIMIZE retail.gold.fact_sales ZORDER BY (store_id, sale_date);

-- Let Databricks size files on write and compact automatically
ALTER TABLE retail.gold.fact_sales SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- Remove files no longer referenced by the table (default retention 7 days)
VACUUM retail.gold.fact_sales;

-- Inspect file counts and sizes before / after
DESCRIBE DETAIL retail.gold.fact_sales;
