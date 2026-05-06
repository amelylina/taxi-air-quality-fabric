CREATE TABLE [meta].[gold_watermark] (

	[gold_table_name] varchar(255) NOT NULL, 
	[last_processed_silver_ts] datetime2(6) NOT NULL, 
	[last_run_at] datetime2(6) NULL, 
	[rows_merged] bigint NULL, 
	[pipeline_run_id] varchar(255) NULL
);