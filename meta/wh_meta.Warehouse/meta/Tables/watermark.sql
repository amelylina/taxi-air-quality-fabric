CREATE TABLE [meta].[watermark] (

	[target_table] varchar(255) NOT NULL, 
	[source_table] varchar(255) NULL, 
	[last_processed_ts] datetime2(6) NOT NULL, 
	[last_run_at] datetime2(6) NULL, 
	[rows_merged] bigint NULL, 
	[pipeline_run_id] varchar(255) NULL
);