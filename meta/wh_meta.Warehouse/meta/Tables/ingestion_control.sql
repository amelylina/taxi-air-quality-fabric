CREATE TABLE [meta].[ingestion_control] (

	[source_name] varchar(255) NOT NULL, 
	[partition_key] varchar(255) NOT NULL, 
	[source_url] varchar(1000) NULL, 
	[status] varchar(50) NOT NULL, 
	[started_at] datetime2(6) NULL, 
	[ended_at] datetime2(6) NULL, 
	[bronze_rows_written] bigint NULL, 
	[error_message] varchar(max) NULL, 
	[silver_status] varchar(50) NULL, 
	[silver_started_at] datetime2(6) NULL, 
	[silver_ended_at] datetime2(6) NULL, 
	[silver_rows_written] bigint NULL, 
	[silver_error_message] varchar(max) NULL, 
	[created_at] datetime2(6) NULL
);