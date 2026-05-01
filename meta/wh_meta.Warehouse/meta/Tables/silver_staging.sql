CREATE TABLE [meta].[silver_staging] (

	[source_name] varchar(255) NOT NULL, 
	[partition_key] varchar(255) NOT NULL, 
	[silver_status] varchar(50) NULL, 
	[silver_rows_written] bigint NULL, 
	[silver_error_message] varchar(max) NULL
);