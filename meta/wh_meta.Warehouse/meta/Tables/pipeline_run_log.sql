CREATE TABLE [meta].[pipeline_run_log] (

	[run_id] varchar(255) NOT NULL, 
	[pipeline_name] varchar(255) NOT NULL, 
	[layer] varchar(10) NOT NULL, 
	[status] varchar(50) NULL, 
	[started_at] datetime2(6) NOT NULL, 
	[ended_at] datetime2(6) NULL, 
	[message] varchar(max) NULL, 
	[logged_at] datetime2(6) NULL
);