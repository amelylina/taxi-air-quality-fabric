CREATE TABLE [dbo].[dm_date] (

	[date_key] bigint NULL, 
	[date] datetime2(6) NULL, 
	[year] int NULL, 
	[quarter] int NULL, 
	[month] int NULL, 
	[day] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(max) NULL, 
	[month_name] varchar(max) NULL, 
	[is_weekend] bit NULL
);