CREATE TABLE [dbo].[dm_date] (

	[date] date NULL, 
	[date_key] int NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[day] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(max) NULL, 
	[month_name] varchar(max) NULL, 
	[quarter] int NULL, 
	[is_weekend] bit NULL, 
	[year_month] varchar(max) NULL
);