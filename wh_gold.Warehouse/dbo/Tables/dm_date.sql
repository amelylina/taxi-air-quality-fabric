CREATE TABLE [dbo].[dm_date] (

	[date] date NULL, 
	[date_key] int NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[day] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(30) NULL, 
	[month_name] varchar(30) NULL, 
	[quarter] int NULL, 
	[is_weekend] bit NULL, 
	[year_month] varchar(8) NULL
);