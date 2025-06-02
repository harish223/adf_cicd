CREATE TABLE [dbo].[nyctaxi_yellow] (

	[vendor] varchar(255) NULL, 
	[tpep_pickup_datetime] date NULL, 
	[tpep_dropoff_datetime] date NULL, 
	[pu_borough] varchar(100) NULL, 
	[pu_zone] varchar(100) NULL, 
	[do_borough] varchar(100) NULL, 
	[do_zone] varchar(100) NULL, 
	[payment_method] varchar(100) NULL, 
	[passenger_count] int NULL, 
	[trip_distance] float NULL, 
	[total_amount] float NULL
);