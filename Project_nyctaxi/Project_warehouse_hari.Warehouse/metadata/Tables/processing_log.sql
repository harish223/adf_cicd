CREATE TABLE [metadata].[processing_log] (

	[pipeline_run_id] varchar(255) NULL, 
	[table_processed] varchar(255) NULL, 
	[rows_processed] int NULL, 
	[latest_processed_pickup] datetime2(6) NULL, 
	[processed_timestamp] datetime2(6) NULL
);