CREATE PROCEDURE metadata.insert_staging_metadata
  @pipeline_run_id varchar(255),
  @table_name varchar(255),
  @processed_date DATETIME2
AS
   INSERT INTO metadata.processing_log(pipeline_run_id,table_processed,rows_processed,latest_processed_pickup,processed_timestamp)
   select 
     @pipeline_run_id AS pipeline_id,
     @table_name AS table_processed,
     count(*) as row_processed,
    max(tpep_pickup_datetime) as latest_processed_pickup,
    @processed_date as processed_timestamp
   from stg.Nyctaxi_yellow