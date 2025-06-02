CREATE PROCEDURE stg.data_cleaning_stg
@end_date datetime2,
@start_date DATETIME2
AS
delete from stg.Nyctaxi_yellow where tpep_pickup_datetime<@start_date or tpep_pickup_datetime>@end_date