# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7df45fb0-bc55-44ab-82a1-504b5bfae03b",
# META       "default_lakehouse_name": "SilverLayer",
# META       "default_lakehouse_workspace_id": "4ece834b-5c38-4c6e-a15f-5507043adb76",
# META       "known_lakehouses": [
# META         {
# META           "id": "7df45fb0-bc55-44ab-82a1-504b5bfae03b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("delta")\
           .load("abfss://Binaryville@onelake.dfs.fabric.microsoft.com/BronzeLayer.Lakehouse/Tables/customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    customer_segment STRING,
    days_since_registration INT,
    last_updated TIMESTAMP)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_customers").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

last_processed_df = spark.sql("select max(last_updated) as last_processed from silver_customers")
print(type(last_processed_df))

last_processed_timestamp = last_processed_df.collect()[0]["last_processed"]
print(type(last_processed_timestamp))

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ** Load incremental data from the Bronze layer**
# 


# CELL ********************

# Create a temporary view of incremental bronze data

spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW bronze_increment as 
SELECT * FROM bronzelayer.customer c where c.ingestion_timestamp > '{last_processed_timestamp}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM bronzelayer.customer
# MAGIC ----select * from bronze_increment

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_temp_view = spark.sql("SELECT * FROM bronzelayer.customer")
display(df_temp_view)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ** Transform the customer data**

# CELL ********************

from pyspark.sql.functions import when,col
df_view =df_temp_view.withColumn("Customer_segment",when(col('total_purchases')>1000,'High Value')\
                                      .when(col("total_purchases")>500,'Medium Value').otherwise('Low Value'))

display(df_view)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
df_transformed =df_view.filter(
                               (col("age").between(18, 100)) &
                               (col("email").isNotNull()) &
                               (col("total_purchases")>=0)
                               )
                              
display(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_date,current_timestamp,datediff,col
df_final = df_transformed.withColumn("days_since_registration",datediff(current_date(),col("registration_date")))\
                          .withColumn("last_updated",current_timestamp())
                                    
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_data=df_final.select("customer_id",
                         "name",
                         "email",
                         "country",
                          "customer_type",
                          "registration_date",
                          "age",
                          "gender",
                          "total_purchases",
                          "customer_segment",
                          "days_since_registration",
                          "last_updated")
display(df_final_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_data.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date,col
from pyspark.sql.types import IntegerType

df_final_data = df_final_data.withColumn("registration_date",to_date(col("registration_date"),'yyyy-MM-dd'))\
                             .withColumn("age",(col("age").cast(IntegerType())))\
                             .withColumn("total_purchases",(col("total_purchases").cast(IntegerType())))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_data.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_data.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from silver_customers

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
