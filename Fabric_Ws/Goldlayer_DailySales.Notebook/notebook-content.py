# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c605ce57-7d99-452d-baaa-76b4e5a8ac10",
# META       "default_lakehouse_name": "GoldLayer",
# META       "default_lakehouse_workspace_id": "4ece834b-5c38-4c6e-a15f-5507043adb76",
# META       "known_lakehouses": [
# META         {
# META           "id": "c605ce57-7d99-452d-baaa-76b4e5a8ac10"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# 
# **Create the Gold table for daily sales**

# CELL ********************

spark.sql('''CREATE or REPLACE TABLE gold_daily_sales AS 
          SELECT 
          transaction_date,
          sum(total_amount) as daily_total_sale 
          from SilverLayer.silver_orders
          GROUP BY transaction_date
        ''' )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from gold_daily_sales 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
