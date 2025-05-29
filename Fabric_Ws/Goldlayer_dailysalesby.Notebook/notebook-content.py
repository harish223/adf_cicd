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

# **Create the Gold table for category sales**

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_category_sales AS
SELECT 
    p.category AS product_category,
    SUM(o.total_amount) AS category_total_sales
FROM 
    SilverLayer.silver_orders o
JOIN 
    SilverLayer.silver_products p ON o.product_id = p.product_id
GROUP BY 
    p.category
""")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from gold_category_sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
