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
# META         },
# META         {
# META           "id": "f0712cb4-4cc9-44bf-aa83-937b3ce3bf47"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


df= spark.read\
          .format("delta")\
          .load("abfss://Binaryville@onelake.dfs.fabric.microsoft.com/BronzeLayer.Lakehouse/Tables/product")


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
CREATE TABLE IF NOT EXISTS silver_products (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    price DOUBLE,
    stock_quantity INT,
    rating double,
    is_active BOOLEAN,
    price_category STRING,
    stock_status STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_products")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_products AS
SELECT *
FROM bronzelayer.product WHERE ingestion_timestamp > '{last_processed_timestamp}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_view = spark.sql("select * from bronzelayer.product")
display(df_view)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_view.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when,col,lit,cast
df_price = df_view.withColumn('price',when(col('price')<0,0).otherwise(col('price')))\
                  .withColumn('stock_quantity',when(col('stock_quantity')<0,0).otherwise(col('stock_quantity')))
                                               

display(df_price)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import IntegerType,DoubleType
df_price=df_price.withColumn('rating', col('rating').cast(DoubleType()))
df_price=df_price.withColumn('rating',when(col('rating')<0,0)
                                       .when(col('rating')>5,5)
                                       .otherwise(col('rating')))


                               
display(df_price)                           

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_price_cat=df_price.withColumn('price_category',when(col('price')>600,'Premium')
                                         .when(col('price')>400,'Standard')
                                         .otherwise('Budget'))
display(df_price_cat)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stock_cat=df_price_cat.withColumn('stock_status',when(col('stock_quantity')==0,'Out of stock')
                                         .when(col('stock_quantity')<300,'Low Stock')
                                         .when(col('stock_quantity')<500,'Moderate Stock')
                                         .otherwise('Sufficient Stock'))

display(df_stock_cat)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp
df_final_product=df_stock_cat.withColumn('last_updated',current_timestamp())

display(df_final_product)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_product=df_final_product.select('product_id',
                                         'name',
                                         'category',
                                         'brand',
                                         'price',
                                         'stock_quantity',
                                         'rating',
                                         'is_active',
                                         'price_category',
                                         'stock_status',
                                         'last_updated'
                                         )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final_product)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StringType,IntegerType
df_final_product=df_final_product.withColumn('product_id',col('product_id').cast(StringType()))\
                                  .withColumn('stock_quantity',col('stock_quantity').cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_product.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_product.write\
                .format('delta')\
                .mode("overwrite")\
                .saveAsTable('silver_products')




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%%sql
select * from silver_products

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
