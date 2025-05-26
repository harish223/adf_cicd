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

df=spark.read.format('delta')\
             .load('abfss://Binaryville@onelake.dfs.fabric.microsoft.com/BronzeLayer.Lakehouse/Tables/Orders')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    total_amount DOUBLE,
    transaction_date DATE,
    order_status STRING,
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

last_processed_df=spark.sql("select max('last_updated') as last_processed from silver_orders")
last_processed_timestamp=last_processed_df.collect()[0]['last_processed']


if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_view=spark.sql('select * from bronzelayer.orders')
display(df_view)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when,col
df_qun_total=df_view.withColumn("quantity",when(col('quantity')<0,0).otherwise(col('quantity')))\
                     .withColumn("total_amount",when(col('total_amount')<0,0).otherwise(col('total_amount')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date,col
df_trans =df_qun_total.withColumn('transaction_date',to_date(col('transaction_date'),'yyyy-MM-dd'))
df_trans =df_qun_total.filter(col('transaction_date').isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_order = df_trans.withColumn('order_status',when((col('quantity')==0) & (col('total_amount')==0),'Cancelled'))\
                   .withColumn('order_status',when((col('quantity')>0) & (col('total_amount')>0),'Completed'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_order_null=df_order.filter(col('order_status').isNotNull())\
                 .filter(col('customer_id').isNotNull())\
                 .filter(col('product_id').isNotNull())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp
df_order_curnt =df_order_null.withColumn('last_updated',current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_order_rename=df_order_curnt.withColumnRenamed('transaction_id','order_id')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_order_rename)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_order = df_final_ord.select('order_id',
                                'customer_id',
                                'product_id',
                                'quantity',
                                'total_amount',
                                'transaction_date',
                                'order_status',
                                'last_updated'
                                )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StringType,DateType
df_final_order=df_order_rename.withColumn('customer_id',col('customer_id').cast(StringType()))\
                         .withColumn('product_id',col('product_id').cast(StringType()))\
                         .withColumn('transaction_date',col('transaction_date').cast(DateType()))
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_finalorder = df_final_order.select('order_id',
                                'customer_id',
                                'product_id',
                                'quantity',
                                'total_amount',
                                'transaction_date',
                                'order_status',
                                'last_updated'
                                )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_finalorder.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_finalorder.write.format('delta')\
                  .mode("overwrite")\
                  .saveAsTable("silver_orders")

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
