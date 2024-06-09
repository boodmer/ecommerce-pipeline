import click
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from datetime import datetime, date
import os
from dotenv import load_dotenv


def get_table_datalake_url(table_name: str) -> str:
    return os.path.join(os.getenv('HDFS_URL'), table_name)


def get_dataframe_from_datalake(spark: SparkSession, table_name: str, date: date) -> DataFrame:
    return spark.read.parquet(get_table_datalake_url(table_name)) \
        .filter((col("year") == date.year) & (col("month") == date.month) & (col("day") == date.day)) \
        .drop('year', 'month', 'day')

@click.command()
@click.option('--execution_date', help='execution date')
def transform(execution_date: str) -> None:
    date_object = datetime.now().date()
    if execution_date:
        date_object = datetime.strptime(execution_date, '%Y-%m-%d').date()

    spark = SparkSession.builder \
        .appName('Transform Data From Datalake to Hive') \
        .config("hive.metastore.uris", os.getenv('HIVE_URL')) \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .master('local[*]').getOrCreate()

    orders_df = get_dataframe_from_datalake(spark, 'orders', date_object)
    order_details_df = get_dataframe_from_datalake(spark, 'order_details', date_object)
    products_df = get_dataframe_from_datalake(spark, 'products', date_object)
    inventories_df = get_dataframe_from_datalake(spark, 'product_inventories', date_object)
    inventories_df = inventories_df.withColumnRenamed(
        'product_id', 'inventory_product_id')

    pre_df = orders_df \
        .join(order_details_df, orders_df['id'] == order_details_df['order_id'], 'inner') \
        .join(products_df, products_df['id'] == orders_df['product_id'], 'inner') \
        .join(inventories_df.select(col('quantity').alias('left_over'), col('inventory_product_id')),
              products_df['id'] == inventories_df['inventory_product_id'], 'inner'
              )

    map_df = pre_df.groupBy('make', 'model', 'category', 'product_id', 'left_over') \
        .agg(
            sum('quantity').alias('sales'),
            sum('total').alias('revenue')
    )

    result_df = map_df \
        .withColumn('year', lit(date_object.year)) \
        .withColumn('month', lit(date_object.month)) \
        .withColumn('day', lit(date_object.day)) \

    result_table = 'reports.daily_gross_revenue'

    result_df.write.format('hive') \
        .partitionBy('year', 'month', 'day')\
        .mode(saveMode='append').saveAsTable(result_table)


if __name__ == "__main__":
    if not os.getenv('ENV'):
        load_dotenv()

    transform()
