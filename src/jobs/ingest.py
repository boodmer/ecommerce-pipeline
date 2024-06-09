import click
from utils.required import NotRequiredIf
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, lit
from datetime import datetime
import os
from dotenv import load_dotenv


@click.command()
@click.option('--table_name', cls=NotRequiredIf, help='table name')
@click.option('--execution_date', cls=NotRequiredIf, help='execution date')
def ingest(table_name: str, execution_date: str):
    date_object = datetime.strptime(execution_date, '%Y-%m-%d').date()
    spark = SparkSession.builder \
        .appName('Ingest Data From PosgreSQL To DataLake') \
        .config('spark.jars', 'jars/postgresql-42.7.3.jar') \
        .master('local[*]').getOrCreate()
    jvm = spark._jvm
    jsc = spark._jsc
    table_location = os.path.join(os.getenv('HDFS_URL'), table_name)
    uri = jvm.java.net.URI(table_location)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        uri, jsc.hadoopConfiguration())
    exists = fs.exists(jvm.org.apache.hadoop.fs.Path(table_location))
    table_query = f'SELECT * FROM {table_name}'

    if exists:
        df = spark.read.parquet(table_location)
        last_record_id = df.agg(max('id')).head()[0]
        table_query += f' WHERE id > {last_record_id}'

    table_data = [{'query': table_query, 'location': table_location}]

    if table_name == 'orders':
        order_detail_sub_query = table_query.replace('*', 'id')
        order_detail_table_query = f'SELECT * FROM order_details WHERE order_id IN ({
            order_detail_sub_query})'
        inventory_sub_query = table_query.replace(
            'SELECT', 'SELECT DISTINCT').replace('*', 'product_id')
        inventory_table_query = f'SELECT * FROM product_inventories WHERE product_id IN ({
            inventory_sub_query})'
        table_data.extend(
            [
                {'query': order_detail_table_query, 'location': os.path.join(
                    os.getenv('HDFS_URL'), 'order_details')},
                {'query': inventory_table_query, 'location': os.path.join(
                 os.getenv('HDFS_URL'), 'product_inventories')}
            ])

    for data in table_data:
        database_df = spark.read.format('jdbc') \
            .option('url', os.getenv('DB_URL')) \
            .option('query', data['query']) \
            .option("user",  os.getenv('DB_USER')) \
            .option("password", os.getenv('DB_PASSWORD')) \
            .option('fetchSize', 10000) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        output_df = database_df \
            .withColumn('year', lit(date_object.year)) \
            .withColumn('month', lit(date_object.month)) \
            .withColumn('day', lit(date_object.day))

        output_df.write.partitionBy('year', 'month', 'day').mode(
            saveMode='append').parquet(data['location'])


if __name__ == "__main__":
    if not os.getenv('ENV'):
        load_dotenv()
    ingest()
