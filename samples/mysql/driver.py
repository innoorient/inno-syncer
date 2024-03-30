import findspark
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark import SparkConf

from inno_syncer.main import start_job
from inno_syncer.sync.config import get_config

this_dir = Path(__file__).parent


findspark.init()

conf = SparkConf()
conf.set(
    "spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,mysql:mysql-connector-java:8.0.30",
)
conf.set("spark.sql.sources.default", "iceberg")

conf.set("spark.sql.execution.pyarrow.enabled", "true")
conf.set(
    "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"
)
conf.set("spark.sql.catalog.spark_catalog.type", "hive")
conf.set("spark.sql.catalogImplementation", "hive")
conf.set("hive.metastore.uris", "thrift://localhost:9083")

conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("USE spark_catalog;")
spark.sql("SHOW DATABASES;").show()
# spark.sql("DROP DATABASE IF EXISTS mysql_raw CASCADE;")

# production_config = get_config(f"{this_dir}/application.conf")

# start_job(spark, production_config)
