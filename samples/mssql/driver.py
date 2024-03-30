from pathlib import Path

import findspark
from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession

from inno_syncer.main import start_job
from inno_syncer.sync.config import get_config

SPARK_VERSION = "3.3"
ICEBERG_VERSION = "1.4.3"

this_dir = Path(__file__).parent
load_dotenv(dotenv_path=Path(".env").resolve())

findspark.init()

conf = SparkConf()
conf.set(
    "spark.jars.packages",
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.12:{ICEBERG_VERSION},com.microsoft.azure:spark-mssql-connector_2.12:1.3.0-BETA",
)
conf.set("spark.sql.sources.default", "iceberg")
conf.set("spark.sql.execution.pyarrow.enabled", "true")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
conf.set("spark.sql.catalog.spark_catalog.type", "hive")
conf.set("hive.metastore.uris", "thrift://localhost:9083")
conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("USE spark_catalog;")

production_config = get_config(f"{this_dir}/application.conf")

start_job(spark, production_config)
