import findspark
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark import SparkConf

from inno_syncer.main import start_job
from inno_syncer.sync.config import get_config

this_dir = Path().cwd().resolve()


findspark.init()

conf = SparkConf()
conf.set(
    "spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.74.0",
)
conf.set("spark.sql.sources.default", "iceberg")
conf.set("spark.jars", f"{this_dir}/jdbc/postgresql-42.7.2.jar")
conf.set("spark.driver.extraClassPath", f"{this_dir}/jdbc/postgresql-42.7.2.jar")

conf.set("spark.sql.execution.pyarrow.enabled", "true")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
conf.set("spark.sql.catalog.spark_catalog.type", "hive")
conf.set("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083")
# conf.set("spark.sql.catalog.hive_metastore", "org.apache.iceberg.spark.SparkCatalog")
# conf.set("spark.sql.catalog.hive_metastore.type", "hive")
# conf.set("spark.sql.catalog.hive_metastore.uri", "thrift://localhost:9083")
conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.sql("USE spark_catalog;")

production_config = get_config("application.conf")

start_job(spark, production_config)
