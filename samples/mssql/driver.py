from iomete_postgresql_sync.main import start_job
from iomete_postgresql_sync.sync.config import get_config
from pyspark.sql import SparkSession
from pyspark import SparkConf
import findspark
from pathlib import Path

this_dir = Path().cwd().resolve()
SPARK_VERSION = "3.3"
ICEBERG_VERSION = "1.4.3"

findspark.init()

conf = SparkConf()
conf.set(
    "spark.jars.packages",
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.12:{ICEBERG_VERSION},com.microsoft.azure:spark-mssql-connector_2.12:1.3.0-BETA",
)
conf.set("spark.sql.sources.default", "iceberg")
# conf.set("spark.jars", f"{this_dir}/jdbc/postgresql-42.7.2.jar")
# conf.set("spark.driver.extraClassPath", f"{this_dir}/jdbc/postgresql-42.7.2.jar")

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
conf.set("spark.debug.maxToStringFields", "1000")
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.sql("USE spark_catalog;")

production_config = get_config("application-mssql.conf")

start_job(spark, production_config)
