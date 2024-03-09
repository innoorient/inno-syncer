"""Main module."""

from inno_syncer import DataSyncer
from inno_syncer.sync.iomete_logger import init_logger


def start_job(spark, config):
    init_logger()
    data_syncer = DataSyncer(spark, config)
    data_syncer.run()
