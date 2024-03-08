from inno_syncer.main import start_job
from inno_syncer.sync.config import get_config
from inno_syncer.test._spark_session import get_spark_session


def test_postgresql_table_migration():
    # create test spark instance
    test_config = get_config("application.conf")

    spark = get_spark_session()
    spark.sql("DROP DATABASE IF EXISTS employees_pg_raw CASCADE")

    # run target
    start_job(spark, test_config)

    # check
    assert spark.sql("select * from employees_pg_raw.employees").count() == 300024
    assert spark.sql("select * from employees_pg_raw.departments").count() == 9
    assert spark.sql("select * from employees_pg_raw.dept_manager").count() == 24
    assert spark.sql("select * from employees_pg_raw.dept_emp").count() == 331603
    assert spark.sql("select * from employees_pg_raw.titles").count() == 443308


if __name__ == '__main__':
    test_postgresql_table_migration()
