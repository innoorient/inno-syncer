class SourceConnection:
    def __init__(self, host: str, port: str, user_name: str, user_pass: str, catalog: str):
        self.host = host
        self.port = port
        self.user_name = user_name
        self.user_pass = user_pass
        self.catalog = catalog

    def jdbc_url(self, schema):
        return None

    @property
    def jdbc_format(self):
        return 'org.apache.spark.sql.jdbc'

    @property
    def jdbc_driver(self):
        return None

    def proxy_table_definition_for_info_schema(self, proxy_table_name):
        return self.proxy_table_definition(
            source_schema="information_schema",
            source_table="tables",
            proxy_table_name=proxy_table_name
        )

    def proxy_table_definition(self, source_schema, source_table, proxy_table_name):
        return f"""
            CREATE TABLE IF NOT EXISTS {proxy_table_name}
            USING {self.jdbc_format}
            OPTIONS (
              url '{self.jdbc_url(self.catalog)}',
              dbtable '{source_schema}.{source_table}',
              user '{self.user_name}',
              password '{self.user_pass}',
              driver '{self.jdbc_driver}'
            )
        """

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(host: '{self.host}')"
