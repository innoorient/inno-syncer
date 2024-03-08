from .source import SourceConnection


class SQLServerConnection(SourceConnection):
    
    def __init__(self, host: str, port: str, user_name: str, user_pass: str, catalog: str):
        super().__init__(host, port, user_name, user_pass, catalog)

    def jdbc_url(self, catalog):
        return f'jdbc:sqlserver://{self.host}:{self.port};databaseName={catalog};'
    
    @property
    def jdbc_format(self):
        return 'com.microsoft.sqlserver.jdbc.spark'

    @property
    def jdbc_driver(self):
        return 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

    def __str__(self):
        return f"SQLServerConnection(host: '{self.host}')"