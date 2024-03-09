from .source import SourceConnection


class PostgreSQLConnection(SourceConnection):
    def __init__(self, host: str, port: str, user_name: str, user_pass: str, catalog: str):
        super().__init__(host, port, user_name, user_pass, catalog)

    def jdbc_url(self, catalog):
        return f'jdbc:postgresql://{self.host}:{self.port}/{catalog}'

    @property
    def jdbc_driver(self):
        return 'org.postgresql.Driver'

    def __str__(self):
        return f"PostgreSQLConnection(host: '{self.host}')"
