from .source import SourceConnection


class MySQLConnection(SourceConnection):

    def __init__(self, host: str, port: str, user_name: str, user_pass: str, catalog: str):
        super().__init__(host, port, user_name, user_pass, catalog)

    def jdbc_url(self, catalog):
        return f"jdbc:mysql://{self.host}:{self.port}"
    
    @property
    def jdbc_driver(self):
        return "com.mysql.cj.jdbc.Driver"
    