
from typing import List
import pandas as pd
from sqlalchemy import create_engine, text
class PostgresReader:
    def __init__(self, host, port, dbname, user, password, **kwargs):
        """
        Initialize the PostgresReader using SQLAlchemy engine.

        Args:
            host (str): Database host
            port (int/str): Database port
            dbname (str): Database name
            user (str): Username
            password (str): Password
        """
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        self.engine = create_engine(connection_string)

    def query(self, sql_query):
        """
        Execute a SQL query and return the results as a pandas DataFrame.

        Args:
            sql_query (str): The SQL query to execute

        Returns:
            pd.DataFrame: Query results
        """
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql_query), conn)
            return df
        except Exception as e:
            print(f"❌ Error querying database: {e}")
            return None
        
def get_connection_informations(connection_name, path_to_connections=None, environment: str = "test") -> dict:
    connections = pd.read_json(path_to_connections)
    return connections[environment][connection_name]

def read_msf_tables(connection_names: List[str], table_names: List[str], table_filters: List[str] = None, path_to_connections=None, environment: str = "test") -> dict:
    tables = {}
    for connection, table_name, table_filter in zip(
        connection_names, 
        table_names,
        table_filters
    ):
        connection_info = get_connection_informations(connection, path_to_connections=path_to_connections, environment=environment)
        
        query = f"SELECT * FROM {table_name} "
        if table_filter is not None:
            query += table_filter

        df = PostgresReader(**connection_info).query(query)
        if df is not None:
            print(f"✅ Successfully read table: {table_name} from connection: {connection} and environment : {environment}")
            tables[table_name] = df
        else:
            print(f"❌ Failed to read table: {table_name} from connection: {connection} and environment : {environment}")
    return tables