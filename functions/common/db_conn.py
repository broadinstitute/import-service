import sqlalchemy
from common import dbsetup
import os

# todo: env vars
db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASS")
db_name = os.environ.get("DB_NAME")
cloud_sql_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")

db = None
connection = None


def get_connection() -> sqlalchemy.engine.Connection:
    global db, connection

    if db is None:
        db = sqlalchemy.create_engine(
            # Equivalent URL:
            # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/<cloud_sql_instance_name>
            sqlalchemy.engine.url.URL(
                drivername='mysql+pymysql',
                username=db_user,
                password=db_pass,
                database=db_name,
                query={
                    'unix_socket': '/cloudsql/{}'.format(cloud_sql_connection_name)
                }
            ),
            pool_size=1,
            max_overflow=0
        )

    if connection is None:
        connection = db.connect()
        dbsetup.create_tables(connection)

    return connection
