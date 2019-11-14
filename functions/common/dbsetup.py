import sqlalchemy.engine


def create_tables(conn: sqlalchemy.engine.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS imports (
                    id TEXT PRIMARY KEY NOT NULL,
                    workspace_name TEXT NOT NULL,
                    workspace_namespace TEXT NOT NULL,
                    submitter TEXT NOT NULL,
                    submit_time INTEGER NOT NULL,
                    status TEXT NOT NULL)""")
