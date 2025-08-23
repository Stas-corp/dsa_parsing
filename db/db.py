from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config.logger import logger

SERVER = "localhost"
DATABASE = "cases"
DRIVER = "ODBC Driver 17 for SQL Server"

connection_string = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&TrustServerCertificate=yes"

def get_engine():
    try:
        engine = create_engine(
            connection_string, 
            echo=False,
            fast_executemany=True,
            future=True
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"{DATABASE} engine")
        return engine
    except Exception:
        master_connection = f"mssql+pyodbc://@{SERVER}/master?driver={DRIVER}&TrustServerCertificate=yes"
        master_engine = create_engine(master_connection, isolation_level="AUTOCOMMIT", echo=True)
        with master_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE [{DATABASE}]"))
        logger.info(f"CREATE DATABASE {DATABASE}")
        return get_engine()

engine = get_engine()
Session_maker = sessionmaker(bind=engine, expire_on_commit=False)

def main():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT @@VERSION"))
        print(result.fetchone())

if __name__ == '__main__':
    main()