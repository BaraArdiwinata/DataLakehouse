from sqlalchemy import create_engine
import config

def get_staging_engine():
    """Membuat engine untuk koneksi ke database Staging/DWH Inti."""
    conn_string = f"postgresql+psycopg2://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME_STAGING}"
    engine = create_engine(conn_string)
    return engine

def get_warehouse_engine():
    """Membuat engine untuk koneksi ke database Laporan/Data Mart."""
    conn_string = f"postgresql+psycopg2://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME_WAREHOUSE}"
    engine = create_engine(conn_string)
    return engine