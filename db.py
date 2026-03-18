import os
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------
# Database configuration from environment
# ----------------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "presales_crm")
}

# ----------------------------------------
# Connection pool
# ----------------------------------------
connection_pool = pooling.MySQLConnectionPool(
    pool_name="presales_pool",
    pool_size=5,
    **DB_CONFIG
)

# ----------------------------------------
# Get DB connection
# ----------------------------------------
def get_db():
    return connection_pool.get_connection()
