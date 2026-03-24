import os
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------
# Database configuration from environment
# ----------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port" : 3306,
    "user": "root",
    "password": "Root@1234",
    "database": "presales_crm_3"
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
