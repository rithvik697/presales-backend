import mysql.connector
from mysql.connector import pooling

# ----------------------------------------
# Database configuration
# ----------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Admin@99",
    "database": "presales_crm"
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
