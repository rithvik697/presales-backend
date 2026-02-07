import mysql.connector
from mysql.connector import pooling

# ----------------------------------------
# Database configuration
# ----------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port" : 3306,
    "user": "root",
    "password": "Root@1234",
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
