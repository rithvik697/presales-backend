from binascii import Error
import mysql.connector
from mysql.connector import pooling

# ----------------------------------------
# Database configuration
# ----------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "2004",
    "database": "presales_crm"
}


def init_db():
    try:
        # First, connect without database to ensure it exists
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
            
        if connection.is_connected():
            cursor = connection.cursor()
            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
                
            # Now connect to the specific database
            connection.database = DB_CONFIG['database']
        return connection

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        print("Please ensure MySQL is running and credentials in backend/Db.py are correct.")
        return None


init_db()


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

get_db_connection = get_db
