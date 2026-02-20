import mysql.connector
from db import DB_CONFIG

def check_schema():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        tables = ['customer']
        for table in tables:
            print(f"\nTable: {table}")
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[0]}: {col[1]}")
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
