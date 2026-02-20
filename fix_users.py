import mysql.connector
from db import DB_CONFIG

def fix_status():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE employee SET emp_status = 'Active' WHERE emp_status IS NULL OR emp_status = ''")
    print(f"Updated {cursor.rowcount} users.")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_status()
