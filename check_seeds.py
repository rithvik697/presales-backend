import mysql.connector
from db import DB_CONFIG

def check():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    print("Sources:")
    cursor.execute("SELECT * FROM lead_sources")
    for row in cursor.fetchall():
        print(row)
        
    print("\nStatuses:")
    cursor.execute("SELECT * FROM lead_status")
    for row in cursor.fetchall():
        print(row)
        
    conn.close()

if __name__ == "__main__":
    check()
