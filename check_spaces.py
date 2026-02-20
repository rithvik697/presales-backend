import mysql.connector
from db import DB_CONFIG

def check_users():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT emp_id, username, email FROM employee")
    for u in cursor.fetchall():
        print(f"ID: {u['emp_id']}, U: '{u['username']}', E: '{u['email']}'")
    conn.close()

if __name__ == "__main__":
    check_users()
