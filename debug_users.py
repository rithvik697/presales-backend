import mysql.connector
from db import DB_CONFIG

def check_users():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT emp_id, username, email, emp_status FROM employee")
    for u in cursor.fetchall():
        print(f"USER_DETAILS: {u}")
    conn.close()

if __name__ == "__main__":
    check_users()
