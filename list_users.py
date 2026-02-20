import mysql.connector
from db import DB_CONFIG

def check_users():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT emp_id, username, email, emp_status FROM employee")
        users = cursor.fetchall()
        print(f"Total users: {len(users)}")
        for u in users:
            print(f"ID: {u['emp_id']}, User: {u['username']}, Email: {u['email']}, Status: {u['emp_status']}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_users()
