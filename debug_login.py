import mysql.connector
from db import DB_CONFIG
from werkzeug.security import check_password_hash

def check_aryan_login(test_password):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT username, email, password_hash, emp_status FROM employee WHERE username = 'aryan'")
        user = cursor.fetchone()
        
        if not user:
            print("User 'aryan' not found in database.")
            return
            
        print(f"User found: {user['username']} | Email: {user['email']} | Status: {user['emp_status']}")
        
        # Check password
        is_valid = check_password_hash(user['password_hash'], test_password)
        print(f"Password '{test_password}' is valid: {is_valid}")
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    pw = sys.argv[1] if len(sys.argv) > 1 else 'password'
    check_aryan_login(pw)
