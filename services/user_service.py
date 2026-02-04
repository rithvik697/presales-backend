from db import get_db
from datetime import datetime
import mysql.connector


def register_user(data):
    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor()

        query = """
            INSERT INTO employee (
                emp_id,
                emp_first_name,
                emp_middle_name,
                emp_last_name,
                role_id,
                emp_status,
                created_by,
                created_on
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            data['emp_id'],
            data['emp_first_name'],
            data.get('emp_middle_name'),
            data['emp_last_name'],
            data['role_id'],
            data['emp_status'],
            data.get('created_by', 'ADMIN'),
            datetime.now()
        )

        cursor.execute(query, values)
        conn.commit()

    except mysql.connector.IntegrityError:
        raise Exception("Invalid role_id or employee already exists")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ✅ NEW FUNCTION
def get_all_users():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            emp_id,
            emp_first_name,
            emp_middle_name,
            emp_last_name,
            role_id,
            emp_status,
            created_by,
            created_on,
            modified_by,
            modified_on
        FROM employee
        ORDER BY created_on DESC
    """

    cursor.execute(query)
    users = cursor.fetchall()

    cursor.close()
    conn.close()

    return users
