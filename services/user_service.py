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
            data.get('created_by', 'admin'),
            datetime.now()
        )

        cursor.execute(query, values)
        conn.commit()

    except mysql.connector.IntegrityError as e:
        # Foreign key / duplicate key / constraint issues
        raise Exception(
            "Invalid role_id or employee already exists"
        )

    except Exception as e:
        # Any other unexpected error
        raise Exception(str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
