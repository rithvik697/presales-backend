from db import get_db
from datetime import datetime
import mysql.connector


# -------------------------
# CREATE USER
# -------------------------
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
        raise Exception("Employee already exists or invalid role_id")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -------------------------
# GET ALL USERS
# -------------------------
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


# -------------------------
# GET USER BY EMP ID ✅
# -------------------------
def get_user_by_id(emp_id):
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
        WHERE emp_id = %s
    """

    cursor.execute(query, (emp_id,))
    user = cursor.fetchone()

    cursor.close()
    conn.close()

    return user


# -------------------------
# UPDATE USER BY EMP ID ✅
# -------------------------
def update_user(emp_id, data):
    conn = get_db()
    cursor = conn.cursor()

    query = """
        UPDATE employee
        SET
            emp_first_name = %s,
            emp_middle_name = %s,
            emp_last_name = %s,
            role_id = %s,
            emp_status = %s,
            modified_by = %s,
            modified_on = %s
        WHERE emp_id = %s
    """

    values = (
        data['emp_first_name'],
        data.get('emp_middle_name'),
        data['emp_last_name'],
        data['role_id'],
        data['emp_status'],
        data.get('modified_by', 'ADMIN'),
        datetime.now(),
        emp_id
    )

    cursor.execute(query, values)
    conn.commit()

    updated = cursor.rowcount > 0

    cursor.close()
    conn.close()

    return updated
