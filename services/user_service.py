from db import get_db
from datetime import datetime
import mysql.connector
from werkzeug.security import generate_password_hash
import secrets


# -------------------------
# GENERATE NEXT EMPLOYEE ID
# -------------------------
def generate_emp_id():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT emp_id
        FROM employee
        ORDER BY emp_id DESC
        LIMIT 1
    """)

    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result:
        last_id = result[0]
        number = int(last_id.replace("EMP", ""))
        new_number = number + 1
    else:
        new_number = 1

    return f"EMP{str(new_number).zfill(3)}"


# -------------------------
# CREATE USER (Scrypt Only)
# -------------------------
def register_user(data):
    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor()

        emp_id = generate_emp_id()

        username = data['username']
        temp_password = secrets.token_urlsafe(8)

        # ✅ STANDARDIZED PASSWORD HASHING (SCRYPT ONLY)
        password_hash = generate_password_hash(
            temp_password,
            method='scrypt'
        )

        query = """
            INSERT INTO employee (
                emp_id,
                emp_first_name,
                emp_middle_name,
                emp_last_name,
                role_id,
                emp_status,
                phone_num,
                created_by,
                created_on,
                username,
                email,
                password_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            emp_id,
            data['emp_first_name'],
            data.get('emp_middle_name'),
            data['emp_last_name'],
            data['role_id'],
            data['emp_status'],
            data.get('phone_num'),
            data.get('created_by', 'ADMIN'),
            datetime.now(),
            username,
            data['email'],
            password_hash
        )

        cursor.execute(query, values)
        conn.commit()

        return {
            "emp_id": emp_id,
            "username": username,
            "temporary_password": temp_password
        }

    except mysql.connector.IntegrityError:
        raise Exception("Username or Email already exists")

    except Exception as e:
        raise Exception(f"Failed to register user: {str(e)}")

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
            phone_num,   -- ✅ FIXED
            created_by,
            created_on,
            modified_by,
            modified_on,
            username,
            email
        FROM employee
        ORDER BY created_on DESC
    """

    cursor.execute(query)
    users = cursor.fetchall()

    cursor.close()
    conn.close()

    return users


# -------------------------
# GET USER BY EMP ID
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
            phone_num,   -- ✅ FIXED
            created_by,
            created_on,
            modified_by,
            modified_on,
            username,
            email
        FROM employee
        WHERE emp_id = %s
    """

    cursor.execute(query, (emp_id,))
    user = cursor.fetchone()

    cursor.close()
    conn.close()

    return user


# -------------------------
# UPDATE USER
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
            phone_num = %s,  -- ✅ FIXED
            email = %s,
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
        data.get('phone_num'),  # ✅ FIXED
        data['email'],
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


# -------------------------
# UPDATE USER STATUS
# -------------------------
def update_user_status(emp_id, new_status):
    conn = get_db()
    cursor = conn.cursor()

    query = """
        UPDATE employee
        SET
            emp_status = %s,
            modified_by = %s,
            modified_on = %s
        WHERE emp_id = %s
    """

    values = (
        new_status,
        'ADMIN',
        datetime.now(),
        emp_id
    )

    cursor.execute(query, values)
    conn.commit()

    updated = cursor.rowcount > 0

    cursor.close()
    conn.close()

    return updated


# -------------------------
# DELETE USER (SOFT DELETE)
# -------------------------
def delete_user_by_id(emp_id):
    conn = get_db()
    cursor = conn.cursor()

    query = """
        UPDATE employee
        SET
            emp_status = 'Inactive',
            modified_by = %s,
            modified_on = %s
        WHERE emp_id = %s
    """

    values = (
        'ADMIN',
        datetime.now(),
        emp_id
    )

    cursor.execute(query, values)
    conn.commit()

    deleted = cursor.rowcount > 0

    cursor.close()
    conn.close()

    return deleted