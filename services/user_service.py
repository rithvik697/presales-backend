from db import get_db
from datetime import datetime
import mysql.connector
from werkzeug.security import generate_password_hash
import re


def _ensure_phone_column(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW COLUMNS FROM employee LIKE 'phone_num'")
        if cursor.fetchone() is None:
            cursor.execute("ALTER TABLE employee ADD COLUMN phone_num VARCHAR(20) NULL AFTER emp_status")
            conn.commit()
    finally:
        cursor.close()


def _generate_next_emp_id(cursor):
    cursor.execute(
        """
        SELECT emp_id
        FROM employee
        WHERE emp_id REGEXP '^EMP[0-9]+$'
        ORDER BY CAST(SUBSTRING(emp_id, 4) AS UNSIGNED) DESC
        LIMIT 1
        FOR UPDATE
        """
    )

    row = cursor.fetchone()
    if not row or not row[0]:
        return 'EMP001'

    current_id = row[0]
    next_num = int(current_id[3:]) + 1
    return f'EMP{next_num:03d}'


def _slug_username(value):
    username = re.sub(r'[^a-z0-9._]', '', str(value).lower())
    return username[:100]


def _generate_unique_username(cursor, data, emp_id):
    requested = (data.get('username') or '').strip().lower()
    if requested:
        base = _slug_username(requested)
    else:
        email = (data.get('email') or '').strip().lower()
        email_local = email.split('@')[0] if '@' in email else email
        name_based = f"{data.get('emp_first_name', '')}.{data.get('emp_last_name', '')}".strip('.')
        base = _slug_username(email_local or name_based or emp_id.lower())

    if not base:
        base = emp_id.lower()

    candidate = base
    suffix = 1
    while True:
        cursor.execute("SELECT 1 FROM employee WHERE username = %s LIMIT 1", (candidate,))
        if cursor.fetchone() is None:
            return candidate
        suffix += 1
        candidate = f"{base}{suffix}"[:100]


def register_user(data):
    conn = None
    cursor = None

    try:
        conn = get_db()
        _ensure_phone_column(conn)
        cursor = conn.cursor()

        new_emp_id = _generate_next_emp_id(cursor)
        username = _generate_unique_username(cursor, data, new_emp_id)
        raw_password = str(data.get('password') or 'Welcome@123')
        password_hash = generate_password_hash(raw_password, method='scrypt')

        query = """
            INSERT INTO employee (
                emp_id,
                emp_first_name,
                emp_middle_name,
                emp_last_name,
                role_id,
                emp_status,
                phone_num,
                username,
                email,
                password_hash,
                created_by,
                created_on
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            new_emp_id,
            data['emp_first_name'].strip(),
            data.get('emp_middle_name'),
            data['emp_last_name'].strip(),
            data['role_id'],
            data['emp_status'],
            str(data['phone_num']).strip(),
            username,
            data['email'].strip().lower(),
            password_hash,
            data.get('created_by', 'ADMIN'),
            datetime.now()
        )

        cursor.execute(query, values)
        conn.commit()
        return new_emp_id

    except mysql.connector.IntegrityError as e:
        raise Exception(f'Employee already exists or invalid role/email data: {e}')

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_all_users():
    conn = get_db()
    _ensure_phone_column(conn)
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            emp_id,
            emp_first_name,
            emp_middle_name,
            emp_last_name,
            role_id,
            emp_status,
            phone_num,
            email,
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


def get_user_by_id(emp_id):
    conn = get_db()
    _ensure_phone_column(conn)
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            emp_id,
            emp_first_name,
            emp_middle_name,
            emp_last_name,
            role_id,
            emp_status,
            phone_num,
            email,
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


def update_user(emp_id, data):
    conn = get_db()
    _ensure_phone_column(conn)
    cursor = conn.cursor()

    query = """
        UPDATE employee
        SET
            emp_first_name = %s,
            emp_middle_name = %s,
            emp_last_name = %s,
            role_id = %s,
            emp_status = %s,
            phone_num = %s,
            email = %s,
            modified_by = %s,
            modified_on = %s
        WHERE emp_id = %s
    """

    values = (
        data['emp_first_name'].strip(),
        data.get('emp_middle_name'),
        data['emp_last_name'].strip(),
        data['role_id'],
        data['emp_status'],
        str(data['phone_num']).strip(),
        data['email'].strip().lower(),
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


def update_user_status(emp_id, status):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE employee
        SET emp_status = %s
        WHERE emp_id = %s
    """, (status, emp_id))

    conn.commit()

    updated_rows = cursor.rowcount

    cursor.close()
    conn.close()

    return updated_rows > 0