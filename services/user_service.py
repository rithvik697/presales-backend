from db import get_db
from datetime import datetime
import mysql.connector
from werkzeug.security import generate_password_hash
import secrets
from services.audit_service import log_audit


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
# CREATE USER
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

        # AUDIT LOG
        try:
            log_audit(
                object_name="employee",
                object_id=emp_id,
                property_name="USER_CREATED",
                old_value=None,
                new_value=f"User {emp_id} created",
                modified_by=data.get('created_by', username),
                action_type="INSERT"
            )
        except Exception as e:
            print("Audit log failed:", e)

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
            phone_num,
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
            phone_num,
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
    cursor = conn.cursor(dictionary=True)

    modified_by = data.get("modified_by", "ADMIN")

    # -------------------------
    # Get existing data (for audit comparison)
    # -------------------------
    cursor.execute("SELECT * FROM employee WHERE emp_id = %s", (emp_id,))
    old_user = cursor.fetchone()

    if not old_user:
        cursor.close()
        conn.close()
        return False

    update_query = """
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
        data['emp_first_name'],
        data.get('emp_middle_name'),
        data['emp_last_name'],
        data['role_id'],
        data['emp_status'],
        data.get('phone_num'),
        data['email'],
        modified_by,
        datetime.now(),
        emp_id
    )

    cursor.execute(update_query, values)
    conn.commit()

    updated = cursor.rowcount > 0

    # -------------------------
    # AUDIT TRAIL (Field-Level Logging)
    # -------------------------
    if updated:

        fields_to_track = [
            "emp_first_name",
            "emp_middle_name",
            "emp_last_name",
            "role_id",
            "emp_status",
            "phone_num",
            "email"
        ]

        for field in fields_to_track:

            old_value = old_user.get(field)
            new_value = data.get(field)

            if str(old_value) != str(new_value):

                try:
                    log_audit(
                        object_name="employee",
                        object_id=emp_id,
                        property_name=field,
                        old_value=old_value,
                        new_value=new_value,
                        modified_by=modified_by,
                        action_type="UPDATE"
                    )
                except Exception as e:
                    print("Audit log failed:", e)

    cursor.close()
    conn.close()

    return updated

# -------------------------
# UPDATE USER STATUS
# -------------------------
def update_user_status(emp_id, new_status, modified_by="ADMIN"):

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    # Get current status
    cursor.execute(
        "SELECT emp_status FROM employee WHERE emp_id = %s",
        (emp_id,)
    )

    row = cursor.fetchone()

    if not row:
        cursor.close()
        conn.close()
        return False

    old_status = row["emp_status"]

    update_query = """
        UPDATE employee
        SET
            emp_status = %s,
            modified_by = %s,
            modified_on = %s
        WHERE emp_id = %s
    """

    cursor.execute(update_query, (
        new_status,
        modified_by,
        datetime.now(),
        emp_id
    ))

    conn.commit()

    updated = cursor.rowcount > 0

    cursor.close()
    conn.close()

    if updated:
        try:
            log_audit(
                object_name="employee",
                object_id=emp_id,
                property_name="emp_status",
                old_value=old_status,
                new_value=new_status,
                modified_by=modified_by,
                action_type="UPDATE"
            )
        except Exception as e:
            print("Audit log failed:", e)

    return updated


# -------------------------
# DELETE USER (SOFT DELETE)
# -------------------------
def delete_user_by_id(emp_id, modified_by="ADMIN"):

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    # Get old status
    cursor.execute(
        "SELECT emp_status FROM employee WHERE emp_id = %s",
        (emp_id,)
    )

    row = cursor.fetchone()
    old_status = row["emp_status"] if row else None

    query = """
        UPDATE employee
        SET
            emp_status = 'Inactive',
            modified_by = %s,
            modified_on = %s
        WHERE emp_id = %s
    """

    cursor.execute(query, (
        modified_by,
        datetime.now(),
        emp_id
    ))

    conn.commit()

    deleted = cursor.rowcount > 0

    cursor.close()
    conn.close()

    if deleted:
        try:
            log_audit(
                object_name="employee",
                object_id=emp_id,
                property_name="emp_status",
                old_value=old_status,
                new_value="Inactive",
                modified_by=modified_by,
                action_type="DELETE"
            )
        except Exception as e:
            print("Audit log failed:", e)

    return deleted