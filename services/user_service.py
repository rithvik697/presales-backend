from db import get_db
from datetime import datetime
import mysql.connector
from werkzeug.security import generate_password_hash
import secrets
from services.audit_service import log_audit
from services.email_service import send_temp_password_email


# -------------------------
# REQUIRED FIELDS
# -------------------------
REQUIRED_CREATE_FIELDS = ['username', 'emp_first_name', 'emp_last_name', 'role_id', 'emp_status', 'email']
REQUIRED_UPDATE_FIELDS = ['emp_first_name', 'emp_last_name', 'role_id', 'emp_status', 'email']


# -------------------------
# INPUT VALIDATION HELPER
# -------------------------
def _validate_fields(data, required_fields):
    missing = [f for f in required_fields if not data.get(f)]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")


# -------------------------
# GENERATE NEXT EMPLOYEE ID (RACE-CONDITION SAFE)
# -------------------------
def _generate_emp_id(cursor):
    """
    Generate next EMP ID using SELECT ... FOR UPDATE to prevent
    two concurrent requests from getting the same ID.
    Must be called inside an active transaction.
    """
    cursor.execute("""
        SELECT emp_id
        FROM employee
        ORDER BY emp_id DESC
        LIMIT 1
        FOR UPDATE
    """)

    result = cursor.fetchone()

    if result:
        last_id = result[0] if isinstance(result, tuple) else result['emp_id']
        number = int(last_id.replace("EMP", ""))
        new_number = number + 1
    else:
        new_number = 1

    return f"EMP{str(new_number).zfill(3)}"


# -------------------------
# CREATE USER
# -------------------------
def register_user(data, created_by='ADMIN'):
    _validate_fields(data, REQUIRED_CREATE_FIELDS)

    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor()

        emp_id = _generate_emp_id(cursor)

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
                password_hash,
                must_change_password
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """

        values = (
            emp_id,
            data['emp_first_name'],
            data.get('emp_middle_name'),
            data['emp_last_name'],
            data['role_id'],
            data['emp_status'],
            data.get('phone_num'),
            created_by,
            datetime.now(),
            username,
            data['email'],
            password_hash
        )

        cursor.execute(query, values)
        conn.commit()

        # Send temporary password email
        try:
            send_temp_password_email(
                data["email"],
                username,
                temp_password
            )
        except Exception as e:
            print(f"Email send failed for {username}: {e}")

        # Audit log
        try:
            log_audit(
                object_name="employee",
                object_id=emp_id,
                property_name="USER_CREATED",
                old_value=None,
                new_value=f"User {emp_id} created",
                modified_by=created_by,
                action_type="INSERT"
            )
        except Exception as e:
            print(f"Audit log failed: {e}")

        return {
            "emp_id": emp_id,
            "username": username,
            "temporary_password": temp_password
        }

    except mysql.connector.IntegrityError:
        if conn:
            conn.rollback()
        raise Exception("Username or Email already exists")

    except ValueError:
        raise

    except Exception as e:
        if conn:
            conn.rollback()
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
    conn = None
    cursor = None

    try:
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
        return users

    except Exception as e:
        raise Exception(f"Failed to fetch users: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -------------------------
# GET USER BY EMP ID
# -------------------------
def get_user_by_id(emp_id):
    conn = None
    cursor = None

    try:
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
        return user

    except Exception as e:
        raise Exception(f"Failed to fetch user {emp_id}: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -------------------------
# UPDATE USER
# -------------------------
def update_user(emp_id, data, modified_by='ADMIN'):
    _validate_fields(data, REQUIRED_UPDATE_FIELDS)

    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        # Fetch old values for audit trail
        cursor.execute("SELECT * FROM employee WHERE emp_id = %s", (emp_id,))
        old_record = cursor.fetchone()

        if not old_record:
            raise Exception(f"User {emp_id} not found")

        # Switch to plain cursor for UPDATE
        cursor.close()
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

        cursor.execute(query, values)
        conn.commit()

        updated = cursor.rowcount > 0

        if updated:
            # Log each changed field for a meaningful audit trail
            tracked_fields = {
                'emp_first_name': 'First Name',
                'emp_last_name': 'Last Name',
                'emp_middle_name': 'Middle Name',
                'role_id': 'Role',
                'emp_status': 'Status',
                'phone_num': 'Phone',
                'email': 'Email'
            }

            for db_field, display_name in tracked_fields.items():
                old_val = old_record.get(db_field)
                new_val = data.get(db_field)

                if str(old_val or '') != str(new_val or ''):
                    try:
                        log_audit(
                            object_name="employee",
                            object_id=emp_id,
                            property_name=db_field,
                            old_value=str(old_val) if old_val else None,
                            new_value=str(new_val) if new_val else None,
                            modified_by=modified_by,
                            action_type="UPDATE"
                        )
                    except Exception as e:
                        print(f"Audit log failed for {db_field}: {e}")

        return updated

    except ValueError:
        raise

    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"Failed to update user {emp_id}: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -------------------------
# UPDATE USER STATUS
# -------------------------
def update_user_status(emp_id, new_status, modified_by='ADMIN'):
    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        # Fetch old status for audit
        cursor.execute("SELECT emp_status FROM employee WHERE emp_id = %s", (emp_id,))
        old_record = cursor.fetchone()

        if not old_record:
            raise Exception(f"User {emp_id} not found")

        old_status = old_record['emp_status']

        # Switch to plain cursor for UPDATE
        cursor.close()
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
            modified_by,
            datetime.now(),
            emp_id
        )

        cursor.execute(query, values)
        conn.commit()

        updated = cursor.rowcount > 0

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
                print(f"Audit log failed: {e}")

        return updated

    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"Failed to update status for {emp_id}: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -------------------------
# DELETE USER (SOFT DELETE)
# -------------------------
def delete_user_by_id(emp_id, modified_by='ADMIN'):
    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        # Fetch old status for audit
        cursor.execute("SELECT emp_status FROM employee WHERE emp_id = %s", (emp_id,))
        old_record = cursor.fetchone()

        if not old_record:
            raise Exception(f"User {emp_id} not found")

        old_status = old_record['emp_status']

        # Switch to plain cursor for UPDATE
        cursor.close()
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
            modified_by,
            datetime.now(),
            emp_id
        )

        cursor.execute(query, values)
        conn.commit()

        deleted = cursor.rowcount > 0

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
                print(f"Audit log failed: {e}")

        return deleted

    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"Failed to delete user {emp_id}: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()