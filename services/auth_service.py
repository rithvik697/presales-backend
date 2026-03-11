from werkzeug.security import check_password_hash, generate_password_hash
from db import get_db
import logging
logger = logging.getLogger(__name__)


def hash_password(password: str) -> str:
    """
    Hash password using scrypt.
    Used when admin creates or resets passwords.
    """
    return generate_password_hash(password, method="scrypt")


class AuthService:

    def login(self, username, email, password):
        db = get_db()
        cursor = db.cursor(dictionary=True)

        try:
            cursor.execute("""
                SELECT
                    emp_id AS user_id,
                    username,
                    email,
                    password_hash,
                    emp_status,
                    role_id,
                    last_login,
                    CONCAT(
                        emp_first_name,
                        COALESCE(CONCAT(' ', emp_middle_name), ''),
                        COALESCE(CONCAT(' ', emp_last_name), '')
                    ) AS full_name
                FROM employee
                WHERE username = %s AND email = %s
            """, (username.lower(), email.lower()))

            user = cursor.fetchone()

            if not user:
                logger.warning(f"Login failed: user not found ({username}, {email})")
                return None

            if user["emp_status"].lower() != "active":
                logger.warning(f"Inactive employee attempted login: {username}")
                return None

            try:
                valid = check_password_hash(user["password_hash"], password)
            except Exception as e:
                logger.error(f"Password verification error for {username}: {e}")
                return None

            if not valid:
                logger.warning(f"Invalid password for {username}")
                return None

            cursor.execute(
                "UPDATE employee SET last_login = NOW() WHERE emp_id = %s",
                (user["user_id"],)
            )
            db.commit()

            logger.info(f"Login successful: {username} ({user['role_id']})")

            return {
                "user_id": user["user_id"],
                "username": user["username"],
                "full_name": user["full_name"],
                "role_type": user["role_id"]
            }

        except Exception as e:
            logger.error(f"AuthService login error: {e}")
            return None

        finally:
            cursor.close()
            db.close()
    
    def change_password(self, user_id, old_password, new_password):

        db = get_db()
        cursor = db.cursor(dictionary=True)

        try:
            cursor.execute(
                "SELECT password_hash FROM employee WHERE emp_id = %s",
                (user_id,)
            )

            user = cursor.fetchone()

            if not user:
                return False

            # verify old password
            if not check_password_hash(user["password_hash"], old_password):
                return False

            # hash new password
            new_hash = generate_password_hash(new_password, method="scrypt")

            cursor.execute(
                "UPDATE employee SET password_hash = %s WHERE emp_id = %s",
                (new_hash, user_id)
            )

            db.commit()

            return True

        except Exception as e:
            logger.error(f"Change password error: {e}")
            return False

        finally:
            cursor.close()
            db.close()