import os
from werkzeug.security import check_password_hash, generate_password_hash
from db import get_db
import logging
import jwt
import datetime
from config import PRIVATE_KEY, PUBLIC_KEY
from services.email_service import send_reset_email
from utils.validators import validate_password_strength

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
                    COALESCE(must_change_password, 0) AS must_change_password,
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
                "role_type": user["role_id"],
                "email": user["email"],
                "must_change_password": bool(user["must_change_password"])
            }

        except Exception as e:
            logger.error(f"AuthService login error: {e}")
            return None

        finally:
            cursor.close()
            db.close()


    def forgot_password(self, email):

        db = get_db()
        cursor = db.cursor(dictionary=True)

        try:
            cursor.execute(
                "SELECT emp_id FROM employee WHERE email=%s",
                (email.lower(),)
            )

            user = cursor.fetchone()

            if not user:
                raise Exception("Email not found")

            payload = {
                "sub": user["emp_id"],
                "purpose": "password_reset",
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
            }

            token = jwt.encode(payload, PRIVATE_KEY, algorithm="RS256")

            frontend_url = os.getenv("FRONTEND_URL", "http://localhost:4200")
            reset_link = f"{frontend_url}/reset-password?token={token}"

            send_reset_email(email, reset_link)

            logger.info(f"Password reset email sent to {email}")

            return True

        except Exception as e:
            logger.error(f"Forgot password error: {e}")
            raise e

        finally:
            cursor.close()
            db.close()


    def reset_password(self, token, new_password):

        try:
            decoded = jwt.decode(
                token,
                PUBLIC_KEY,
                algorithms=["RS256"]
            )

            if decoded.get("purpose") != "password_reset":
                raise Exception("Invalid reset token")

            emp_id = decoded["sub"]

        except jwt.ExpiredSignatureError:
            raise Exception("Reset link expired")

        except jwt.InvalidTokenError:
            raise Exception("Invalid reset token")

        valid, err = validate_password_strength(new_password)
        if not valid:
            raise ValueError(err)

        password_hash = generate_password_hash(
            new_password,
            method="scrypt"
        )

        db = get_db()
        cursor = db.cursor(dictionary=True)

        try:
            cursor.execute(
                "SELECT password_hash FROM employee WHERE emp_id = %s",
                (emp_id,)
            )
            user = cursor.fetchone()

            if not user:
                raise Exception("User not found")

            if check_password_hash(user["password_hash"], new_password):
                raise ValueError("New password must be different from the current password")

            cursor.execute("""
                UPDATE employee
                SET password_hash=%s, must_change_password=0
                WHERE emp_id=%s
            """, (password_hash, emp_id))

            db.commit()

            logger.info(f"Password successfully reset for employee {emp_id}")

            return True

        except Exception as e:
            logger.error(f"Reset password error: {e}")
            raise e

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
                return False, "User not found"

            # verify old password
            if not check_password_hash(user["password_hash"], old_password):
                return False, "Current password is incorrect"

            if old_password == new_password:
                return False, "New password must be different from the current password"

            # validate new password strength
            valid, err = validate_password_strength(new_password)
            if not valid:
                return False, err

            # hash new password
            new_hash = generate_password_hash(new_password, method="scrypt")

            cursor.execute(
                "UPDATE employee SET password_hash = %s, must_change_password = 0 WHERE emp_id = %s",
                (new_hash, user_id)
            )

            db.commit()

            return True, None

        except Exception as e:
            logger.error(f"Change password error: {e}")
            return False, "An unexpected error occurred"

        finally:
            cursor.close()
            db.close()
