from werkzeug.security import check_password_hash, generate_password_hash
from db import get_db
import logging
import jwt
import datetime
from config import PRIVATE_KEY, PUBLIC_KEY
from services.email_service import send_reset_email

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
                    last_login
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
                "role_type": user["role_id"]
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

            reset_link = f"http://localhost:4200/reset-password?token={token}"

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

        password_hash = generate_password_hash(
            new_password,
            method="scrypt"
        )

        db = get_db()
        cursor = db.cursor()

        try:
            cursor.execute("""
                UPDATE employee
                SET password_hash=%s
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