import smtplib
from email.mime.text import MIMEText
from config import SMTP_EMAIL, SMTP_PASSWORD, SMTP_SERVER, SMTP_PORT


def send_temp_password_email(email, username, temp_password):

    subject = "Your CRM Account Login Details"

    body = f"""
Hello,

Your CRM account has been created.

Username: {username}
Temporary Password: {temp_password}

Please login and change your password.

Regards,
CRM Team
"""

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = email

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(SMTP_EMAIL, SMTP_PASSWORD)
    server.send_message(msg)
    server.quit()

def send_reset_email(email, reset_link):

    subject = "Reset Your Password"

    body = f"""
    Hello,

    Click the link below to reset your password:

    {reset_link}

    This link will expire in 15 minutes.

    If you did not request a password reset, please ignore this email.

    Regards,
    CRM Team
    """

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = email

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(SMTP_EMAIL, SMTP_PASSWORD)
    server.send_message(msg)
    server.quit()