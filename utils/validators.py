import re


def validate_email(email):
    """Validate email format."""
    if not email:
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def validate_phone(phone):
    """Validate phone number in local or international format."""
    if not phone:
        return False
    clean = ''.join(filter(str.isdigit, phone))
    return 10 <= len(clean) <= 15


def validate_password_strength(password):
    """Validate password meets minimum requirements."""
    if not password or len(password) < 6:
        return False, "Password must be at least 6 characters"
    return True, None


def sanitize_string(value, max_length=255):
    """Strip and truncate a string input."""
    if not value:
        return value
    return str(value).strip()[:max_length]


def validate_lead_input(data):
    """Validate lead creation/update payload."""
    errors = []

    if not data.get('name') or not data['name'].strip():
        errors.append("Name is required")

    phone = data.get('phone')
    if not phone:
        errors.append("Phone number is required")
    elif not validate_phone(phone):
        errors.append("Invalid phone number format")

    email = data.get('email')
    if email and not validate_email(email):
        errors.append("Invalid email format")

    return errors


def validate_user_input(data):
    """Validate user creation payload."""
    errors = []

    if not data.get('username') or not data['username'].strip():
        errors.append("Username is required")

    if not data.get('emp_first_name') or not data['emp_first_name'].strip():
        errors.append("First name is required")

    if not data.get('emp_last_name') or not data['emp_last_name'].strip():
        errors.append("Last name is required")

    email = data.get('email')
    if not email:
        errors.append("Email is required")
    elif not validate_email(email):
        errors.append("Invalid email format")

    if not data.get('role_id'):
        errors.append("Role is required")

    return errors


def validate_project_input(data):
    """Validate project creation/update payload."""
    errors = []

    if not data.get('project_name') or not data['project_name'].strip():
        errors.append("Project name is required")

    return errors
