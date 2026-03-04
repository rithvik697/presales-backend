from flask import Blueprint, request, jsonify
from services.user_service import update_user_status
from services.user_service import (
    register_user,
    get_all_users,
    get_user_by_id,
    update_user
)
import re

user_controller_bp = Blueprint(
    'user_controller_bp',
    __name__
)

EMAIL_REGEX = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')
PHONE_REGEX = re.compile(r'^\d{10}$')


def _is_valid_email(email):
    return bool(email and EMAIL_REGEX.match(email.strip()))


def _is_valid_phone(phone):
    return bool(phone and PHONE_REGEX.match(str(phone).strip()))


@user_controller_bp.route('/users/register', methods=['POST'])
def create_user():
    data = request.json or {}

    required_fields = [
        'emp_first_name',
        'emp_last_name',
        'role_id',
        'emp_status',
        'phone_num',
        'email'
    ]

    for field in required_fields:
        if field not in data or not str(data[field]).strip():
            return jsonify({
                'success': False,
                'error': f'{field} is required'
            }), 400

    if not _is_valid_phone(data.get('phone_num')):
        return jsonify({
            'success': False,
            'error': 'Invalid phone number. It must be exactly 10 digits.'
        }), 400

    if not _is_valid_email(data.get('email')):
        return jsonify({
            'success': False,
            'error': 'Invalid email format.'
        }), 400

    try:
        new_emp_id = register_user(data)
        return jsonify({
            'success': True,
            'message': 'User created successfully',
            'data': {
                'emp_id': new_emp_id
            }
        }), 201

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@user_controller_bp.route('/users', methods=['GET'])
def fetch_users():
    try:
        users = get_all_users()
        return jsonify({
            'success': True,
            'data': users
        }), 200
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@user_controller_bp.route('/users/<emp_id>', methods=['GET'])
def fetch_user_by_id_controller(emp_id):
    try:
        user = get_user_by_id(emp_id)

        if not user:
            return jsonify({
                'success': False,
                'error': 'User not found'
            }), 404

        return jsonify({
            'success': True,
            'data': user
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@user_controller_bp.route('/users/<emp_id>', methods=['PUT'])
def update_user_controller(emp_id):
    data = request.json or {}

    if 'phone_num' in data and not _is_valid_phone(data.get('phone_num')):
        return jsonify({
            'success': False,
            'error': 'Invalid phone number. It must be exactly 10 digits.'
        }), 400

    if 'email' in data and not _is_valid_email(data.get('email')):
        return jsonify({
            'success': False,
            'error': 'Invalid email format.'
        }), 400

    try:
        updated = update_user(emp_id, data)

        if not updated:
            return jsonify({
                'success': False,
                'error': 'User not found'
            }), 404

        return jsonify({
            'success': True,
            'message': 'User updated successfully'
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@user_controller_bp.route("/users/status/<emp_id>", methods=["PUT"])
def update_status(emp_id):
    data = request.get_json() or {}
    status = data.get("emp_status")

    if status not in ["Active", "Inactive"]:
        return jsonify({
            "success": False,
            "error": "Invalid status value"
        }), 400

    try:
        updated = update_user_status(emp_id, status)

        if not updated:
            return jsonify({
                "success": False,
                "error": "User not found"
            }), 404

        return jsonify({
            "success": True,
            "message": "Status updated successfully"
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500 
