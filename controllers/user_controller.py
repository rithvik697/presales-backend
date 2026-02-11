from db import get_db 

from flask import Blueprint, request, jsonify
from services.user_service import (
    register_user,
    get_all_users,
    get_user_by_id,
    update_user,
    update_user_status
)

user_controller_bp = Blueprint(
    'user_controller_bp',
    __name__,
    url_prefix='/api'   # ✅ IMPORTANT
)

# -------------------------
# CREATE USER
# -------------------------
@user_controller_bp.route('/users/register', methods=['POST'])
def create_user():
    data = request.json

    required_fields = [
        'emp_id',
        'emp_first_name',
        'emp_last_name',
        'role_id',
        'emp_status'
    ]

    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({
                "success": False,
                "error": f"{field} is required"
            }), 400

    try:
        register_user(data)
        return jsonify({
            "success": True,
            "message": "User created successfully"
        }), 201

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# -------------------------
# GET ALL USERS
# -------------------------
@user_controller_bp.route('/users', methods=['GET'])
def fetch_users():
    try:
        users = get_all_users()
        return jsonify({
            "success": True,
            "data": users
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# -------------------------
# GET USER BY EMP ID
# -------------------------
@user_controller_bp.route('/users/<emp_id>', methods=['GET'])
def fetch_user_by_id_controller(emp_id):
    try:
        user = get_user_by_id(emp_id)

        if not user:
            return jsonify({
                "success": False,
                "error": "User not found"
            }), 404

        return jsonify({
            "success": True,
            "data": user
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# -------------------------
# UPDATE FULL USER
# -------------------------
@user_controller_bp.route('/users/<emp_id>', methods=['PUT'])
def update_user_controller(emp_id):
    data = request.json

    try:
        updated = update_user(emp_id, data)

        if not updated:
            return jsonify({
                "success": False,
                "error": "User not found"
            }), 404

        return jsonify({
            "success": True,
            "message": "User updated successfully"
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ===========================================
# UPDATE ONLY USER STATUS (TOGGLE)
# ===========================================
@user_controller_bp.route('/users/<emp_id>/status', methods=['PUT'])
def update_user_status_controller(emp_id):

    data = request.json
    new_status = data.get('emp_status')

    if new_status not in ['Active', 'Inactive']:
        return jsonify({
            "success": False,
            "error": "Invalid status"
        }), 400

    try:
        updated = update_user_status(emp_id, new_status)

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
