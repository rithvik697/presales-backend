from flask import Blueprint, request, jsonify
from services.user_service import (
    register_user,
    get_all_users,
    get_user_by_id,
    update_user,
    update_user_status,
    delete_user_by_id,
    resign_user
)
from decorators.auth_decorators import token_required
from utils.validators import validate_user_input


user_controller_bp = Blueprint(
    'user_controller_bp',
    __name__,
    url_prefix='/api'
)


# -------------------------
# CREATE USER (Auto emp_id)
# -------------------------
@user_controller_bp.route('/users/register', methods=['POST'])
@token_required
def create_user(decoded):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"success": False, "error": "Admin access required"}), 403

    data = request.json

    if not data:
        return jsonify({
            "success": False,
            "error": "Request body is required"
        }), 400

    try:
        errors = validate_user_input(data)
        if errors:
            return jsonify({"success": False, "error": ', '.join(errors)}), 400

        current_user = decoded.get('username', 'ADMIN')

        user_data = register_user(
            data,
            created_by=current_user
        )

        return jsonify({
            "success": True,
            "message": "User created successfully",
            "data": user_data
        }), 201

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# -------------------------
# GET ALL USERS
# -------------------------
@user_controller_bp.route('/users', methods=['GET'])
@token_required
def fetch_users(decoded):
    if decoded.get("role_type") not in ["ADMIN", "SALES_MGR"]:
        return jsonify({"success": False, "error": "Admin or Sales Manager access required"}), 403

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
@token_required
def fetch_user_by_id_controller(decoded, emp_id):
    if decoded.get("role_type") not in ["ADMIN", "SALES_MGR"]:
        return jsonify({"success": False, "error": "Admin or Sales Manager access required"}), 403

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
@token_required
def update_user_controller(decoded, emp_id):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"success": False, "error": "Admin access required"}), 403

    data = request.json

    if not data:
        return jsonify({
            "success": False,
            "error": "Request body is required"
        }), 400

    try:
        current_user = decoded.get('username', 'ADMIN')

        updated = update_user(
            emp_id,
            data,
            modified_by=current_user
        )

        if not updated:
            return jsonify({
                "success": False,
                "error": "User not found or no changes made"
            }), 404

        return jsonify({
            "success": True,
            "message": "User updated successfully"
        }), 200

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# -------------------------
# UPDATE USER STATUS ONLY
# -------------------------
@user_controller_bp.route('/users/<emp_id>/status', methods=['PUT'])
@token_required
def update_user_status_controller(decoded, emp_id):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"success": False, "error": "Admin access required"}), 403

    data = request.json

    if not data:
        return jsonify({
            "success": False,
            "error": "Request body is required"
        }), 400

    new_status = data.get('emp_status')
    modified_by = data.get('modified_by', 'ADMIN')

    if new_status not in ['Active', 'Inactive']:
        return jsonify({
            "success": False,
            "error": "Invalid status. Must be 'Active' or 'Inactive'"
        }), 400

    try:
        current_user = decoded.get('username', 'ADMIN')

        updated = update_user_status(
            emp_id,
            new_status,
            modified_by=current_user
        )

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


# -------------------------
# DELETE USER (SOFT DELETE)
# -------------------------
@user_controller_bp.route('/users/<emp_id>', methods=['DELETE'])
@token_required
def delete_user(decoded, emp_id):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"success": False, "error": "Admin access required"}), 403

    try:
        current_user = decoded.get('username', 'ADMIN')

        deleted = delete_user_by_id(
            emp_id,
            modified_by=current_user
        )

        if not deleted:
            return jsonify({
                "success": False,
                "error": "User not found"
            }), 404

        return jsonify({
            "success": True,
            "message": "User deleted successfully"
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
@user_controller_bp.route('/users/<emp_id>/resign', methods=['PUT'])
@token_required
def resign_user_controller(decoded, emp_id):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"success": False, "error": "Admin access required"}), 403

    try:
        current_user = decoded.get('username', 'ADMIN')

        resigned = resign_user(
            emp_id,
            modified_by=current_user
        )

        if not resigned:
            return jsonify({
                "success": False,
                "error": "User not found"
            }), 404

        return jsonify({
            "success": True,
            "message": "User resigned successfully"
        }), 200

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
