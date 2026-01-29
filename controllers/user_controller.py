from flask import Blueprint, request, jsonify
from services.user_service import register_user

user_controller_bp = Blueprint(
    'user_controller_bp',
    __name__
)

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
