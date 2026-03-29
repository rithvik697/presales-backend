from flask import Blueprint, request, jsonify
import jwt, datetime
from services.auth_service import AuthService
from config import PRIVATE_KEY, JWT_ISSUER, JWT_AUDIENCE
import logging
logger = logging.getLogger(__name__)
from decorators.auth_decorators import token_required


auth_controller_bp = Blueprint("auth_controller", __name__)
auth_service = AuthService()



@auth_controller_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json() or {}

    username = data.get("username").strip()
    email = data.get("email").strip()
    password = data.get("password")

    if not username or not email or not password:
        return jsonify({"message": "Invalid credentials"}), 400

    result = auth_service.login(username, email, password)

    if result is None:
        return jsonify({"message": "Invalid credentials"}), 401

    payload = {
        "sub": str(result["user_id"]),
        "username": result["username"],
        "full_name": result["full_name"],
        "role_type": result["role_type"],
        "must_change_password": result["must_change_password"],
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=2)
    }

    token = jwt.encode(payload, PRIVATE_KEY, algorithm="RS256")

    return jsonify({
        "access_token": token,
        "username": result["username"],
        "full_name": result["full_name"],
        "role_type": result["role_type"],
        "email": result["email"],
        "must_change_password": result["must_change_password"]
    }), 200

@auth_controller_bp.route("/me", methods=["GET"])
@token_required
def me(decoded):
    return {
        "user_id": decoded["sub"],
        "username": decoded["username"],
        "role_type": decoded["role_type"]
    }, 200

@auth_controller_bp.route("/change-password", methods=["PUT"])
@token_required
def change_password(decoded):

    data = request.get_json() or {}

    old_password = data.get("old_password")
    new_password = data.get("new_password")

    if not old_password or not new_password:
        return jsonify({"message": "Missing password fields"}), 400

    user_id = decoded["sub"]

    success, err = auth_service.change_password(user_id, old_password, new_password)

    if not success:
        return jsonify({"message": err}), 400

    return jsonify({"message": "Password updated successfully"}), 200
@auth_controller_bp.route("/forgot-password", methods=["POST"])
def forgot_password():

    data = request.json or {}
    email = data.get("email")

    if not email:
        return jsonify({
            "success": False,
            "error": "Email is required"
        }), 400

    try:
        auth_service.forgot_password(email)
        return jsonify({
            "success": True,
            "message": "Reset email has been sent."
        }), 200
    except Exception as e:
        logger.warning(f"Forgot password failed for {email}: {e}")
        return jsonify({
            "success": False,
            "error": "Account does not exist."
        }), 404
    
@auth_controller_bp.route("/reset-password", methods=["POST"])
def reset_password():

    data = request.json
    token = data.get("token")
    new_password = data.get("password")

    if not token or not new_password:
        return jsonify({
            "success": False,
            "error": "Token and password are required"
        }), 400

    try:
        auth_service.reset_password(token, new_password)

        return jsonify({
            "success": True,
            "message": "Password reset successful"
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
