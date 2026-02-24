from flask import Blueprint, request, jsonify
import jwt, datetime
from services.auth_service import AuthService
from config import PRIVATE_KEY, JWT_ISSUER, JWT_AUDIENCE
import logging
logger = logging.getLogger(__name__)


auth_controller_bp = Blueprint("auth_controller", __name__)
auth_service = AuthService()



@auth_controller_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json() or {}

    username = data.get("username")
    email = data.get("email")
    password = data.get("password")

    if not username or not email or not password:
        return jsonify({"message": "Invalid credentials"}), 400

    result = auth_service.login(username, email, password)

    if result is None:
        return jsonify({"message": "Invalid credentials"}), 401

    payload = {
        "sub": str(result["user_id"]),
        "username": result["username"],
        "role_type": result["role_type"],
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=2)
    }

    token = jwt.encode(payload, PRIVATE_KEY, algorithm="RS256")

    return jsonify({
        "access_token": token,
        "emp_id": result["user_id"],
        "username": result["username"],
        "email": result["email"],
        "role_type": result["role_type"]
    }), 200