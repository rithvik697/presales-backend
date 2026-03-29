from flask import Blueprint, jsonify
from services.audit_service import get_audit_logs
from decorators.auth_decorators import token_required

# Create Blueprint
audit_controller_bp = Blueprint(
    "audit_controller_bp",
    __name__
)

# -------------------------
# GET AUDIT TRAIL
# -------------------------
@audit_controller_bp.route("/audit-trail", methods=["GET"])
@token_required
def fetch_audit_logs(decoded):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"success": False, "error": "Admin access required"}), 403

    try:
        logs = get_audit_logs()

        return jsonify({
            "success": True,
            "data": logs
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500