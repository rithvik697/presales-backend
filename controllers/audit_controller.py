from flask import Blueprint, jsonify
from services.audit_service import get_audit_logs

# Create Blueprint
audit_controller_bp = Blueprint(
    "audit_controller_bp",
    __name__
)

# -------------------------
# GET AUDIT TRAIL
# -------------------------
@audit_controller_bp.route("/audit-trail", methods=["GET"])
def fetch_audit_logs():

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