from flask import Blueprint, jsonify, request
from decorators.auth_decorators import token_required
from services.report_email_service import (
    get_all_recipients,
    add_recipient,
    update_recipient,
    delete_recipient
)

report_email_bp = Blueprint("report_email_bp", __name__)


@report_email_bp.route("/config/report-emails", methods=["GET"])
@token_required
def list_recipients(decoded):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"error": "Admin access required"}), 403
    try:
        return jsonify(get_all_recipients()), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@report_email_bp.route("/config/report-emails", methods=["POST"])
@token_required
def create_recipient(decoded):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"error": "Admin access required"}), 403

    data = request.get_json() or {}
    created_by = decoded.get("sub") or decoded.get("username", "SYSTEM")

    try:
        result = add_recipient(data, created_by)
        return jsonify({"success": True, "data": result}), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@report_email_bp.route("/config/report-emails/<int:recipient_id>", methods=["PUT"])
@token_required
def toggle_recipient(decoded, recipient_id):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"error": "Admin access required"}), 403

    data = request.get_json() or {}
    try:
        result = update_recipient(recipient_id, data)
        if result is None:
            return jsonify({"error": "Recipient not found"}), 404
        return jsonify({"success": True, "data": result}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@report_email_bp.route("/config/report-emails/<int:recipient_id>", methods=["DELETE"])
@token_required
def remove_recipient(decoded, recipient_id):
    if decoded.get("role_type") != "ADMIN":
        return jsonify({"error": "Admin access required"}), 403
    try:
        deleted = delete_recipient(recipient_id)
        if not deleted:
            return jsonify({"error": "Recipient not found"}), 404
        return jsonify({"success": True, "message": "Recipient removed"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
