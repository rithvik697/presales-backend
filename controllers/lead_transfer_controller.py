from flask import Blueprint, request, jsonify
from decorators.auth_decorators import token_required
from services.lead_transfer_service import (
    get_lead_transfer_history,
    preview_lead_transfer,
    transfer_leads,
)


lead_transfer_bp = Blueprint("lead_transfer_bp", __name__)


@lead_transfer_bp.route("/config/lead-transfers/history", methods=["GET"])
@token_required
def list_lead_transfer_history(decoded):
    try:
        return jsonify(get_lead_transfer_history()), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@lead_transfer_bp.route("/config/lead-transfers/preview", methods=["POST"])
@token_required
def preview_transfer(decoded):
    data = request.get_json() or {}
    from_emp_id = data.get("from_emp_id")

    if not from_emp_id:
        return jsonify({"error": "from_emp_id is required"}), 400

    try:
        preview = preview_lead_transfer(
            from_emp_id=from_emp_id,
            from_project_id=data.get("from_project_id"),
            from_source_id=data.get("from_source_id"),
            from_status_id=data.get("from_status_id"),
            date_type=data.get("date_type"),
            from_date=data.get("from_date"),
            to_date=data.get("to_date"),
        )
        return jsonify(preview), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@lead_transfer_bp.route("/config/lead-transfers", methods=["POST"])
@token_required
def run_lead_transfer(decoded):
    data = request.get_json() or {}
    from_emp_id = data.get("from_emp_id")
    to_emp_id = data.get("to_emp_id")

    if not from_emp_id or not to_emp_id:
        return jsonify({"error": "from_emp_id and to_emp_id are required"}), 400

    try:
        actor_id = decoded.get("sub") or decoded.get("username", "SYSTEM")
        result = transfer_leads(
            from_emp_id=from_emp_id,
            to_emp_id=to_emp_id,
            actor_id=actor_id,
            from_project_id=data.get("from_project_id"),
            from_source_id=data.get("from_source_id"),
            from_status_id=data.get("from_status_id"),
            to_project_id=data.get("to_project_id"),
            to_source_id=data.get("to_source_id"),
            to_status_id=data.get("to_status_id"),
            date_type=data.get("date_type"),
            from_date=data.get("from_date"),
            to_date=data.get("to_date"),
            limit=data.get("limit"),
        )
        return jsonify(result), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
