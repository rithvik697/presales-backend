from flask import Blueprint, request, jsonify
from services.call_logs_service import (
    start_call_service,
    end_call_service,
    get_call_logs_service
)

call_logs_bp = Blueprint("call_logs", __name__)


@call_logs_bp.route("/start", methods=["POST"])
def start_call():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    lead_id = data.get("lead_id")
    emp_id = data.get("emp_id")

    if not lead_id or not emp_id:
        return jsonify({"error": "lead_id and emp_id are required"}), 400

    call_id = start_call_service(lead_id, emp_id)

    return jsonify({
        "call_id": call_id,
        "message": "Call started"
    }), 201


@call_logs_bp.route("/end", methods=["POST"])
def end_call():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    call_id = data.get("call_id")

    if not call_id:
        return jsonify({"error": "call_id is required"}), 400

    result = end_call_service(call_id)

    if result is None:
        return jsonify({"error": "Call not found"}), 404

    if result == "ALREADY_ENDED":
        return jsonify({"error": "Call already ended"}), 409

    return jsonify({
        "message": "Call ended",
        "call_duration": result
    }), 200


@call_logs_bp.route("/logs", methods=["GET"])
def get_call_logs():
    logs = get_call_logs_service()
    return jsonify(logs), 200

@call_logs_bp.route("/ui", methods=["GET"])
def get_call_logs_ui():
    from services.call_logs_service import get_call_logs_for_ui
    logs = get_call_logs_for_ui()
    return jsonify(logs), 200

