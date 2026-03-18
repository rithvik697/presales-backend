from flask import Blueprint, request, jsonify
from services.call_logs_service import (
    start_call_service,
    end_call_service,
    get_call_logs_service,
    get_call_logs_for_lead_ui,
    create_manual_call_log,
    update_call_log,
    delete_call_log
)
from decorators.auth_decorators import token_required

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


@call_logs_bp.route("/ui/lead/<lead_id>", methods=["GET"])
def get_call_logs_for_lead(lead_id):
    logs = get_call_logs_for_lead_ui(lead_id)
    return jsonify(logs), 200


@call_logs_bp.route("/manual", methods=["POST"])
@token_required
def create_manual_log(decoded):
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    lead_id = data.get("lead_id")
    if not lead_id:
        return jsonify({"error": "lead_id is required"}), 400

    data["emp_id"] = decoded["sub"]

    try:
        call_id = create_manual_call_log(data)
        return jsonify({"call_id": call_id, "message": "Call log created"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@call_logs_bp.route("/<int:call_id>", methods=["PUT"])
@token_required
def update_log(decoded, call_id):
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    updated = update_call_log(call_id, data)
    if not updated:
        return jsonify({"error": "Call log not found"}), 404

    return jsonify({"message": "Call log updated"}), 200


@call_logs_bp.route("/<int:call_id>", methods=["DELETE"])
@token_required
def delete_log(decoded, call_id):
    deleted = delete_call_log(call_id)
    if not deleted:
        return jsonify({"error": "Call log not found"}), 404

    return jsonify({"message": "Call log deleted"}), 200

