import os
import logging
from flask import Blueprint, request, jsonify
from services.mcube_service import process_mcube_call, initiate_click2call
from decorators.auth_decorators import token_required
from db import get_db

logger = logging.getLogger(__name__)

mcube_bp = Blueprint("mcube", __name__)


def _verify_mcube_key():
    """Verify the MCube API key from request header or query param."""
    expected_key = os.getenv("MCUBE_API_KEY")
    if not expected_key:
        return True  # No key configured, allow (dev mode)

    provided_key = (
        request.headers.get("X-API-Key")
        or request.args.get("api_key")
    )

    return provided_key == expected_key


@mcube_bp.route("/mcube-webhook", methods=["POST"])
def mcube_webhook():
    """
    Receives call data from MCube when a call ends.
    No JWT auth - uses MCube API key for verification.
    """
    if not _verify_mcube_key():
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or request.form.to_dict()

    if not data:
        return jsonify({"error": "No data received"}), 400

    try:
        result = process_mcube_call(data)
        return jsonify({
            "success": True,
            "message": "Call logged successfully",
            **result
        }), 201

    except Exception as e:
        logger.error(f"MCube webhook error: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@mcube_bp.route("/mcube-webhook", methods=["GET"])
def mcube_webhook_health():
    """Health check endpoint for MCube webhook verification."""
    return jsonify({"status": "ok", "message": "MCube webhook is active"}), 200


@mcube_bp.route("/click2call", methods=["POST"])
@token_required
def click2call(decoded):
    """
    Initiate a Click2Call from the CRM.
    Requires: customer_phone, optionally lead_id.
    Agent phone is looked up from the employee record.
    """
    data = request.get_json() or {}

    customer_phone = data.get("customer_phone")
    lead_id = data.get("lead_id")

    if not customer_phone:
        return jsonify({"error": "customer_phone is required"}), 400

    emp_id = decoded["sub"]

    # Look up agent's phone number
    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute(
            "SELECT phone_num FROM employee WHERE emp_id = %s",
            (emp_id,)
        )
        employee = cursor.fetchone()

        if not employee or not employee["phone_num"]:
            return jsonify({
                "error": "Your phone number is not configured. Please update your profile."
            }), 400

        agent_phone = employee["phone_num"]

        result = initiate_click2call(agent_phone, customer_phone)

        # Log the outbound call attempt
        from services.call_logs_service import start_call_service
        if lead_id:
            start_call_service(lead_id, emp_id)

        return jsonify({
            "success": True,
            "message": "Call initiated",
            "data": result
        }), 200

    except Exception as e:
        logger.error(f"Click2Call error: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

    finally:
        cursor.close()
        db.close()
