import logging
from flask import Blueprint, request, jsonify
from decorators.webhook_auth import webhook_key_required
from services.webhook_service import process_webhook_lead

logger = logging.getLogger(__name__)

webhook_bp = Blueprint("webhook", __name__)


@webhook_bp.route("/lead", methods=["POST"])
@webhook_key_required
def receive_lead():
    """
    Receive a lead from Make.com (or any external webhook).

    Headers:
        X-API-Key: <WEBHOOK_API_KEY>

    Body (JSON):
    {
        "source": "Facebook",
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@example.com",
        "phone": "9876543210",
        "project_name": "Project Alpha",
        "remarks": "Interested in 2BHK"
    }
    """
    data = request.get_json()

    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        result = process_webhook_lead(data)

        if result.get("status") == "duplicate":
            return jsonify({
                "success": True,
                "status": "duplicate",
                "message": result["message"]
            }), 200

        return jsonify({
            "success": True,
            "status": "created",
            **result
        }), 201

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Webhook lead error: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@webhook_bp.route("/lead", methods=["GET"])
def webhook_health():
    """Health check for webhook endpoint."""
    return jsonify({
        "status": "ok",
        "message": "Webhook endpoint is active"
    }), 200
