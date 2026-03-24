import logging
from flask import Blueprint, request, jsonify
from decorators.webhook_auth import website_key_required
from services.webhook_service import process_webhook_lead

logger = logging.getLogger(__name__)

website_leads_bp = Blueprint("website_leads", __name__)


@website_leads_bp.route("/lead", methods=["POST"])
@website_key_required
def receive_website_lead():
    """
    Receive a lead directly from a website backend or chatbot backend.

    Headers:
        X-Website-Key: <WEBSITE_FORM_API_KEY>

    Body (JSON):
    {
        "source": "Website",
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

    if not data.get("source"):
        data["source"] = "Website"

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
        logger.error(f"Website lead error: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@website_leads_bp.route("/lead", methods=["GET"])
def website_lead_health():
    return jsonify({
        "status": "ok",
        "message": "Website lead endpoint is active"
    }), 200
