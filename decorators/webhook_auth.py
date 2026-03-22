import os
from functools import wraps
from flask import request, jsonify


def webhook_key_required(f):
    """
    Decorator that validates the X-API-Key header against WEBHOOK_API_KEY env var.
    Used for external webhook integrations (Make.com, Zapier, etc.)
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        expected_key = os.getenv("WEBHOOK_API_KEY")

        if not expected_key:
            return jsonify({"error": "Webhook API key not configured on server"}), 500

        provided_key = request.headers.get("X-API-Key")

        if not provided_key or provided_key != expected_key:
            return jsonify({"error": "Unauthorized: Invalid API key"}), 401

        return f(*args, **kwargs)

    return decorated
