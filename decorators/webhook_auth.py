import os
from functools import wraps
from flask import request, jsonify


def api_key_required(env_var_name, header_name="X-API-Key", config_label="API key"):
    """Validate a shared secret from a request header against an env var."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            expected_key = os.getenv(env_var_name)

            if not expected_key:
                return jsonify({"error": f"{config_label} not configured on server"}), 500

            provided_key = request.headers.get(header_name)

            if not provided_key or provided_key.strip() != expected_key.strip():
                return jsonify({"error": "Unauthorized: Invalid API key"}), 401

            return f(*args, **kwargs)

        return decorated

    return decorator


def webhook_key_required(f):
    """
    Decorator that validates the X-API-Key header against WEBHOOK_API_KEY env var.
    Used for external webhook integrations (Make.com, Zapier, etc.)
    """
    return api_key_required(
        "WEBHOOK_API_KEY",
        header_name="X-API-Key",
        config_label="Webhook API key"
    )(f)


def website_key_required(f):
    """
    Decorator for website/server-side form submissions.
    Uses a separate shared key so website integrations do not reuse the generic webhook key.
    """
    return api_key_required(
        "WEBSITE_FORM_API_KEY",
        header_name="X-Website-Key",
        config_label="Website form API key"
    )(f)
