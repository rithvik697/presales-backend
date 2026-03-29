from flask import Blueprint, jsonify, request

from decorators.auth_decorators import token_required
from services.bulk_upload_service import get_bulk_upload_history, process_bulk_lead_upload


bulk_upload_bp = Blueprint("bulk_upload_bp", __name__)


@bulk_upload_bp.route("/config/bulk-leads/history", methods=["GET"])
@token_required
def list_bulk_upload_history(decoded):
    try:
        return jsonify(get_bulk_upload_history()), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bulk_upload_bp.route("/config/bulk-leads/upload", methods=["POST"])
@token_required
def upload_bulk_leads(decoded):
    if decoded.get("role_type") not in ["ADMIN", "SALES_MGR"]:
        return jsonify({"error": "Permission denied"}), 403

    upload_file = request.files.get("file")
    if not upload_file:
        return jsonify({"error": "No file provided"}), 400

    # 5 MB max file size
    MAX_FILE_SIZE = 5 * 1024 * 1024
    upload_file.seek(0, 2)
    file_size = upload_file.tell()
    upload_file.seek(0)

    if file_size > MAX_FILE_SIZE:
        return jsonify({"error": "File too large. Maximum size is 5 MB."}), 413

    actor_id = decoded.get("sub") or decoded.get("username", "SYSTEM")

    try:
        result = process_bulk_lead_upload(upload_file, actor_id)
        return jsonify(result), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
