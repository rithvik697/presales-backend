from flask import Blueprint, request, jsonify
from services.project_service import project_service
import traceback
project_bp = Blueprint(
    "project_bp",
    __name__
)

# -----------------------------
# Create Project
# -----------------------------
@project_bp.route('/projects', methods=['POST'])
def create_project():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid or missing JSON body"}), 400

        project_id = project_service.create_project(data)

        return jsonify({
            "message": "Project created successfully",
            "project_id": project_id
        }), 201

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error"}), 500


# -----------------------------
# Get All Projects
# -----------------------------
@project_bp.route('/projects', methods=['GET'])
def get_all_projects():
    try:
        projects = project_service.get_all_projects()
        return jsonify(projects), 200

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error"}), 500


# -----------------------------
# Get Project by ID
# -----------------------------
@project_bp.route('/projects/<project_id>', methods=['GET'])
def get_project_by_id(project_id):
    try:
        project = project_service.get_project_by_id(project_id)

        if not project:
            return jsonify({"error": "Project not found"}), 404

        return jsonify(project), 200

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error"}), 500


# -----------------------------
# Update Project (General Fields)
# -----------------------------
@project_bp.route('/projects/<project_id>', methods=['PUT'])
def update_project(project_id):
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid or missing JSON body"}), 400

        result = project_service.update_project(project_id, data)

        if isinstance(result, dict) and result.get("message") == "Nothing to update":
            return jsonify(result), 400

        return jsonify(result), 200

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error"}), 500


# -----------------------------
# Update Project Status
# -----------------------------
@project_bp.route('/projects/status-options', methods=['GET'])
def get_project_status_options():
    try:
        statuses = ["RERA_APPROVED", "COMPLETED", "PRE_LAUNCH"]

        return jsonify([
            {
                "label": status.replace("_", " ").title(),
                "value": status
            }
            for status in statuses
        ]), 200

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error"}), 500

@project_bp.route('/projects/type-options', methods=['GET'])
def get_project_type_options():
    try:
        types = ["Villa", "Apartment"]

        return jsonify([
            {
                "label": t,
                "value": t
            }
            for t in types
        ]), 200

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error"}), 500
    
@project_bp.route('/projects/<project_id>/status', methods=['PUT'])
def update_project_status(project_id):
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid or missing JSON body"}), 400

        status = data.get("status")
        if not status:
            return jsonify({"error": "Status is required"}), 400

        project_service.update_project_status(project_id, status)

        return jsonify({"message": "Project status updated successfully"}), 200

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    except Exception:
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error"}), 500