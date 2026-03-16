from flask import Blueprint, request, jsonify
from decorators.auth_decorators import token_required
from services.project_assignment_service import (
    get_project_assignments,
    create_project_assignment,
    delete_project_assignment,
)


project_assignment_bp = Blueprint("project_assignment_bp", __name__)


@project_assignment_bp.route("/config/project-assignments", methods=["GET"])
@token_required
def list_project_assignments(decoded):
    try:
        assignments = get_project_assignments()
        return jsonify(assignments), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@project_assignment_bp.route("/config/project-assignments", methods=["POST"])
@token_required
def add_project_assignment(decoded):
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    project_id = data.get("project_id")
    emp_id = data.get("emp_id")

    if not project_id or not emp_id:
        return jsonify({"error": "project_id and emp_id are required"}), 400

    try:
        created_by = decoded.get("sub") or decoded.get("username", "SYSTEM")
        assignment = create_project_assignment(project_id, emp_id, created_by)
        return jsonify(assignment), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@project_assignment_bp.route("/config/project-assignments/<int:mapping_id>", methods=["DELETE"])
@token_required
def remove_project_assignment(decoded, mapping_id):
    try:
        deleted_by = decoded.get("sub") or decoded.get("username", "SYSTEM")
        deleted = delete_project_assignment(mapping_id, deleted_by)
        if not deleted:
            return jsonify({"error": "Mapping not found"}), 404
        return jsonify({"message": "Mapping removed successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
