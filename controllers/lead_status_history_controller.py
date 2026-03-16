from decorators.auth_decorators import token_required
from flask import Blueprint, request, jsonify, g
from services.lead_status_history_service import (
    get_history_by_lead,
    create_history,
    update_history,
    delete_history,
    get_status_options
)
from services.followup_calls_service import create_scheduled_activity
from services.lead_comments_service import create_lead_comment

lead_status_history_bp = Blueprint('lead_status_history', __name__)


@lead_status_history_bp.route('/leads/<lead_id>/status-history', methods=['GET'])
@token_required
def list_history(decoded, lead_id):
    try:
        g.user = decoded
        history = get_history_by_lead(lead_id)
        return jsonify(history), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@lead_status_history_bp.route('/leads/<lead_id>/status-history', methods=['POST'])
@token_required
def add_history(decoded, lead_id):
    g.user = decoded

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    if not data.get('new_status_id'):
        return jsonify({'error': 'new_status_id is required'}), 400

    try:
        emp_id = decoded.get('sub') or decoded.get('username', 'System')

        entry = create_history(lead_id, data, emp_id)

        return jsonify(entry), 201

    except ValueError as ve:
        return jsonify({'error': str(ve)}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@lead_status_history_bp.route('/leads/<lead_id>/scheduled-activities', methods=['POST'])
@token_required
def add_scheduled_activity(decoded, lead_id):
    g.user = decoded

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    status_id = data.get('status_id')
    scheduled_at = data.get('scheduled_at')
    remarks = data.get('remarks', '')

    if not status_id:
        return jsonify({'error': 'status_id is required'}), 400
    if not scheduled_at:
        return jsonify({'error': 'scheduled_at is required'}), 400

    try:
        emp_id = decoded.get('sub') or decoded.get('username', 'System')
        entry = create_scheduled_activity(lead_id, status_id, scheduled_at, remarks, emp_id)
        return jsonify(entry), 201

    except ValueError as ve:
        return jsonify({'error': str(ve)}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@lead_status_history_bp.route('/leads/<lead_id>/comments', methods=['POST'])
@token_required
def add_comment(decoded, lead_id):
    g.user = decoded

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    comment_text = (data.get('comment_text') or '').strip()
    if not comment_text:
        return jsonify({'error': 'comment_text is required'}), 400

    try:
        emp_id = decoded.get('sub') or decoded.get('username', 'System')
        entry = create_lead_comment(lead_id, comment_text, emp_id)
        return jsonify(entry), 201

    except ValueError as ve:
        return jsonify({'error': str(ve)}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@lead_status_history_bp.route('/leads/<lead_id>/status-history/<int:history_id>', methods=['PUT'])
@token_required
def edit_history(decoded, lead_id, history_id):
    g.user = decoded

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    try:
        emp_id = decoded.get('sub') or decoded.get('username', 'System')

        entry = update_history(history_id, data, emp_id)

        if not entry:
            return jsonify({'error': 'History entry not found'}), 404

        return jsonify(entry), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@lead_status_history_bp.route('/leads/<lead_id>/status-history/<int:history_id>', methods=['DELETE'])
@token_required
def remove_history(decoded, lead_id, history_id):
    g.user = decoded

    user_role = decoded.get('role_type', '')

    if user_role != 'ADMIN':
        return jsonify({'error': 'Only admins can delete history entries'}), 403

    try:
        deleted = delete_history(history_id)

        if not deleted:
            return jsonify({'error': 'History entry not found'}), 404

        return jsonify({'message': 'Deleted successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@lead_status_history_bp.route('/status-options', methods=['GET'])
@token_required
def list_status_options(decoded):
    try:
        g.user = decoded
        options = get_status_options()
        return jsonify(options), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
