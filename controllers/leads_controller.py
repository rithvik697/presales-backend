from flask import Blueprint, request, jsonify
from services import leads_service
from utils.token_helper import get_emp_id_from_token

leads_bp = Blueprint('leads', __name__)

def to_frontend_format(backend_lead):
    """
    Maps backend flat dictionary to frontend CamelCase.
    The Data Layer now returns mostly formatted data, we just enforce CamelCase keys.
    """
    if not backend_lead:
        return None
    
    description = backend_lead.get('description') or ''
    project = backend_lead.get('project')
    projectId = backend_lead.get('projectId')
    
    return {
        'id': backend_lead.get('id'),
        'name': backend_lead.get('name'),
        'phone': backend_lead.get('phone'),
        'alternatePhone': backend_lead.get('alternatePhone'),
        'email': backend_lead.get('email'),
        'profession': backend_lead.get('profession'),
        'project': project, 
        'projectId': projectId,
        'source': backend_lead.get('source'),
        'status': backend_lead.get('status'),
        'assignedTo': backend_lead.get('assignedTo'), 
        'description': description,
        'createdAt': backend_lead.get('createdAt'),
        'createdBy': backend_lead.get('createdBy'),
        'modifiedAt': backend_lead.get('modifiedAt'),
        'modifiedBy': backend_lead.get('modifiedBy')
    }

@leads_bp.route('', methods=['GET'])
def get_leads():
    try:
        filters = {
            'customer': request.args.get('customer'),
            'mobile': request.args.get('mobile'),
            'source': request.args.get('source'),
            'employee': request.args.get('employee'),
            'project': request.args.get('project')
        }
        leads = leads_service.fetch_all_leads(filters)
        return jsonify([to_frontend_format(lead) for lead in leads]), 200
    except Exception as e:
        print(f"API Error: {e}")
        return jsonify({'error': str(e)}), 500

@leads_bp.route('/<string:lead_id>', methods=['GET'])
def get_lead(lead_id):
    try:
        lead = leads_service.fetch_lead_by_id(lead_id)
        if lead:
            return jsonify(to_frontend_format(lead)), 200
        return jsonify({'message': 'Lead not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@leads_bp.route('', methods=['POST'])
def create_lead():
    try:
        req_data = request.json

        # Identify the caller from JWT — never trust the request body for this
        actor_id = get_emp_id_from_token()
        if not actor_id:
            return jsonify({'error': 'Unauthorized: valid token required'}), 401

        data = {
            'name': req_data.get('name'),
            'phone': req_data.get('phone'),
            'email': req_data.get('email'),
            'project': req_data.get('project'),
            'source': req_data.get('source'),
            'status': req_data.get('status'),
            'assigned_to': req_data.get('assignedTo'),
            'description': req_data.get('description'),
            'alternate_phone': req_data.get('alternatePhone'),
            'profession': req_data.get('profession')
        }

        new_id = leads_service.add_new_lead(data, actor_name=actor_id)
        return jsonify({'id': new_id, 'message': 'Lead created successfully'}), 201
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@leads_bp.route('/employees', methods=['GET'])
def get_employees():
    try:
        employees = leads_service.fetch_all_employees()
        return jsonify(employees), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@leads_bp.route('/sources', methods=['GET'])
def get_all_sources():
    try:
        sources = leads_service.fetch_all_sources()
        return jsonify(sources), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@leads_bp.route('/statuses', methods=['GET'])
def get_all_statuses():
    try:
        statuses = leads_service.fetch_all_statuses()
        return jsonify(statuses), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@leads_bp.route('/<string:lead_id>', methods=['PUT'])
def update_lead(lead_id):
    try:
        data = request.json

        # Identify the caller from JWT — never trust the request body for this
        actor_id = get_emp_id_from_token()
        if not actor_id:
            return jsonify({'error': 'Unauthorized: valid token required'}), 401

        update_data = {
            'name': data.get('name'),
            'email': data.get('email'),
            'source': data.get('source'),
            'status': data.get('status'),
            'assigned_to': data.get('assignedTo'),
            'project': data.get('project'),
            'description': data.get('description'),
            'alternate_phone': data.get('alternatePhone'),
            'profession': data.get('profession')
        }

        success = leads_service.update_existing_lead(lead_id, update_data, actor_name=actor_id)
        if success:
            return jsonify({'message': 'Lead updated successfully'}), 200
        else:
            return jsonify({'error': 'Failed to update lead'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@leads_bp.route('/<string:lead_id>', methods=['DELETE'])
def delete_lead_api(lead_id):
    try:
        success = leads_service.delete_existing_lead(lead_id)
        if success:
            return jsonify({'message': 'Lead deleted successfully'}), 200
        else:
            return jsonify({'error': 'Failed to delete lead'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
