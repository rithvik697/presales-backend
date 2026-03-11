from flask import Blueprint, jsonify, request, Response
from decorators.auth_decorators import token_required
import services.reports_service as reports_service
import csv

reports_bp = Blueprint('reports_controller', __name__)

def is_authorized(decoded):
    return decoded.get("role_type") in ["ADMIN", "Sales Manager"]

@reports_bp.route('/summary', methods=['GET'])
@token_required
def get_summary(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_reports_summary(start_date, end_date, project_id, user_id)
    if result.get("success"):
        return jsonify(result), 200
    return jsonify({"error": result.get("message")}), 500

@reports_bp.route('/weekly', methods=['GET'])
@token_required
def get_weekly(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_weekly_leads(start_date, end_date, project_id, user_id)
    if result.get("success"):
        return jsonify(result), 200
    return jsonify({"error": result.get("message")}), 500

@reports_bp.route('/monthly', methods=['GET'])
@token_required
def get_monthly(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_monthly_leads(start_date, end_date, project_id, user_id)
    if result.get("success"):
        return jsonify(result), 200
    return jsonify({"error": result.get("message")}), 500

@reports_bp.route('/annual', methods=['GET'])
@token_required
def get_annual(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_annual_leads(start_date, end_date, project_id, user_id)
    if result.get("success"):
        return jsonify(result), 200
    return jsonify({"error": result.get("message")}), 500

@reports_bp.route('/status', methods=['GET'])
@token_required
def get_status_distribution(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_leads_by_status(start_date, end_date, project_id, user_id)
    if result.get("success"):
        return jsonify(result), 200
    return jsonify({"error": result.get("message")}), 500

@reports_bp.route('/user-performance', methods=['GET'])
@token_required
def get_user_performance(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_user_performance(start_date, end_date, project_id, user_id)
    if result.get("success"):
        return jsonify(result), 200
    return jsonify({"error": result.get("message")}), 500

@reports_bp.route('/daily-log', methods=['GET'])
@token_required
def get_daily_log(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_daily_log(project_id, user_id)
    if result.get("success"):
        return jsonify(result), 200
    return jsonify({"error": result.get("message")}), 500

@reports_bp.route('/download', methods=['GET'])
@token_required
def download_active_leads(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    result = reports_service.get_active_leads_for_download()
    if not result.get("success"):
        return jsonify({"error": result.get("message")}), 500
        
    rows = result.get("data", [])
    columns = result.get("columns", [])
    
    def generate():
        # Header
        yield ','.join(columns) + '\n'
        # Data
        for r in rows:
            # Escape commas by enclosing in quotes
            clean_row = []
            for item in r:
                val = str(item) if item is not None else ""
                if ',' in val or '"' in val:
                    val = f'"{val.replace('"', '""')}"'  
                clean_row.append(val)
            yield ','.join(clean_row) + '\n'
            
    return Response(generate(), mimetype="text/csv", headers={"Content-Disposition": "attachment;filename=active_leads.csv"})
