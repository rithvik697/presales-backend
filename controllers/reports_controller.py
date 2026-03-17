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
                    val = '"' + val.replace('"', '""') + '"'
                clean_row.append(val)
            yield ','.join(clean_row) + '\n'
            
    return Response(generate(), mimetype="text/csv", headers={"Content-Disposition": "attachment;filename=active_leads.csv"})

@reports_bp.route('/active-leads-json', methods=['GET'])
@token_required
def get_active_leads_json(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    result = reports_service.get_active_leads_for_download()
    if not result.get("success"):
        return jsonify({"error": result.get("message")}), 500
        
    return jsonify(result), 200

@reports_bp.route('/user-leads-export', methods=['GET'])
@token_required
def export_user_leads(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    emp_id = request.args.get('emp_id')
    user_name = request.args.get('user_name', '')
    activity = request.args.get('activity')
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    
    if not emp_id or not activity:
        return jsonify({"error": "emp_id and activity are required"}), 400
        
    result = reports_service.get_user_leads_export(emp_id, activity, start_date, end_date, project_id)
    if not result.get("success"):
        return jsonify({"error": result.get("message")}), 500
        
    rows = result.get("data", [])
    
    def generate():
        yield f'"EMP ID: {emp_id}","User Name: {user_name}"\n\n'
        yield ','.join(["Lead ID", "Lead Name", "Activity Status", "Description", "Project", "Created On", "Current Status"]) + '\n'
        for r in rows:
            clean_row = []
            row_data = [
                r.get('lead_id', ''),
                r.get('lead_name', '').strip(),
                activity,
                r.get('lead_description', ''),
                r.get('project_name', ''),
                r.get('created_on', ''),
                r.get('status_name', '')
            ]
            for val in row_data:
                val = str(val) if val is not None else ""
                if ',' in val or '"' in val:
                    val = '"' + val.replace('"', '""') + '"'
                clean_row.append(val)
            yield ','.join(clean_row) + '\n'
            
    filename = f"leads_{emp_id}_{activity.replace(' ', '_')}.csv"
    return Response(generate(), mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename={filename}"})

@reports_bp.route('/user-leads-export-json', methods=['GET'])
@token_required
def export_user_leads_json(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    emp_id = request.args.get('emp_id')
    activity = request.args.get('activity')
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    
    if not emp_id or not activity:
        return jsonify({"error": "emp_id and activity are required"}), 400
        
    result = reports_service.get_user_leads_export(emp_id, activity, start_date, end_date, project_id)
    if not result.get("success"):
        return jsonify({"error": result.get("message")}), 500
        
    return jsonify(result), 200

@reports_bp.route('/summary-leads', methods=['GET'])
@token_required
def summary_leads(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    summary_type = request.args.get('type')
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    if not summary_type:
        return jsonify({"error": "type is required"}), 400
        
    result = reports_service.get_summary_leads(summary_type, start_date, end_date, project_id, user_id)
    if not result.get("success"):
        return jsonify({"error": result.get("message")}), 500
        
    return jsonify(result), 200

@reports_bp.route('/weekly-log', methods=['GET'])
@token_required
def get_weekly_log(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    
    result = reports_service.get_weekly_report_log(start_date, end_date, project_id, user_id)
    if not result.get("success"):
        return jsonify({"error": result.get("message")}), 500
        
    return jsonify(result), 200

@reports_bp.route('/monthly-performance-report', methods=['GET'])
@token_required
def get_monthly_performance_report(decoded):
    if not is_authorized(decoded):
        return jsonify({"message": "Unauthorized"}), 403
        
    target_month = request.args.get('month')
    target_year = request.args.get('year')
    project_id = request.args.get('projectId')
    
    result = reports_service.get_monthly_performance_report(target_month, target_year, project_id)
    if not result.get("success"):
        return jsonify({"error": result.get("message")}), 500
        
    return jsonify(result), 200
