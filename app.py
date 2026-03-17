from flask import Flask
from flask_cors import CORS

# Import Blueprints
from controllers.project_controller import project_bp
from controllers.call_logs_controller import call_logs_bp
from controllers.user_controller import user_controller_bp 
from controllers.auth_controller import auth_controller_bp
from controllers.leads_controller import leads_bp
from controllers.lead_status_history_controller import lead_status_history_bp
from controllers.audit_controller import audit_controller_bp
from controllers.reports_controller import reports_bp
from services.scheduler_service import init_scheduler
from controllers.notification_controller import notification_bp
from controllers.project_assignment_controller import project_assignment_bp
from controllers.lead_transfer_controller import lead_transfer_bp

app = Flask(__name__)
CORS(app)

# Initialize Scheduler
init_scheduler(app)


# Register Blueprints
app.register_blueprint(auth_controller_bp, url_prefix='/api')
app.register_blueprint(project_bp, url_prefix="/api")
app.register_blueprint(call_logs_bp, url_prefix="/api/calls")
app.register_blueprint(user_controller_bp, url_prefix="/api")
app.register_blueprint(leads_bp, url_prefix="/api/leads")
app.register_blueprint(lead_status_history_bp, url_prefix='/api')
app.register_blueprint(audit_controller_bp, url_prefix="/api")
app.register_blueprint(reports_bp, url_prefix="/api/reports")
app.register_blueprint(notification_bp, url_prefix="/api/notifications")
app.register_blueprint(project_assignment_bp, url_prefix="/api")
app.register_blueprint(lead_transfer_bp, url_prefix="/api")




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
