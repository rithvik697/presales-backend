import os
from flask import Flask, Response
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

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
from controllers.mcube_controller import mcube_bp
from controllers.webhook_controller import webhook_bp
from controllers.bulk_upload_controller import bulk_upload_bp
from controllers.website_leads_controller import website_leads_bp
from controllers.report_email_controller import report_email_bp

app = Flask(__name__)
allowed_origins = os.getenv("CORS_ORIGINS", "http://localhost:4200").split(",")
CORS(app, origins=allowed_origins, supports_credentials=True)

# Security headers on every response
@app.after_request
def set_security_headers(response: Response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Cache-Control"] = "no-store"
    return response

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
app.register_blueprint(mcube_bp, url_prefix="/api/calls")
app.register_blueprint(webhook_bp, url_prefix="/api/webhook")
app.register_blueprint(website_leads_bp, url_prefix="/api/website")
app.register_blueprint(bulk_upload_bp, url_prefix="/api")
app.register_blueprint(report_email_bp, url_prefix="/api")




if __name__ == "__main__":
    is_debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=5000, debug=is_debug)
