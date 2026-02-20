from flask import Flask, jsonify
from flask_cors import CORS
from db import get_db_connection

# Import Blueprints
from controllers.project_controller import project_bp
from controllers.call_logs_controller import call_logs_bp
from controllers.user_controller import user_controller_bp 
from controllers.auth_controller import auth_controller_bp
from controllers.leads_controller import leads_bp

app = Flask(__name__)
CORS(app)

# Register Blueprints
app.register_blueprint(auth_controller_bp, url_prefix='/api')
app.register_blueprint(project_bp, url_prefix="/api")
app.register_blueprint(call_logs_bp, url_prefix="/api/calls")
app.register_blueprint(user_controller_bp, url_prefix="/api")  
app.register_blueprint(leads_bp, url_prefix='/api/leads')




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
