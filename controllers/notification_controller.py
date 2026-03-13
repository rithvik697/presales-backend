from flask import Blueprint, jsonify
from db import get_db
from utils.token_helper import get_emp_id_from_token

notification_bp = Blueprint("notifications", __name__)


@notification_bp.route("", methods=["GET"])
def get_notifications():

    emp_id = get_emp_id_from_token()
    print("NOTIFICATION FETCH FOR:", emp_id)

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT notification_id,title,message,is_read,created_on
        FROM notifications
        WHERE emp_id=%s
        ORDER BY created_on DESC
        LIMIT 10
    """,(emp_id,))

    notifications = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(notifications)