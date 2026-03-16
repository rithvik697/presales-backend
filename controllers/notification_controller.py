from flask import Blueprint, jsonify
from db import get_db
from utils.token_helper import get_emp_id_from_token

notification_bp = Blueprint("notifications", __name__)


# ============================
# GET NOTIFICATIONS
# ============================

@notification_bp.route("", methods=["GET"])
def get_notifications():

    emp_id = get_emp_id_from_token()
    print("NOTIFICATION FETCH FOR:", emp_id)

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT notification_id, title, message, is_read, created_on
        FROM notifications
        WHERE emp_id = %s
        ORDER BY created_on DESC
        LIMIT 10
    """, (emp_id,))

    notifications = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(notifications)


# ============================
# MARK NOTIFICATION AS READ
# ============================

@notification_bp.route("/<int:notification_id>/read", methods=["PUT"])
def mark_notification_read(notification_id):

    emp_id = get_emp_id_from_token()

    print("MARK READ:", notification_id, "FOR:", emp_id)

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE notifications
        SET is_read = 1
        WHERE notification_id = %s AND emp_id = %s
    """, (notification_id, emp_id))

    conn.commit()

    cursor.close()
    conn.close()

    return jsonify({
        "message": "Notification marked as read",
        "notification_id": notification_id
    })