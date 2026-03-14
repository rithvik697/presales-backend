"""
Service: Lead Status History
Tracks every status change a lead goes through.
Table: lead_status_history

"Activity" and "Status" are the same concept in this app.
"""

from db import get_db
from services.audit_service import log_audit
from services.notification_service import create_notification
from services.followup_calls_service import get_scheduled_activities_by_lead
from services.lead_comments_service import get_comments_by_lead


def get_history_by_lead(lead_id):
    """Fetch status, reassignment, scheduled-activity, and comment history for a lead."""

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                h.history_id,
                h.lead_id,
                'status_change' AS event_type,
                h.old_status_id,
                h.new_status_id,
                NULL AS old_assigned_to,
                NULL AS new_assigned_to,
                h.remarks,
                h.changed_by,
                h.changed_at,
                os.status_name  AS old_status_name,
                ns.status_name  AS new_status_name,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS changed_by_name
            FROM lead_status_history h
            LEFT JOIN lead_status os ON os.status_id = h.old_status_id
            LEFT JOIN lead_status ns ON ns.status_id = h.new_status_id
            LEFT JOIN employee e ON e.emp_id = h.changed_by
            WHERE h.lead_id = %s
            ORDER BY h.changed_at DESC
        """, (lead_id,))

        status_rows = cursor.fetchall()

        cursor.execute("""
            SELECT
                a.audit_id AS history_id,
                a.object_id AS lead_id,
                'assignment_change' AS event_type,
                NULL AS old_status_id,
                NULL AS new_status_id,
                CONCAT(emp_old.emp_first_name, ' ', COALESCE(emp_old.emp_last_name, '')) AS old_assigned_to,
                CONCAT(emp_new.emp_first_name, ' ', COALESCE(emp_new.emp_last_name, '')) AS new_assigned_to,
                NULL AS remarks,
                a.modified_by AS changed_by,
                a.modified_on AS changed_at,
                NULL AS old_status_name,
                NULL AS new_status_name,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS changed_by_name
            FROM audit_trail a
            LEFT JOIN employee emp_old ON a.old_value = emp_old.emp_id
            LEFT JOIN employee emp_new ON a.new_value = emp_new.emp_id
            LEFT JOIN employee e ON a.modified_by = e.emp_id
            WHERE a.object_name = 'Leads'
              AND a.object_id = %s
              AND a.property_name = 'emp_id'
              AND a.action_type = 'UPDATE'
            ORDER BY a.modified_on DESC
        """, (lead_id,))

        assignment_rows = cursor.fetchall()

        scheduled_rows = []
        for activity in get_scheduled_activities_by_lead(lead_id):
            scheduled_rows.append({
                'history_id': activity['schedule_id'],
                'lead_id': activity['lead_id'],
                'event_type': 'scheduled_activity',
                'old_status_id': None,
                'new_status_id': None,
                'old_assigned_to': None,
                'new_assigned_to': None,
                'remarks': activity.get('remarks'),
                'changed_by': activity['created_by'],
                'changed_by_name': activity.get('created_by_name'),
                'changed_at': activity['created_on'],
                'old_status_name': None,
                'new_status_name': None,
                'scheduled_at': activity['scheduled_at'],
                'schedule_status': activity.get('status', 'SCHEDULED'),
                'scheduled_status_id': activity['status_id'],
                'scheduled_status_name': activity.get('status_name')
            })

        comment_rows = []
        for comment in get_comments_by_lead(lead_id):
            comment_rows.append({
                'history_id': comment['comment_id'],
                'lead_id': comment['lead_id'],
                'event_type': 'comment',
                'old_status_id': None,
                'new_status_id': None,
                'old_assigned_to': None,
                'new_assigned_to': None,
                'remarks': comment['comment_text'],
                'changed_by': comment['created_by'],
                'changed_by_name': comment.get('created_by_name'),
                'changed_at': comment['created_on'],
                'old_status_name': None,
                'new_status_name': None
            })

        rows = status_rows + assignment_rows + scheduled_rows + comment_rows

        for row in rows:
            if row.get('changed_at'):
                changed_at = row['changed_at']
                row['changed_at'] = changed_at.isoformat() if hasattr(changed_at, 'isoformat') else changed_at

        rows.sort(key=lambda r: r.get('changed_at') or '', reverse=True)

        return rows

    finally:
        cursor.close()
        conn.close()


def get_history_entry(history_id):

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                h.history_id,
                h.lead_id,
                h.old_status_id,
                h.new_status_id,
                h.remarks,
                h.changed_by,
                h.changed_at,
                os.status_name AS old_status_name,
                ns.status_name AS new_status_name,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS changed_by_name
            FROM lead_status_history h
            LEFT JOIN lead_status os ON os.status_id = h.old_status_id
            LEFT JOIN lead_status ns ON ns.status_id = h.new_status_id
            LEFT JOIN employee e ON e.emp_id = h.changed_by
            WHERE h.history_id = %s
        """, (history_id,))

        row = cursor.fetchone()

        if row and row.get('changed_at'):
            row['changed_at'] = row['changed_at'].isoformat()

        return row

    finally:
        cursor.close()
        conn.close()


def create_history(lead_id, data, emp_id):

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:

        cursor.execute(
            "SELECT status_id FROM leads WHERE lead_id = %s",
            (lead_id,)
        )

        lead = cursor.fetchone()

        if not lead:
            raise ValueError(f"Lead {lead_id} not found")

        old_status_id = lead["status_id"]
        new_status_id = data.get("new_status_id")
        remarks = data.get("remarks", "")

        if not new_status_id:
            raise ValueError("new_status_id is required")

        cursor.execute("""
            INSERT INTO lead_status_history
            (lead_id, old_status_id, new_status_id, remarks, changed_by)
            VALUES (%s,%s,%s,%s,%s)
        """, (
            lead_id,
            old_status_id,
            new_status_id,
            remarks,
            emp_id
        ))

        history_id = cursor.lastrowid

        cursor.execute("""
            UPDATE leads
            SET status_id = %s,
                lead_description = %s,
                modified_by = %s,
                modified_on = NOW()
            WHERE lead_id = %s
        """, (
            new_status_id,
            remarks,
            emp_id,
            lead_id
        ))

        log_audit(
            "Leads",
            lead_id,
            "status_id",
            old_status_id,
            new_status_id,
            emp_id,
            "UPDATE"
        )

        cursor.execute("""
            SELECT status_name
            FROM lead_status
            WHERE status_id = %s
        """, (new_status_id,))

        status_row = cursor.fetchone()
        status_name = status_row["status_name"] if status_row else None

        cursor.execute("""
            SELECT TRIM(CONCAT(c.customer_first_name,' ',IFNULL(c.customer_last_name,''))) AS lead_name
            FROM leads l
            JOIN customer c ON l.customer_id = c.customer_id
            WHERE l.lead_id = %s
        """, (lead_id,))

        lead_row = cursor.fetchone()
        lead_name = lead_row["lead_name"] if lead_row else lead_id

        # SITE VISIT → ALL USERS

        if status_name in ["Expected Site Visit", "Site Visit Done"]:

            if status_name == "Expected Site Visit":
                message = f"{lead_name} ({lead_id}) is expected to visit the site."
            else:
                message = f"{lead_name} ({lead_id}) has completed the site visit."

            cursor.execute("""
                SELECT emp_id
                FROM employee
                WHERE emp_status = 'Active'
            """)

            users = cursor.fetchall()

            for user in users:
                create_notification(
                    user["emp_id"],
                    status_name,
                    message,
                    "Leads",
                    lead_id
                )

        # OFFICE VISIT → ADMINS ONLY

        if status_name in ["Expected Office Visit", "Office Visit Done"]:

            if status_name == "Expected Office Visit":
                message = f"{lead_name} ({lead_id}) is expected to visit the office."
            else:
                message = f"{lead_name} ({lead_id}) has completed the office visit."

            cursor.execute("""
                SELECT emp_id
                FROM employee
                WHERE emp_status = 'Active'
                AND role_id = 'ADMIN'
            """)

            admins = cursor.fetchall()

            for admin in admins:
                create_notification(
                    admin["emp_id"],
                    status_name,
                    message,
                    "Leads",
                    lead_id
                )

        conn.commit()

        return get_history_entry(history_id)

    except Exception as e:

        conn.rollback()
        raise e

    finally:

        cursor.close()
        conn.close()


def update_history(history_id, data, emp_id):

    conn = get_db()
    cursor = conn.cursor()

    try:

        cursor.execute("""
            UPDATE lead_status_history
            SET remarks = %s
            WHERE history_id = %s
        """, (data.get('remarks', ''), history_id))

        conn.commit()

        return get_history_entry(history_id)

    finally:

        cursor.close()
        conn.close()


def delete_history(history_id):

    conn = get_db()
    cursor = conn.cursor()

    try:

        cursor.execute(
            "DELETE FROM lead_status_history WHERE history_id = %s",
            (history_id,)
        )

        conn.commit()

        return cursor.rowcount > 0

    finally:

        cursor.close()
        conn.close()


def get_status_options():

    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:

        cursor.execute("""
            SELECT status_id, status_name, status_category
            FROM lead_status
            WHERE is_active = 1
            ORDER BY status_name
        """)

        return cursor.fetchall()

    finally:

        cursor.close()
        conn.close()
