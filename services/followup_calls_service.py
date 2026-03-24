from db import get_db
from services.audit_service import log_audit
from services.notification_service import create_notification
from datetime import datetime


def _ensure_scheduled_activities_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lead_scheduled_activities (
            schedule_id INT AUTO_INCREMENT PRIMARY KEY,
            lead_id VARCHAR(20) NOT NULL,
            status_id VARCHAR(20) NOT NULL,
            scheduled_at DATETIME NOT NULL,
            remarks TEXT NULL,
            created_by VARCHAR(20) NOT NULL,
            created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED'
        )
    """)


def create_scheduled_activity(lead_id, status_id, scheduled_at, remarks, created_by):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        _ensure_scheduled_activities_table(cursor)
        normalized_scheduled_at = str(scheduled_at).replace('T', ' ')

        cursor.execute(
            """
            SELECT
                l.lead_id,
                l.status_id,
                l.customer_id
            FROM leads l
            WHERE l.lead_id = %s AND l.is_active = 1
            """,
            (lead_id,)
        )
        lead = cursor.fetchone()

        if not lead:
            raise ValueError(f"Lead {lead_id} not found")

        cursor.execute(
            "SELECT status_id FROM lead_status WHERE status_id = %s",
            (status_id,)
        )
        status = cursor.fetchone()

        if not status:
            raise ValueError(f"Status {status_id} not found")

        old_status_id = lead.get('status_id')

        cursor.execute("""
            INSERT INTO lead_scheduled_activities
                (lead_id, status_id, scheduled_at, remarks, created_by, status)
            VALUES (%s, %s, %s, %s, %s, 'SCHEDULED')
        """, (
            lead_id,
            status_id,
            normalized_scheduled_at,
            remarks,
            created_by
        ))

        cursor.execute("""
            INSERT INTO lead_status_history
                (lead_id, old_status_id, new_status_id, remarks, changed_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            lead_id,
            old_status_id,
            status_id,
            remarks or '',
            created_by
        ))

        cursor.execute("""
            UPDATE leads
            SET status_id = %s,
                modified_by = %s,
                modified_on = NOW()
            WHERE lead_id = %s
        """, (
            status_id,
            created_by,
            lead_id
        ))

        if old_status_id != status_id:
            log_audit(
                "Leads",
                lead_id,
                "status_id",
                old_status_id,
                status_id,
                created_by,
                "UPDATE"
            )

        cursor.execute("""
            SELECT status_name
            FROM lead_status
            WHERE status_id = %s
        """, (status_id,))
        status_row = cursor.fetchone()
        status_name = status_row["status_name"] if status_row else None

        cursor.execute("""
            SELECT TRIM(CONCAT(c.customer_first_name,' ',IFNULL(c.customer_last_name,''))) AS lead_name
            FROM customer c
            WHERE c.customer_id = %s
        """, (lead["customer_id"],))
        lead_row = cursor.fetchone()
        lead_name = lead_row["lead_name"] if lead_row else lead_id

        if status_name in ["Expected Site Visit", "Site Visit Done"]:
            if status_name == "Expected Site Visit":
                visit_time = normalized_scheduled_at
                try:
                    visit_time = datetime.strptime(normalized_scheduled_at, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %I:%M %p")
                except Exception:
                    pass

                message = f"{lead_name} ({lead_id}) is expected to visit the site on {visit_time}."
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

        schedule_id = cursor.lastrowid
        conn.commit()

        cursor.execute("""
            SELECT
                s.schedule_id,
                s.lead_id,
                s.status_id,
                ls.status_name,
                s.scheduled_at,
                s.remarks,
                s.created_by,
                s.created_on,
                s.status,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS created_by_name
            FROM lead_scheduled_activities s
            LEFT JOIN lead_status ls ON ls.status_id = s.status_id
            LEFT JOIN employee e ON e.emp_id = s.created_by
            WHERE s.schedule_id = %s
        """, (schedule_id,))

        row = cursor.fetchone()

        if row and row.get('scheduled_at'):
            row['scheduled_at'] = row['scheduled_at'].isoformat()
        if row and row.get('created_on'):
            row['created_on'] = row['created_on'].isoformat()
        if row and row.get('created_by_name'):
            row['created_by_name'] = row['created_by_name'].strip()

        return row

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def get_scheduled_activities_by_lead(lead_id):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        _ensure_scheduled_activities_table(cursor)

        cursor.execute("""
            SELECT
                s.schedule_id,
                s.lead_id,
                s.status_id,
                ls.status_name,
                s.scheduled_at,
                s.remarks,
                s.created_by,
                s.created_on,
                s.status,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS created_by_name
            FROM lead_scheduled_activities s
            LEFT JOIN lead_status ls ON ls.status_id = s.status_id
            LEFT JOIN employee e ON e.emp_id = s.created_by
            WHERE s.lead_id = %s
            ORDER BY s.created_on DESC
        """, (lead_id,))

        rows = cursor.fetchall()

        for row in rows:
            if row.get('scheduled_at'):
                row['scheduled_at'] = row['scheduled_at'].isoformat()
            if row.get('created_on'):
                row['created_on'] = row['created_on'].isoformat()
            if row.get('created_by_name'):
                row['created_by_name'] = row['created_by_name'].strip()

        return rows

    finally:
        cursor.close()
        conn.close()
