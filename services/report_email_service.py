import logging
from db import get_db

logger = logging.getLogger(__name__)


def _serialize_row(row):
    if row and row.get("created_on"):
        row["created_on"] = row["created_on"].isoformat() if hasattr(row["created_on"], "isoformat") else str(row["created_on"])
    return row


def get_all_recipients():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT id, recipient_name, email,
                   weekly_report, monthly_report, quarterly_report, annual_report,
                   is_active, created_by, created_on
            FROM report_email_recipients
            ORDER BY created_on DESC
        """)
        rows = cursor.fetchall()
        return [_serialize_row(r) for r in rows]
    finally:
        cursor.close()
        db.close()


def add_recipient(data, created_by):
    name = (data.get("recipient_name") or "").strip()
    email = (data.get("email") or "").strip()

    if not name:
        raise ValueError("Recipient name is required")
    if not email:
        raise ValueError("Email is required")

    weekly = 1 if data.get("weekly_report", True) else 0
    monthly = 1 if data.get("monthly_report", True) else 0
    quarterly = 1 if data.get("quarterly_report", True) else 0
    annual = 1 if data.get("annual_report", True) else 0

    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("""
            INSERT INTO report_email_recipients
            (recipient_name, email, weekly_report, monthly_report, quarterly_report, annual_report, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (name, email, weekly, monthly, quarterly, annual, created_by))
        db.commit()
        new_id = cursor.lastrowid

        cursor.execute("SELECT * FROM report_email_recipients WHERE id = %s", (new_id,))
        return _serialize_row(cursor.fetchone())
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.close()
        db.close()


def update_recipient(recipient_id, data):
    allowed_fields = ["weekly_report", "monthly_report", "quarterly_report", "annual_report", "is_active"]
    updates = []
    values = []

    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            values.append(1 if data[field] else 0)

    if not updates:
        raise ValueError("No valid fields to update")

    values.append(recipient_id)

    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute(
            f"UPDATE report_email_recipients SET {', '.join(updates)} WHERE id = %s",
            tuple(values)
        )
        db.commit()

        if cursor.rowcount == 0:
            return None

        cursor.execute("SELECT * FROM report_email_recipients WHERE id = %s", (recipient_id,))
        return _serialize_row(cursor.fetchone())
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.close()
        db.close()


def delete_recipient(recipient_id):
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("DELETE FROM report_email_recipients WHERE id = %s", (recipient_id,))
        db.commit()
        return cursor.rowcount > 0
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.close()
        db.close()


def get_recipients_for_report(report_type):
    valid_types = ["weekly_report", "monthly_report", "quarterly_report", "annual_report"]
    if report_type not in valid_types:
        return []

    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute(f"""
            SELECT email FROM report_email_recipients
            WHERE {report_type} = 1 AND is_active = 1
        """)
        rows = cursor.fetchall()
        return [row["email"] for row in rows]
    except Exception as e:
        logger.warning(f"Error fetching report emails for {report_type}: {e}")
        return []
    finally:
        cursor.close()
        db.close()
