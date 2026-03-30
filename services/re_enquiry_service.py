import logging
from services.notification_service import create_notification
from services.email_service import send_html_email

logger = logging.getLogger(__name__)


def find_existing_lead_assignment(cursor, phone):
    if not phone:
        return None

    clean_phone = ''.join(filter(str.isdigit, str(phone)))[-10:]

    cursor.execute("""
        SELECT
            l.lead_id,
            l.emp_id AS owner_emp_id,
            TRIM(CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, ''))) AS owner_name,
            e.role_id AS owner_role,
            e.email AS owner_email
        FROM leads l
        JOIN customer c ON l.customer_id = c.customer_id
        LEFT JOIN employee e ON l.emp_id = e.emp_id
        WHERE l.is_active = 1
          AND (
            c.phone_num = %s
            OR REPLACE(REPLACE(REPLACE(c.phone_num, ' ', ''), '-', ''), '+', '') = %s
          )
        ORDER BY l.created_on DESC
        LIMIT 1
    """, (phone, clean_phone))

    return cursor.fetchone()


def _get_employee_name(cursor, emp_id):
    if not emp_id:
        return None

    cursor.execute("""
        SELECT TRIM(CONCAT(emp_first_name, ' ', IFNULL(emp_last_name, ''))) AS full_name
        FROM employee
        WHERE emp_id = %s
        LIMIT 1
    """, (emp_id,))
    row = cursor.fetchone()
    return row["full_name"] if row and row.get("full_name") else None


def notify_admin_owned_reenquiry(cursor, phone, source_channel, handling_emp_id=None):
    lead_owner = find_existing_lead_assignment(cursor, phone)
    if not lead_owner or lead_owner.get("owner_role") != "ADMIN" or not lead_owner.get("owner_emp_id"):
        return None

    handler_name = _get_employee_name(cursor, handling_emp_id) or "an active sales executive"
    clean_phone = ''.join(filter(str.isdigit, str(phone or '')))[-10:] or str(phone or "Unknown")
    source_label = source_channel or "Lead Source"

    message = (
        f"Parked lead {lead_owner['lead_id']} has re-enquired via {source_label} "
        f"from {clean_phone}. Handled by {handler_name}."
    )

    try:
        create_notification(
            lead_owner["owner_emp_id"],
            "Parked Lead Re-enquired",
            message,
            "Leads",
            lead_owner["lead_id"]
        )
    except Exception as exc:
        logger.warning(f"Admin re-enquiry notification failed for {lead_owner['lead_id']}: {exc}")

    owner_email = lead_owner.get("owner_email")
    if owner_email:
        html = f"""
        <html><body style="font-family: Arial, sans-serif; color: #333;">
          <h3>Parked Lead Re-enquiry Alert</h3>
          <p>Lead <strong>{lead_owner['lead_id']}</strong> assigned to admin has re-enquired.</p>
          <p><strong>Source:</strong> {source_label}</p>
          <p><strong>Phone:</strong> {clean_phone}</p>
          <p><strong>Handled by:</strong> {handler_name}</p>
          <p>Please review and reassign the lead to the appropriate sales executive if needed.</p>
        </body></html>
        """
        try:
            send_html_email(owner_email, f"Parked Lead Re-enquiry - {lead_owner['lead_id']}", html)
        except Exception as exc:
            logger.warning(f"Admin re-enquiry email failed for {lead_owner['lead_id']}: {exc}")

    return {
        "lead_id": lead_owner["lead_id"],
        "owner_emp_id": lead_owner["owner_emp_id"],
        "owner_name": lead_owner.get("owner_name"),
        "source_channel": source_label
    }
