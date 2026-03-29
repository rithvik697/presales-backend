import os
import logging
import requests
from db import get_db
from services.leads_service import (
    _generate_id,
    _check_duplicate_phone,
    _get_or_create_customer,
    add_new_lead
)
from services.webhook_service import _find_source_by_name, _get_default_status
from services.notification_service import create_notification

logger = logging.getLogger(__name__)

# MCube call status mapping to our CRM statuses
MCUBE_STATUS_MAP = {
    "ANSWERED": "Connected",
    "CONNECTED": "Connected",
    "NO ANSWER": "Not Connected",
    "NOANSWER": "Not Connected",
    "BUSY": "Not Connected",
    "FAILED": "Not Connected",
    "CANCEL": "Not Connected",
    "CONGESTION": "Not Connected",
}


def map_mcube_status(mcube_status):
    if not mcube_status:
        return "Not Connected"
    return MCUBE_STATUS_MAP.get(mcube_status.upper(), "Not Connected")


def _match_lead_by_phone(cursor, phone):
    """Match a lead by customer phone number."""
    if not phone:
        return None

    clean_phone = ''.join(filter(str.isdigit, phone))[-10:]

    cursor.execute("""
        SELECT l.lead_id
        FROM leads l
        JOIN customer c ON l.customer_id = c.customer_id
        WHERE l.is_active = 1
          AND (
            c.phone_num = %s
            OR RIGHT(REPLACE(REPLACE(REPLACE(c.phone_num, ' ', ''), '-', ''), '+', ''), 10) = %s
          )
        ORDER BY l.created_on DESC
        LIMIT 1
    """, (phone, clean_phone))

    result = cursor.fetchone()
    return result["lead_id"] if result else None


def _match_employee_by_phone(cursor, phone):
    """Match an employee by phone number."""
    if not phone:
        return None

    clean_phone = ''.join(filter(str.isdigit, phone))[-10:]

    cursor.execute("""
        SELECT emp_id
        FROM employee
        WHERE emp_status = 'Active'
          AND (
            phone_num = %s
            OR RIGHT(REPLACE(REPLACE(REPLACE(phone_num, ' ', ''), '-', ''), '+', ''), 10) = %s
          )
        LIMIT 1
    """, (phone, clean_phone))

    result = cursor.fetchone()
    return result["emp_id"] if result else None


def _auto_create_lead_from_call(cursor, db, caller_phone, emp_id):
    """
    Auto-create a lead when an inbound call comes from an unknown number.
    Returns the new lead_id or None if creation fails.
    """
    if not caller_phone:
        return None

    clean_phone = ''.join(filter(str.isdigit, caller_phone))[-10:]

    # Check for duplicate (phone might exist but lead was inactive/deleted)
    try:
        _check_duplicate_phone(cursor, clean_phone)
    except ValueError:
        # Duplicate phone — don't create, just return None
        logger.info(f"MCube auto-create skipped: duplicate phone {clean_phone}")
        return None

    # Find "IVR" or "MCube" source, or first available
    source_id = _find_source_by_name(cursor, "IVR")
    if not source_id:
        source_id = _find_source_by_name(cursor, "MCube")
    if not source_id:
        cursor.execute("""
            SELECT source_id FROM lead_sources
            WHERE is_active = 1 ORDER BY source_id ASC LIMIT 1
        """)
        result = cursor.fetchone()
        source_id = result["source_id"] if result else None

    if not source_id:
        logger.warning("MCube auto-create: no lead sources configured")
        return None

    # Get default status (New Enquiry — first in pipeline)
    status_id = _get_default_status(cursor)
    if not status_id:
        logger.warning("MCube auto-create: no lead statuses configured")
        return None

    # Get a default project for the employee (first mapped project)
    project_id = None
    if emp_id:
        cursor.execute("""
            SELECT epm.project_id
            FROM employee_project_mapping epm
            WHERE epm.emp_id = %s AND epm.is_active = 1
            LIMIT 1
        """, (emp_id,))
        result = cursor.fetchone()
        project_id = result["project_id"] if result else None

    if not project_id:
        # Fall back to first active project
        cursor.execute("""
            SELECT project_id FROM project_registration
            ORDER BY project_id ASC LIMIT 1
        """)
        result = cursor.fetchone()
        project_id = result["project_id"] if result else None

    if not project_id:
        logger.warning("MCube auto-create: no projects configured")
        return None

    assigned_to = emp_id

    # If no employee matched the agent phone, skip auto-creation
    if not assigned_to:
        logger.warning("MCube auto-create: no employee to assign, skipping lead creation")
        return None

    lead_data = {
        "name": f"IVR Lead ({clean_phone})",
        "phone": clean_phone,
        "email": None,
        "source": source_id,
        "status": status_id,
        "assigned_to": assigned_to,
        "project": project_id,
        "description": "Auto-created from inbound IVR call"
    }

    lead_id = add_new_lead(lead_data, actor_id=assigned_to, role="ADMIN")
    db.commit()

    # Notify the assigned employee
    if assigned_to:
        try:
            create_notification(
                assigned_to,
                f"New IVR lead assigned: {clean_phone}",
                "lead",
                lead_id
            )
        except Exception as e:
            logger.warning(f"MCube auto-create: notification failed: {e}")

    logger.info(f"MCube auto-created lead: {lead_id}, phone={clean_phone}, assigned_to={assigned_to}")
    return lead_id


def process_mcube_call(data):
    """
    Process an incoming MCube webhook call record.
    Expected fields from MCube:
    - caller: customer phone number
    - agent: agent phone/extension
    - duration: call duration in seconds
    - status: call disposition (ANSWERED, NO ANSWER, BUSY, etc.)
    - recording_url: URL to call recording
    - call_type: inbound/outbound
    - callid: MCube's unique call ID
    """
    caller_phone = data.get("caller") or data.get("caller_number") or data.get("from")
    agent_phone = data.get("agent") or data.get("agent_number") or data.get("to")
    duration = data.get("duration") or data.get("call_duration") or 0
    mcube_status = data.get("status") or data.get("call_status") or ""
    recording_url = data.get("recording_url") or data.get("recording") or None
    call_type = data.get("call_type") or data.get("type") or "MCube"
    mcube_call_id = data.get("callid") or data.get("call_id") or None

    try:
        duration = int(duration)
    except (ValueError, TypeError):
        duration = 0

    crm_status = map_mcube_status(mcube_status)

    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        lead_id = _match_lead_by_phone(cursor, caller_phone)
        emp_id = _match_employee_by_phone(cursor, agent_phone)

        if not emp_id:
            logger.warning(f"MCube webhook: no employee found for phone {agent_phone}")

        # Auto-create lead for inbound calls from unknown numbers
        auto_created = False
        if not lead_id:
            is_inbound = call_type and call_type.lower() in ["inbound", "incoming"]
            if is_inbound:
                lead_id = _auto_create_lead_from_call(cursor, db, caller_phone, emp_id)
                if lead_id:
                    auto_created = True
            else:
                logger.warning(f"MCube webhook: no lead found for phone {caller_phone}")

        # Determine call source label
        source_label = "MCube"
        if call_type and call_type.lower() in ["inbound", "incoming"]:
            source_label = "MCube-Inbound"
        elif call_type and call_type.lower() in ["outbound", "outgoing"]:
            source_label = "MCube-Outbound"

        cursor.execute("""
            INSERT INTO call_log
            (lead_id, emp_id, call_time, call_duration, call_status, call_source, recording_url)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s)
        """, (
            lead_id,
            emp_id,
            duration,
            crm_status,
            source_label,
            recording_url
        ))

        db.commit()
        call_id = cursor.lastrowid

        logger.info(
            f"MCube call logged: call_id={call_id}, lead={lead_id}, "
            f"emp={emp_id}, status={crm_status}, duration={duration}s"
            f"{', auto_created=True' if auto_created else ''}"
        )

        return {
            "call_id": call_id,
            "lead_id": lead_id,
            "emp_id": emp_id,
            "status": crm_status,
            "matched": bool(lead_id),
            "auto_created": auto_created
        }

    except Exception as e:
        db.rollback()
        logger.error(f"MCube webhook processing error: {e}")
        raise

    finally:
        cursor.close()
        db.close()


def initiate_click2call(agent_phone, customer_phone):
    """
    Initiate a Click2Call via MCube API.
    MCube connects the agent first, then bridges to the customer.
    """
    api_key = os.getenv("MCUBE_API_KEY")
    click2call_url = os.getenv("MCUBE_CLICK2CALL_URL")

    if not api_key or not click2call_url:
        raise Exception("MCube Click2Call not configured. Set MCUBE_API_KEY and MCUBE_CLICK2CALL_URL in .env")

    payload = {
        "api_key": api_key,
        "agent": agent_phone,
        "destination": customer_phone,
    }

    try:
        response = requests.post(click2call_url, json=payload, timeout=10)
        response.raise_for_status()

        result = response.json()
        logger.info(f"Click2Call initiated: agent={agent_phone}, customer={customer_phone}")
        return result

    except requests.RequestException as e:
        logger.error(f"Click2Call API error: {e}")
        raise Exception(f"Failed to initiate call: {str(e)}")
