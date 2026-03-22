import logging
from db import get_db
from services.leads_service import (
    _generate_id,
    _check_duplicate_phone,
    _get_or_create_customer,
    add_new_lead
)
from services.notification_service import create_notification

logger = logging.getLogger(__name__)


def _ensure_assignment_tracker_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lead_assignment_tracker (
            project_id VARCHAR(150) PRIMARY KEY,
            last_emp_id VARCHAR(150) NULL,
            updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            CONSTRAINT fk_lead_assignment_tracker_project
                FOREIGN KEY (project_id) REFERENCES project_registration(project_id),
            CONSTRAINT fk_lead_assignment_tracker_emp
                FOREIGN KEY (last_emp_id) REFERENCES employee(emp_id)
        )
    """)


def _find_source_by_name(cursor, source_name):
    """Find a lead source by name, or return a default."""
    if not source_name:
        return None

    cursor.execute("""
        SELECT source_id FROM lead_sources
        WHERE LOWER(source_name) = LOWER(%s) AND is_active = 1
        LIMIT 1
    """, (source_name,))
    result = cursor.fetchone()

    if result:
        return result["source_id"]

    # Try partial match
    cursor.execute("""
        SELECT source_id FROM lead_sources
        WHERE LOWER(source_name) LIKE LOWER(%s) AND is_active = 1
        LIMIT 1
    """, (f"%{source_name}%",))
    result = cursor.fetchone()

    return result["source_id"] if result else None


def _find_project_by_name(cursor, project_name):
    """Find a project by name."""
    if not project_name:
        return None

    cursor.execute("""
        SELECT project_id FROM project_registration
        WHERE LOWER(project_name) = LOWER(%s) AND project_status = 'Active'
        LIMIT 1
    """, (project_name,))
    result = cursor.fetchone()

    if result:
        return result["project_id"]

    # Try partial match
    cursor.execute("""
        SELECT project_id FROM project_registration
        WHERE LOWER(project_name) LIKE LOWER(%s) AND project_status = 'Active'
        LIMIT 1
    """, (f"%{project_name}%",))
    result = cursor.fetchone()

    return result["project_id"] if result else None


def _get_default_status(cursor):
    """Get the default (first pipeline) status for new leads."""
    cursor.execute("""
        SELECT status_id FROM lead_status
        WHERE is_active = 1
        ORDER BY pipeline_order ASC
        LIMIT 1
    """)
    result = cursor.fetchone()
    return result["status_id"] if result else None


def _auto_assign_employee(cursor, project_id=None):
    """
    Auto-assign a sales exec project-wise in round robin alphabetical order.
    Only active SALES_EXEC users mapped to the selected project are eligible.
    """
    if not project_id:
        raise ValueError("Project is required for automatic lead assignment")

    _ensure_assignment_tracker_table(cursor)

    cursor.execute("""
        SELECT
            e.emp_id,
            TRIM(CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, ''))) AS full_name
        FROM employee_project_mapping epm
        JOIN employee e ON epm.emp_id = e.emp_id
        WHERE epm.project_id = %s
          AND epm.is_active = 1
          AND e.emp_status = 'Active'
          AND e.role_id = 'SALES_EXEC'
        ORDER BY e.emp_first_name ASC, e.emp_last_name ASC, e.emp_id ASC
    """, (project_id,))
    eligible_employees = cursor.fetchall()

    if not eligible_employees:
        raise ValueError("No active sales executives are mapped to this project")

    cursor.execute("""
        SELECT last_emp_id
        FROM lead_assignment_tracker
        WHERE project_id = %s
    """, (project_id,))
    tracker = cursor.fetchone()
    last_emp_id = tracker["last_emp_id"] if tracker else None

    employee_ids = [employee["emp_id"] for employee in eligible_employees]

    if last_emp_id in employee_ids:
        last_index = employee_ids.index(last_emp_id)
        next_index = (last_index + 1) % len(employee_ids)
    else:
        next_index = 0

    next_emp_id = employee_ids[next_index]
    return next_emp_id


def _update_assignment_tracker(cursor, project_id, emp_id):
    _ensure_assignment_tracker_table(cursor)
    cursor.execute("""
        INSERT INTO lead_assignment_tracker (project_id, last_emp_id)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            last_emp_id = VALUES(last_emp_id),
            updated_on = CURRENT_TIMESTAMP
    """, (project_id, emp_id))


def process_webhook_lead(data):
    """
    Process an incoming lead from Make.com webhook.

    Expected payload:
    {
        "source": "Facebook|Google|Website|99acres|Housing",
        "first_name": "...",
        "last_name": "...",
        "email": "...",
        "phone": "...",
        "project_name": "...",
        "remarks": "...",
        "raw_data": {}
    }
    """
    phone = data.get("phone")
    if not phone:
        raise ValueError("Phone number is required")

    first_name = data.get("first_name", "").strip()
    last_name = data.get("last_name", "").strip()
    name = f"{first_name} {last_name}".strip() or "Unknown Lead"
    email = data.get("email", "").strip() or None
    source_name = data.get("source", "").strip()
    project_name = data.get("project_name", "").strip()
    remarks = data.get("remarks", "").strip()

    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        # Check for duplicate phone
        try:
            _check_duplicate_phone(cursor, phone)
        except ValueError as e:
            logger.info(f"Webhook duplicate lead: {e}")
            return {
                "status": "duplicate",
                "message": str(e)
            }

        # Match source
        source_id = _find_source_by_name(cursor, source_name)
        if not source_id:
            # Get first available source as fallback
            cursor.execute("""
                SELECT source_id FROM lead_sources
                WHERE is_active = 1
                ORDER BY source_id ASC
                LIMIT 1
            """)
            result = cursor.fetchone()
            source_id = result["source_id"] if result else None

        if not source_id:
            raise ValueError("No lead sources configured in the system")

        # Match project
        project_id = _find_project_by_name(cursor, project_name)
        if not project_id:
            raise ValueError("Could not match project for automatic assignment")

        # Get default status
        status_id = _get_default_status(cursor)
        if not status_id:
            raise ValueError("No lead statuses configured in the system")

        # Auto-assign employee
        emp_id = _auto_assign_employee(cursor, project_id)

        # Build lead data for existing add_new_lead function
        lead_data = {
            "name": name,
            "phone": phone,
            "email": email,
            "source": source_id,
            "status": status_id,
            "assigned_to": emp_id,
            "project": project_id,
            "description": remarks or f"Auto-created from {source_name or 'webhook'}"
        }

        # Use the existing lead creation (which handles customer, audit, notifications)
        # We pass actor_id as the assigned emp and role as ADMIN to bypass visibility checks
        lead_id = add_new_lead(lead_data, actor_id=emp_id, role="ADMIN")
        _update_assignment_tracker(cursor, project_id, emp_id)
        db.commit()

        logger.info(
            f"Webhook lead created: {lead_id}, source={source_name}, "
            f"assigned_to={emp_id}, project={project_id}"
        )

        return {
            "status": "created",
            "lead_id": lead_id,
            "assigned_to": emp_id,
            "source_id": source_id,
            "project_id": project_id
        }

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"Webhook lead processing error: {e}")
        raise

    finally:
        cursor.close()
        db.close()
