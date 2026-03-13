from services.lead_status_history_service import create_history
from db import get_db
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------

def _generate_id(cursor, table, id_col, prefix):
    """
    Production-safe sequential ID generator.
    Generates IDs like:
    L001, L002, L003 ... L100000+
    """

    query = f"""
        SELECT MAX(CAST(SUBSTRING({id_col}, {len(prefix)+1}) AS UNSIGNED))
        FROM {table}
        WHERE {id_col} LIKE %s
    """

    cursor.execute(query, (f"{prefix}%",))
    result = cursor.fetchone()

    last_number = result[0] if result and result[0] else 0
    new_number = last_number + 1

    return f"{prefix}{str(new_number).zfill(3)}"


def _validate_foreign_key(cursor, table, id_col, id_val, label):
    """
    Validates that a given ID exists in the referenced table.
    Raises ValueError with a clear message if not found.
    """
    if not id_val:
        return  # Allow None for optional fields

    cursor.execute(f"SELECT 1 FROM {table} WHERE {id_col} = %s LIMIT 1", (id_val,))
    if cursor.fetchone() is None:
        raise ValueError(f"Invalid {label}: '{id_val}' does not exist.")


def _check_duplicate_phone(cursor, phone, exclude_lead_id=None):
    """
    Checks if a lead already exists with this phone number.
    Client requirement: prevent duplicate leads for the same customer.
    """
    if not phone:
        return

    clean_phone = phone.replace(' ', '').replace('-', '').replace('+', '')

    query = """
        SELECT l.lead_id, c.phone_num
        FROM leads l
        JOIN customer c ON l.customer_id = c.customer_id
        WHERE l.is_active = 1
          AND (
            c.phone_num = %s
            OR REPLACE(REPLACE(REPLACE(c.phone_num, ' ', ''), '-', ''), '+', '') = %s
          )
    """
    params = [phone, clean_phone]

    if exclude_lead_id:
        query += " AND l.lead_id != %s"
        params.append(exclude_lead_id)

    cursor.execute(query, tuple(params))
    existing = cursor.fetchone()

    if existing:
        raise ValueError(
            f"A lead with phone number '{phone}' already exists (Lead ID: {existing[0]}). "
            f"Use the existing lead instead of creating a duplicate."
        )


def _get_or_create_customer(cursor, data, actor_id=None):
    """Find or create a customer by phone number."""
    phone = data.get('phone')
    if not phone:
        return None

    clean_phone = phone.replace(' ', '').replace('-', '').replace('+', '')

    cursor.execute(
        """SELECT customer_id FROM customer
           WHERE phone_num = %s
              OR REPLACE(REPLACE(REPLACE(phone_num, ' ', ''), '-', ''), '+', '') = %s""",
        (phone, clean_phone)
    )
    res = cursor.fetchone()
    if res:
        return res[0]

    customer_id = _generate_id(cursor, 'customer', 'customer_id', 'CUST')
    full_name = data.get('name', '').split(' ')
    first_name = full_name[0]
    last_name = " ".join(full_name[1:]) if len(full_name) > 1 else ""

    cursor.execute(
        """INSERT INTO customer
           (customer_id, customer_first_name, customer_last_name,
            phone_num, alt_num, email, profession,
            created_on, created_by, modified_on, modified_by, is_active)
           VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, NULL, NULL, 1)""",
        (customer_id, first_name, last_name, clean_phone,
         ''.join(filter(str.isdigit, data.get('alternate_phone', ''))) if data.get('alternate_phone') else None, data.get('email'),
         data.get('profession'), actor_id)
    )
    return customer_id


# ---------------------------------------------------------
# SERVICE FUNCTIONS (Public API)
# ---------------------------------------------------------

def fetch_all_leads(filters=None):
    """
    Fetches leads with JOINs.
    Returns BOTH IDs and names for foreign keys so the frontend
    can populate dropdowns correctly in edit mode.
    """
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT
            l.lead_id                                                       AS id,
            TRIM(CONCAT(c.customer_first_name, ' ',
                 IFNULL(c.customer_last_name, '')))                         AS name,
            c.phone_num                                                     AS phone,
            c.alt_num                                                       AS alternatePhone,
            c.email                                                         AS email,
            c.profession                                                    AS profession,

            /* Source — ID + name */
            l.source_id                                                     AS sourceId,
            ls.source_name                                                  AS source,

            /* Status — ID + name */
            l.status_id                                                     AS statusId,
            lst.status_name                                                 AS status,

            /* Assigned employee — ID + name */
            l.emp_id                                                        AS assignedToId,
            TRIM(CONCAT(e.emp_first_name, ' ',
                 IFNULL(e.emp_last_name, '')))                              AS assignedTo,

            /* Project — ID + name */
            l.project_id                                                    AS projectId,
            pr.project_name                                                 AS project,

            l.lead_description                                              AS description,
            l.created_on                                                    AS createdAt,
            l.modified_on                                                   AS modifiedAt,
            ec.emp_first_name                                               AS createdBy,
            em.emp_first_name                                               AS modifiedBy
        FROM leads l
        LEFT JOIN customer c          ON l.customer_id = c.customer_id
        LEFT JOIN lead_sources ls     ON l.source_id   = ls.source_id
        LEFT JOIN lead_status lst     ON l.status_id   = lst.status_id
        LEFT JOIN employee e          ON l.emp_id      = e.emp_id
        LEFT JOIN employee ec         ON l.created_by  = ec.emp_id
        LEFT JOIN employee em         ON l.modified_by = em.emp_id
        LEFT JOIN project_registration pr ON l.project_id = pr.project_id
        WHERE l.is_active = 1
        """

        params = []
        if filters:
            if filters.get('customer'):
                query += " AND (c.customer_first_name LIKE %s OR c.customer_last_name LIKE %s)"
                term = f"%{filters['customer']}%"
                params.extend([term, term])
            if filters.get('mobile'):
                query += " AND c.phone_num LIKE %s"
                params.append(f"%{filters['mobile']}%")
            if filters.get('source'):
                query += " AND ls.source_name = %s"
                params.append(filters['source'])
            if filters.get('employee'):
                query += " AND e.emp_first_name = %s"
                params.append(filters['employee'])
            if filters.get('project'):
                query += " AND pr.project_name = %s"
                params.append(filters['project'])

        query += " ORDER BY l.created_on DESC"
        cursor.execute(query, tuple(params))
        return cursor.fetchall()

    except Exception as e:
        logger.error(f"Error fetching leads: {e}")
        return []
    finally:
        conn.close()


def fetch_lead_by_id(lead_id):
    """
    Fetches a single lead by ID.
    Returns BOTH IDs and names for all foreign keys.
    """
    conn = get_db()
    if not conn:
        return None

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT
            l.lead_id                                                       AS id,
            c.customer_first_name,
            c.customer_last_name,
            c.phone_num                                                     AS phone,
            c.alt_num                                                       AS alternatePhone,
            c.email,
            c.profession,

            l.source_id                                                     AS sourceId,
            ls.source_name                                                  AS source,

            l.status_id                                                     AS statusId,
            lst.status_name                                                 AS status,

            l.emp_id                                                        AS assignedToId,
            TRIM(CONCAT(e.emp_first_name, ' ',
                 IFNULL(e.emp_last_name, '')))                              AS assignedTo,

            l.project_id                                                    AS projectId,
            pr.project_name                                                 AS project,

            l.lead_description                                              AS description,
            l.created_on                                                    AS createdAt,
            l.modified_on                                                   AS modifiedAt,
            ec.emp_first_name                                               AS createdBy,
            em.emp_first_name                                               AS modifiedBy
        FROM leads l
        LEFT JOIN customer c          ON l.customer_id = c.customer_id
        LEFT JOIN lead_sources ls     ON l.source_id   = ls.source_id
        LEFT JOIN lead_status lst     ON l.status_id   = lst.status_id
        LEFT JOIN employee e          ON l.emp_id      = e.emp_id
        LEFT JOIN employee ec         ON l.created_by  = ec.emp_id
        LEFT JOIN employee em         ON l.modified_by = em.emp_id
        LEFT JOIN project_registration pr ON l.project_id = pr.project_id
        WHERE l.lead_id = %s AND l.is_active = 1
        """
        cursor.execute(query, (lead_id,))
        result = cursor.fetchone()

        if result:
            result['name'] = (
                f"{result['customer_first_name']} "
                f"{result['customer_last_name'] or ''}"
            ).strip()
            del result['customer_first_name']
            del result['customer_last_name']
            return result

    except Exception as e:
        logger.error(f"Error fetching lead {lead_id}: {e}")
    finally:
        conn.close()
    return None


def add_new_lead(data, actor_id=None):
    """
    Creates a new lead.
    Expects IDs for: source, status, assigned_to, project.
    Validates all IDs exist before inserting.
    Checks for duplicate phone numbers.
    """
    if not data.get('name') or not data.get('phone'):
        raise ValueError("Name and Phone are required fields.")

    if not actor_id:
        raise ValueError("Unauthorized: actor not identified from token.")

    conn = get_db()
    if not conn:
        raise Exception("DB connection failed")

    try:
        cursor = conn.cursor()

        # --- Extract IDs from request ---
        source_id   = data.get('source')
        status_id   = data.get('status')
        emp_id      = data.get('assigned_to')
        project_id  = data.get('project')
        description = data.get('description', '')

        # --- Validate required fields ---
        if not source_id:
            raise ValueError("Source is required.")
        if not status_id:
            raise ValueError("Status is required.")
        if not emp_id:
            raise ValueError("Assigned employee is required.")

        # --- Validate all foreign keys exist in DB ---
        _validate_foreign_key(cursor, 'lead_sources', 'source_id', source_id, 'source')
        _validate_foreign_key(cursor, 'lead_status', 'status_id', status_id, 'status')
        _validate_foreign_key(cursor, 'employee', 'emp_id', emp_id, 'employee')
        if project_id:
            _validate_foreign_key(cursor, 'project_registration', 'project_id', project_id, 'project')

        # --- Check for duplicate phone (client requirement) ---
        _check_duplicate_phone(cursor, data.get('phone'))

        # --- Create or find customer ---
        customer_id = _get_or_create_customer(cursor, data, actor_id)

        # --- Generate lead ID and insert ---
        new_lead_id = _generate_id(cursor, 'leads', 'lead_id', 'L')

        cursor.execute(
            """INSERT INTO leads
               (lead_id, customer_id, source_id, status_id, emp_id,
                project_id, lead_description,
                created_on, created_by, modified_on, modified_by, is_active)
               VALUES (%s, %s, %s, %s, %s, %s, %s,
                       NOW(), %s, NULL, NULL, 1)""",
            (new_lead_id, customer_id, source_id, status_id, emp_id,
             project_id, description, actor_id)
        )

        initial_history = {
            "new_status_id": status_id,
            "remarks": "Lead created"
        }


        conn.commit()
        logger.info(f"Lead {new_lead_id} created by {actor_id}")
        create_history(new_lead_id, initial_history, actor_id)
        return new_lead_id

    except ValueError:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating lead: {e}")
        raise
    finally:
        conn.close()


def update_existing_lead(lead_id, data, actor_id=None):
    """
    Updates an existing lead.
    NOW ACCEPTS IDs (not names) — consistent with create.
    Validates all foreign keys before updating.
    """
    conn = get_db()
    if not conn:
        return False

    try:
        cursor = conn.cursor(dictionary=True)

        # --- Extract IDs from request ---
        source_id   = data.get('source')       # Now expects source_id (e.g., 'SRC001')
        status_id   = data.get('status')       # Now expects status_id (e.g., 'ST001')
        emp_id      = data.get('assigned_to')  # Now expects emp_id   (e.g., 'EMP003')
        project_id  = data.get('project')      # Already expects project_id
        description = data.get('description', '')

        # --- Validate foreign keys if provided ---
        if source_id:
            _validate_foreign_key(cursor, 'lead_sources', 'source_id', source_id, 'source')
        if status_id:
            _validate_foreign_key(cursor, 'lead_status', 'status_id', status_id, 'status')
        if emp_id:
            _validate_foreign_key(cursor, 'employee', 'emp_id', emp_id, 'employee')
        if project_id:
            _validate_foreign_key(cursor, 'project_registration', 'project_id', project_id, 'project')

        # --- Update the lead ---
        cursor.execute(
            """UPDATE leads
               SET source_id       = IFNULL(%s, source_id),
                   status_id       = IFNULL(%s, status_id),
                   emp_id          = IFNULL(%s, emp_id),
                   project_id      = IFNULL(%s, project_id),
                   lead_description = %s,
                   modified_on     = NOW(),
                   modified_by     = %s
               WHERE lead_id = %s AND is_active = 1""",
            (source_id, status_id, emp_id, project_id,
             description, actor_id, lead_id)
        )

        # --- Update customer details if provided ---
        cursor.execute(
            "SELECT customer_id FROM leads WHERE lead_id = %s",
            (lead_id,)
        )
        res = cursor.fetchone()

        if res and (data.get('name') or data.get('email')):
            cust_id = res['customer_id']
            names = data.get('name', '').split(' ')
            first = names[0] if names else ''
            last = " ".join(names[1:]) if len(names) > 1 else ""

            cursor.execute(
                """UPDATE customer
                   SET customer_first_name = %s,
                       customer_last_name  = %s,
                       email               = %s,
                       alt_num             = %s,
                       profession          = %s,
                       modified_on         = NOW(),
                       modified_by         = %s
                   WHERE customer_id = %s""",
                (first, last, data.get('email'),
                 ''.join(filter(str.isdigit, data.get('alternate_phone', ''))) if data.get('alternate_phone') else None, data.get('profession'),
                 actor_id, cust_id)
            )

        conn.commit()
        logger.info(f"Lead {lead_id} updated by {actor_id}")
        return True

    except ValueError:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating lead {lead_id}: {e}")
        return False
    finally:
        conn.close()


def delete_existing_lead(lead_id):
    """Soft-deletes a lead."""
    conn = get_db()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE leads SET is_active = 0 WHERE lead_id = %s",
            (lead_id,)
        )
        conn.commit()
        deleted = cursor.rowcount > 0
        if deleted:
            logger.info(f"Lead {lead_id} soft-deleted")
        return deleted
    except Exception as e:
        logger.error(f"Error deleting lead {lead_id}: {e}")
        return False
    finally:
        conn.close()


def fetch_all_employees():
    """Fetches all active employees (ID + full name)."""
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT
                emp_id,
                TRIM(CONCAT(emp_first_name, ' ',
                     IFNULL(emp_last_name, ''))) AS full_name
            FROM employee
            WHERE emp_status = 'Active'
            ORDER BY emp_first_name
        """)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error fetching employees: {e}")
        return []
    finally:
        conn.close()


def fetch_all_sources():
    """Fetch all lead sources."""
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT source_id, source_name
            FROM lead_sources
            ORDER BY source_name ASC
        """)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error fetching lead sources: {e}")
        return []
    finally:
        conn.close()


def fetch_all_statuses():
    """Fetch all lead statuses."""
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT status_id, status_name
            FROM lead_status
            ORDER BY pipeline_order ASC
        """)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error fetching lead statuses: {e}")
        return []
    finally:
        conn.close()