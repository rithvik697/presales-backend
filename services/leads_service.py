from db import get_db
import mysql.connector
from datetime import datetime

# ---------------------------------------------------------
# INTERNAL HELPERS (formerly in leads_data.py)
# ---------------------------------------------------------

def _generate_id(cursor, table, id_col, prefix):
    """Generates a sequential ID (e.g., L001, L002)."""
    pattern = f"^{prefix}[0-9]+$"
    query = f"SELECT {id_col} FROM {table} WHERE {id_col} REGEXP %s ORDER BY LENGTH({id_col}) DESC, {id_col} DESC LIMIT 1"
    cursor.execute(query, (pattern,))
    result = cursor.fetchone()
    
    if result:
        last_id = result[0]
        try:
            number_part = int(last_id[len(prefix):])
            new_number = number_part + 1
        except ValueError:
            new_number = 1
    else:
        new_number = 1
        
    return f"{prefix}{str(new_number).zfill(3)}"

def _get_id_by_name(cursor, table, name_col, id_col, name_val):
    """Helper to lookup ID by Name."""
    if not name_val: return None
    query = f"SELECT {id_col} FROM {table} WHERE {name_col} = %s LIMIT 1"
    cursor.execute(query, (name_val,))
    res = cursor.fetchone()
    return res[0] if res else None

def _get_employee_id_by_name(cursor, full_name):
    """Helper to find employee ID by full name."""
    if not full_name: return None
    cursor.execute("SELECT emp_id FROM employee WHERE emp_first_name = %s", (full_name,))
    res = cursor.fetchone()
    if res: return res[0]
    
    query = "SELECT emp_id FROM employee WHERE TRIM(CONCAT(emp_first_name, ' ', IFNULL(emp_last_name, ''))) = %s"
    cursor.execute(query, (full_name,))
    res = cursor.fetchone()
    return res[0] if res else None

def _get_employee_name_by_id(cursor, emp_id):
    """Helper to find employee full name by ID."""
    if not emp_id: return None
    query = "SELECT TRIM(CONCAT(emp_first_name, ' ', IFNULL(emp_last_name, ''))) FROM employee WHERE emp_id = %s"
    cursor.execute(query, (emp_id,))
    res = cursor.fetchone()
    return res[0] if res else None

def _get_or_create_customer(cursor, data, actor_name=None):
    """Helper to find or create a customer by phone number."""
    phone = data.get('phone')
    if not phone: return None
        
    clean_phone = phone.replace(' ', '').replace('-', '').replace('+', '')
    cursor.execute("SELECT customer_id FROM customer WHERE phone_num = %s OR REPLACE(REPLACE(REPLACE(phone_num, ' ', ''), '-', ''), '+', '') = %s", (phone, clean_phone))
    res = cursor.fetchone()
    if res: return res[0]

    customer_id = _generate_id(cursor, 'customer', 'customer_id', 'CUST')
    full_name = data.get('name', '').split(' ')
    first_name = full_name[0]
    last_name = " ".join(full_name[1:]) if len(full_name) > 1 else ""

    query = """
    INSERT INTO customer (customer_id, customer_first_name, customer_last_name, phone_num, alt_num, email, profession, created_on, created_by, modified_on, modified_by, is_active)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, NULL, NULL, 1)
    """
    cursor.execute(query, (customer_id, first_name, last_name, phone, data.get('alternate_phone'), data.get('email'), data.get('profession'), actor_name))
    return customer_id

# ---------------------------------------------------------
# SERVICE FUNCTIONS (Public API)
# ---------------------------------------------------------

def fetch_all_leads(filters=None):
    """Fetches leads with JOINs on customer, employee, source, status."""
    conn = get_db()
    leads = []
    if not conn: return []

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT 
            l.lead_id as id,
            TRIM(CONCAT(c.customer_first_name, ' ', IFNULL(c.customer_last_name, ''))) as name,
            c.phone_num as phone,
            c.alt_num as alternatePhone,
            c.email as email,
            c.profession as profession,
            ls.source_name as source,
            lst.status_name as status,
            TRIM(CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, ''))) as assignedTo,
            pr.project_name as project,
            l.project_id as projectId,
            l.lead_description as description,
            l.created_on as createdAt,
            l.modified_on as modifiedAt,
            ec.emp_first_name as createdBy,
            em.emp_first_name as modifiedBy
        FROM leads l
        LEFT JOIN customer c ON l.customer_id = c.customer_id
        LEFT JOIN lead_sources ls ON l.source_id = ls.source_id
        LEFT JOIN lead_status lst ON l.status_id = lst.status_id
        LEFT JOIN employee e ON l.emp_id = e.emp_id
        LEFT JOIN employee ec ON l.created_by = ec.emp_id
        LEFT JOIN employee em ON l.modified_by = em.emp_id
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
        result = cursor.fetchall()
        
        for row in result:
             leads.append(row)
        
    except Exception as e:
        print(f"Error fetching leads: {e}")
    finally:
        conn.close()
    return leads

def fetch_lead_by_id(lead_id):
    """Fetches a single lead by ID."""
    conn = get_db()
    if not conn: return None

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT 
            l.lead_id as id,
            c.customer_first_name,
            c.customer_last_name,
            c.phone_num as phone,
            c.alt_num as alternatePhone,
            c.email,
            c.profession, 
            ls.source_name as source,
            lst.status_name as status,
            TRIM(CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, ''))) as assignedTo,
            pr.project_name as project,
            l.project_id as projectId,
            l.lead_description as description,
            l.created_on as createdAt,
            l.modified_on as modifiedAt,
            ec.emp_first_name as createdBy,
            em.emp_first_name as modifiedBy
        FROM leads l
        LEFT JOIN customer c ON l.customer_id = c.customer_id
        LEFT JOIN lead_sources ls ON l.source_id = ls.source_id
        LEFT JOIN lead_status lst ON l.status_id = lst.status_id
        LEFT JOIN employee e ON l.emp_id = e.emp_id
        LEFT JOIN employee ec ON l.created_by = ec.emp_id
        LEFT JOIN employee em ON l.modified_by = em.emp_id
        LEFT JOIN project_registration pr ON l.project_id = pr.project_id
        WHERE l.lead_id = %s AND l.is_active = 1
        """
        cursor.execute(query, (lead_id,))
        result = cursor.fetchone()
        
        if result:
            result['name'] = f"{result['customer_first_name']} {result['customer_last_name'] or ''}".strip()
            del result['customer_first_name']
            del result['customer_last_name']
            return result
    except Exception as e:
        print(f"Error fetching lead {lead_id}: {e}")
    finally:
        conn.close()
    return None


def add_new_lead(data, actor_name=None):
    """
    Creates a new lead.
    Expects IDs for:
      - source
      - status
      - assigned_to
      - project
    """

    if not data.get('name') or not data.get('phone'):
        raise ValueError("Name and Phone are required fields.")

    if not actor_name:
        raise ValueError("Unauthorized: actor not identified from token.")

    conn = get_db()
    if not conn:
        raise Exception("DB Connection failed")

    try:
        cursor = conn.cursor()

        # Create or fetch customer
        customer_id = _get_or_create_customer(cursor, data, actor_name)

        # 🔥 Directly use IDs sent from frontend
        source_id = data.get('source')
        status_id = data.get('status')
        emp_id = data.get('assigned_to')
        project_id = data.get('project')
        description = data.get('description', '')

        # Optional validation (recommended)
        if not source_id:
            raise ValueError("Source is required.")
        if not status_id:
            raise ValueError("Status is required.")
        if not emp_id:
            raise ValueError("Assigned employee is required.")

        new_lead_id = _generate_id(cursor, 'leads', 'lead_id', 'L')

        query = """
        INSERT INTO leads
        (lead_id, customer_id, source_id, status_id, emp_id,
         project_id, lead_description,
         created_on, created_by, modified_on, modified_by, is_active)
        VALUES (%s, %s, %s, %s, %s,
                %s, %s,
                NOW(), %s, NULL, NULL, 1)
        """

        cursor.execute(query, (
            new_lead_id,
            customer_id,
            source_id,
            status_id,
            emp_id,
            project_id,
            description,
            actor_name
        ))

        conn.commit()
        return new_lead_id

    except Exception as e:
        conn.rollback()
        print("Error creating lead:", e)
        raise e

    finally:
        conn.close()


def update_existing_lead(lead_id, data, actor_name=None):
    """Updates an existing lead."""
    conn = get_db()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        source_id = _get_id_by_name(cursor, 'lead_sources', 'source_name', 'source_id', data.get('source'))
        status_id = _get_id_by_name(cursor, 'lead_status', 'status_name', 'status_id', data.get('status'))
        emp_id = _get_employee_id_by_name(cursor, data.get('assignedTo'))
        project_id = data.get('project')  # 🔥 Now directly update project_id

        description = data.get('description', '')

        query = """
        UPDATE leads 
        SET source_id = IFNULL(%s, source_id),
            status_id = IFNULL(%s, status_id),
            emp_id = IFNULL(%s, emp_id),
            project_id = IFNULL(%s, project_id),
            lead_description = %s,
            modified_on = NOW(),
            modified_by = %s
        WHERE lead_id = %s
        """

        cursor.execute(query, (
            source_id,
            status_id,
            emp_id,
            project_id,  # 🔥 Proper column update
            description,
            actor_name,
            lead_id
        ))

        # Update customer details if needed
        cursor.execute("SELECT customer_id FROM leads WHERE lead_id = %s", (lead_id,))
        res = cursor.fetchone()

        if res and (data.get('name') or data.get('email')):
            cust_id = res[0]
            names = data.get('name', '').split(' ')
            first = names[0]
            last = " ".join(names[1:]) if len(names) > 1 else ""

            cust_query = """
            UPDATE customer 
            SET customer_first_name = %s,
                customer_last_name = %s,
                email = %s,
                alt_num = %s,
                profession = %s,
                modified_on = NOW(),
                modified_by = %s
            WHERE customer_id = %s
            """

            cursor.execute(cust_query, (
                first,
                last,
                data.get('email'),
                data.get('alternate_phone'),
                data.get('profession'),
                actor_name,
                cust_id
            ))

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        print("Error updating lead:", e)
        return False

    finally:
        conn.close()

def delete_existing_lead(lead_id):
    """Deactivates a lead (Soft delete)."""
    conn = get_db()
    if not conn: return False
    try:
        cursor = conn.cursor()
        query = "UPDATE leads SET is_active = 0 WHERE lead_id = %s"
        cursor.execute(query, (lead_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        return False
    finally:
        conn.close()

def fetch_all_employees():
    """Fetches all employees with ID and full name."""
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT 
            emp_id,
            TRIM(CONCAT(emp_first_name, ' ', IFNULL(emp_last_name, ''))) AS full_name
        FROM employee
        WHERE emp_status = 'Active'
        ORDER BY emp_first_name
        """

        cursor.execute(query)
        return cursor.fetchall()

    except Exception as e:
        print("Error fetching employees:", e)
        return []

    finally:
        conn.close()


def fetch_all_sources():
    """Fetch all lead sources from DB."""
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT source_id, source_name
        FROM lead_sources
        ORDER BY source_name ASC
        """

        cursor.execute(query)
        return cursor.fetchall()

    except Exception as e:
        print("Error fetching lead sources:", e)
        return []

    finally:
        conn.close()

def fetch_all_statuses():
    """Fetch all lead statuses from DB."""
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT status_id, status_name
        FROM lead_status
        ORDER BY status_name ASC
        """

        cursor.execute(query)
        return cursor.fetchall()

    except Exception as e:
        print("Error fetching lead statuses:", e)
        return []

    finally:
        conn.close()
