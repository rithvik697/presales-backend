from db import get_db_connection
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

def _get_or_create_customer(cursor, data):
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
    INSERT INTO customer (customer_id, customer_first_name, customer_last_name, phone_num, alt_num, email, profession, created_on, is_active)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), 1)
    """
    cursor.execute(query, (customer_id, first_name, last_name, phone, data.get('alternate_phone'), data.get('email'), data.get('profession')))
    return customer_id

# ---------------------------------------------------------
# SERVICE FUNCTIONS (Public API)
# ---------------------------------------------------------

def fetch_all_leads(filters=None):
    """Fetches leads with JOINs on customer, employee, source, status."""
    conn = get_db_connection()
    leads = []
    if not conn: return []

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT 
            l.lead_id as id,
            CONCAT(c.customer_first_name, ' ', IFNULL(c.customer_last_name, '')) as name,
            c.phone_num as phone,
            c.alt_num as alternatePhone,
            c.email as email,
            c.profession as profession,
            ls.source_name as source,
            lst.status_name as status,
            e.emp_first_name as assigned_to,
            l.lead_description as description,
            l.created_on as createdAt,
            l.modified_on as modifiedAt,
            l.created_by as createdById,
            l.modified_by as modifiedById,
            ec.emp_first_name as createdByName,
            em.emp_first_name as modifiedByName
        FROM leads l
        LEFT JOIN customer c ON l.customer_id = c.customer_id
        LEFT JOIN lead_sources ls ON l.source_id = ls.source_id
        LEFT JOIN lead_status lst ON l.status_id = lst.status_id
        LEFT JOIN employee e ON l.emp_id = e.emp_id
        LEFT JOIN employee ec ON l.created_by = ec.emp_id
        LEFT JOIN employee em ON l.modified_by = em.emp_id
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
                query += " AND l.lead_description LIKE %s"
                params.append(f"Project: {filters['project']}%")
        
        query += " ORDER BY l.created_on DESC"
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        for row in result:
             row['created_by'] = row['createdByName'] or row['createdById']
             row['modified_by'] = row['modifiedByName'] or row['modifiedById']
             leads.append(row)
        
    except Exception as e:
        print(f"Error fetching leads: {e}")
    finally:
        conn.close()
    return leads

def fetch_lead_by_id(lead_id):
    """Fetches a single lead by ID."""
    conn = get_db_connection()
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
            e.emp_first_name as assigned_to,
            l.lead_description as description,
            l.created_on as createdAt,
            l.modified_on as modifiedAt,
            ec.emp_first_name as createdByName,
            ec.emp_id as createdById,
            em.emp_first_name as modifiedByName,
            em.emp_id as modifiedById
        FROM leads l
        LEFT JOIN customer c ON l.customer_id = c.customer_id
        LEFT JOIN lead_sources ls ON l.source_id = ls.source_id
        LEFT JOIN lead_status lst ON l.status_id = lst.status_id
        LEFT JOIN employee e ON l.emp_id = e.emp_id
        LEFT JOIN employee ec ON l.created_by = ec.emp_id
        LEFT JOIN employee em ON l.modified_by = em.emp_id
        WHERE l.lead_id = %s AND l.is_active = 1
        """
        cursor.execute(query, (lead_id,))
        result = cursor.fetchone()
        
        if result:
            result['name'] = f"{result['customer_first_name']} {result['customer_last_name'] or ''}".strip()
            del result['customer_first_name']
            del result['customer_last_name']
            result['created_by'] = result['createdByName'] or result['createdById']
            result['modified_by'] = result['modifiedByName'] or result['modifiedById']
            return result
    except Exception as e:
        print(f"Error fetching lead {lead_id}: {e}")
    finally:
        conn.close()
    return None

def add_new_lead(data):
    """Adds a new lead and returns its generated ID."""
    if not data.get('name') or not data.get('phone'):
        raise ValueError("Name and Phone are required fields.")

    conn = get_db_connection()
    if not conn: raise Exception("DB Connection failed")

    try:
        cursor = conn.cursor()
        customer_id = _get_or_create_customer(cursor, data)
        if not customer_id: raise ValueError("Invalid Customer Data")

        source_id = _get_id_by_name(cursor, 'lead_sources', 'source_name', 'source_id', data.get('source')) or 'S001'
        status_id = _get_id_by_name(cursor, 'lead_status', 'status_name', 'status_id', data.get('status')) or 'ST001'
        emp_id = _get_employee_id_by_name(cursor, data.get('assigned_to')) or 'EMP001'
        
        description = data.get('description', '')
        project = data.get('project')
        if project:
            description = f"Project: {project} | {description}" if description else f"Project: {project}"

        new_lead_id = _generate_id(cursor, 'leads', 'lead_id', 'L')
        query = """
        INSERT INTO leads (lead_id, customer_id, source_id, status_id, emp_id, lead_description, created_on, created_by, modified_on, modified_by, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, NOW(), %s, 1)
        """
        admin_id = 'EMP001'
        cursor.execute(query, (new_lead_id, customer_id, source_id, status_id, emp_id, description, admin_id, admin_id))
        conn.commit()
        return new_lead_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def update_existing_lead(lead_id, data):
    """Updates an existing lead."""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        cursor = conn.cursor()
        source_id = _get_id_by_name(cursor, 'lead_sources', 'source_name', 'source_id', data.get('source'))
        status_id = _get_id_by_name(cursor, 'lead_status', 'status_name', 'status_id', data.get('status'))
        emp_id = _get_employee_id_by_name(cursor, data.get('assigned_to'))
        
        description = data.get('description', '')
        project = data.get('project')
        if project:
            description = f"Project: {project} | {description}" if description else f"Project: {project}"
                
        query = """
        UPDATE leads 
        SET source_id = IFNULL(%s, source_id),
            status_id = IFNULL(%s, status_id),
            emp_id = IFNULL(%s, emp_id),
            lead_description = %s,
            modified_on = %s,
            modified_by = %s
        WHERE lead_id = %s
        """
        modifier_id = 'EMP001' 
        cursor.execute(query, (source_id, status_id, emp_id, description, datetime.utcnow(), modifier_id, lead_id))

        cursor.execute("SELECT customer_id FROM leads WHERE lead_id = %s", (lead_id,))
        res = cursor.fetchone()
        if res and (data.get('name') or data.get('email')):
            cust_id = res[0]
            names = data.get('name', '').split(' ')
            first = names[0]
            last = " ".join(names[1:]) if len(names) > 1 else ""
            cust_query = "UPDATE customer SET customer_first_name = %s, customer_last_name = %s, email = %s, alt_num = %s, profession = %s WHERE customer_id = %s"
            cursor.execute(cust_query, (first, last, data.get('email'), data.get('alternate_phone'), data.get('profession'), cust_id))

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False
    finally:
        conn.close()

def delete_existing_lead(lead_id):
    """Deactivates a lead (Soft delete)."""
    conn = get_db_connection()
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
    """Fetches all employee names."""
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT emp_first_name, emp_last_name FROM employee ORDER BY emp_first_name"
        cursor.execute(query)
        result = cursor.fetchall()
        return [f"{r['emp_first_name']} {r['emp_last_name'] or ''}".strip() for r in result]
    except Exception as e:
        return []
    finally:
        conn.close()
