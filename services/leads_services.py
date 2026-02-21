from db import get_db_connection

import mysql.connector
from datetime import datetime

def _generate_id(cursor, table, id_col, prefix):
    """
    Generates a sequential ID (e.g., L001, L002).
    Finds the highest numeric suffix for the given prefix and increments it.
    Uses REGEXP to ignore IDs that don't match the standard format (e.g., mixed with UUIDs).
    """
    # Pattern: ^PREFIX followed by digits only
    pattern = f"^{prefix}[0-9]+$"
    query = f"SELECT {id_col} FROM {table} WHERE {id_col} REGEXP %s ORDER BY LENGTH({id_col}) DESC, {id_col} DESC LIMIT 1"
    cursor.execute(query, (pattern,))
    result = cursor.fetchone()
    
    if result:
        last_id = result[0]
        try:
            # Extract number part
            number_part = int(last_id[len(prefix):])
            new_number = number_part + 1
        except ValueError:
             # Should not happen with REGEXP but safety fallback
            new_number = 1
    else:
        new_number = 1
        
    return f"{prefix}{str(new_number).zfill(3)}"

def get_all_leads(filters=None):
    """
    Fetches leads with JOINs on customer, employee, source, status, and audit users.
    Returns list of dictionaries with flattened, human-readable data.
    """
    conn = get_db_connection()
    leads = []
    if not conn:
        print("DB connection failed during get_all_leads")
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        
        # Base Query with JOINs
        # Joining employee table 3 times: Assigned To, Created By, Modified By
        query = """
        SELECT 
            l.lead_id as id,
            CONCAT(c.customer_first_name, ' ', IFNULL(c.customer_last_name, '')) as name,
            c.phone_num as phone,
            c.email as email,
            ls.source_name as source,
            lst.status_name as status,
            TRIM(CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, ''))) as assigned_to,
            l.lead_description as description,
            l.created_on as createdAt,
            l.modified_on as modifiedAt,
            l.created_by as createdById,
            l.modified_by as modifiedById,
            -- Try to resolve names if foreign keys, otherwise pass raw
            ec.emp_first_name as createdByName,
            em.emp_first_name as modifiedByName
        FROM leads l
        LEFT JOIN customer c ON l.customer_id = c.customer_id
        LEFT JOIN lead_sources ls ON l.source_id = ls.source_id
        LEFT JOIN lead_status lst ON l.status_id = lst.status_id
        LEFT JOIN employee e ON l.emp_id = e.emp_id
        LEFT JOIN employee ec ON l.created_by = ec.emp_id
        LEFT JOIN employee em ON l.modified_by = em.emp_id
        WHERE 1=1
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
        
        # Post-process to fallback if names are null (e.g. if created_by was just a string name)
        for row in result:
             # Logic: if createdByName found (join success), use it. Else use raw createdById (might be name)
             created_by = row['createdByName'] or row['createdById']
             modified_by = row['modifiedByName'] or row['modifiedById']
             
             row['created_by'] = created_by
             row['modified_by'] = modified_by
             leads.append(row)
        
    except mysql.connector.Error as e:
        print(f"Error fetching leads: {e}")
    finally:
        conn.close()
    return leads
    
def update_lead(lead_id, data):
    """
    Updates a lead.
    Fields customizable: Status, Assigned To, Project(Desc), Source, Name (Customer update), Email.
    Phone is NOT editable (Customer identity).
    """
    conn = get_db_connection()
    if not conn: return False
    
    try:
        cursor = conn.cursor()
        
        # 1. Resolve IDs for lookups
        source_id = _get_id_by_name(cursor, 'lead_sources', 'source_name', 'source_id', data.get('source'))
        status_id = _get_id_by_name(cursor, 'lead_status', 'status_name', 'status_id', data.get('status'))
        emp_id = _get_employee_id_by_name(cursor, data.get('assigned_to'))
        
        # 2. Handle Project/Description
        # We need to preserve original non-project description if possible or just overwrite?
        # For simplicity, we reconstruct it: "Project: X | Description"
        description = data.get('description', '')
        project = data.get('project')
        if project:
            if description:
                description = f"Project: {project} | {description}"
            else:
                description = f"Project: {project}"
                
        # 3. Update Leads Table
        # Note: We update modified_by and modified_on
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
        current_time = datetime.utcnow()
        cursor.execute(query, (source_id, status_id, emp_id, description, current_time, modifier_id, lead_id))

        # 4. Update Customer Name/Email if provided (Optional, but "editing should be possible")
        # We need to find the customer_id first to be safe
        cursor.execute("SELECT customer_id FROM leads WHERE lead_id = %s", (lead_id,))
        res = cursor.fetchone()
        if res and (data.get('name') or data.get('email')):
            cust_id = res[0]
            # Split name
            full_name = data.get('name', '').split(' ')
            if len(full_name) > 0:
                first = full_name[0]
                last = " ".join(full_name[1:]) if len(full_name) > 1 else ""
                
                alt_phone_update = data.get('alternate_phone')
                if alt_phone_update:
                    alt_phone_update = re.sub(r'\D', '', str(alt_phone_update))
                if not alt_phone_update:
                    alt_phone_update = None
                    
                cust_query = "UPDATE customer SET customer_first_name = %s, customer_last_name = %s, email = %s, alt_num = %s WHERE customer_id = %s"
                cursor.execute(cust_query, (first, last, data.get('email'), alt_phone_update, cust_id))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Update failed: {e}")
        return False
    finally:
        conn.close()

def get_lead_by_id(lead_id):
    """Fetches a single lead by ID with JOIN details."""
    conn = get_db_connection()
    lead = None
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
            ls.source_name as source,
            lst.status_name as status,
            e.emp_first_name as assigned_to,
            l.lead_description as description,
            l.created_on as createdAt,
            l.modified_on as modifiedAt,
            -- Resolve user names
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
        WHERE l.lead_id = %s
        """
        cursor.execute(query, (lead_id,))
        result = cursor.fetchone()
        
        if result:
            # Format name for consistency
            result['name'] = f"{result['customer_first_name']} {result['customer_last_name'] or ''}".strip()
            del result['customer_first_name']
            del result['customer_last_name']
            
            # Resolve audit names
            result['created_by'] = result['createdByName'] or result['createdById']
            result['modified_by'] = result['modifiedByName'] or result['modifiedById']
            
            lead = result

    except mysql.connector.Error as e:
        print(f"Error fetching lead {lead_id}: {e}")
    finally:
        conn.close()
    return lead

import re

def _get_or_create_customer(cursor, data):
    """Helper to find or create a customer by phone number."""
    phone = data.get('phone')
    if not phone:
        return None
        
    # Sanitize phone to digits only (for BIGINT schema)
    phone = re.sub(r'\D', '', str(phone))
        
    # Check existence
    cursor.execute("SELECT customer_id FROM customer WHERE phone_num = %s", (phone,))
    res = cursor.fetchone()
    if res:
        return res[0] # Return existing ID (tuple access since default cursor)

    # Create new
    customer_id = _generate_id(cursor, 'customer', 'customer_id', 'CUST')
    # Handle name splitting
    full_name = data.get('name', '').split(' ')
    first_name = full_name[0]
    last_name = " ".join(full_name[1:]) if len(full_name) > 1 else ""

    alt_phone = data.get('alternate_phone')
    if alt_phone:
        alt_phone = re.sub(r'\D', '', str(alt_phone))
    if not alt_phone:
        alt_phone = None

    query = """
    INSERT INTO customer (customer_id, customer_first_name, customer_last_name, phone_num, alt_num, email, created_on, is_active)
    VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
    """
    cursor.execute(query, (customer_id, first_name, last_name, phone, alt_phone, data.get('email'), datetime.utcnow()))
    return customer_id

def _get_id_by_name(cursor, table, name_col, id_col, name_val):
    """Helper to lookup ID by Name. Returns None if not found."""
    if not name_val: return None
    query = f"SELECT {id_col} FROM {table} WHERE {name_col} = %s LIMIT 1"
    cursor.execute(query, (name_val,))
    res = cursor.fetchone()
    return res[0] if res else None

def _get_employee_id_by_name(cursor, full_name):
    """Helper to find employee ID by full name (First Last)."""
    if not full_name: return None
    
    # Try exact match on First Name (legacy/simple case)
    cursor.execute("SELECT emp_id FROM employee WHERE emp_first_name = %s", (full_name,))
    res = cursor.fetchone()
    if res: return res[0]
    
    # Try match on Full Name "First Last"
    query = "SELECT emp_id FROM employee WHERE TRIM(CONCAT(emp_first_name, ' ', IFNULL(emp_last_name, ''))) = %s"
    cursor.execute(query, (full_name,))
    res = cursor.fetchone()
    if res: return res[0]
    
    return None

def create_lead(data):
    """
    Transactional creation of Lead with Sequential ID generation.
    1. Find/Create Customer.
    2. Lookup Source, Status, Employee IDs.
    3. Insert Lead.
    """
    if not data.get('name') or not data.get('phone'):
        raise ValueError("Name and Phone are required fields.")

    conn = get_db_connection()
    if not conn: raise Exception("DB Connection failed")

    new_lead_id = None
    try:
        cursor = conn.cursor() # Default tuple cursor for internal lookups
        
        # 1. Handle Customer
        customer_id = _get_or_create_customer(cursor, data)
        if not customer_id:
            raise ValueError("Invalid Customer Data (Phone required)")

        # 2. Resolve Foreign Keys
        # Use simple lookups or defaults from seeds
        source_id = _get_id_by_name(cursor, 'lead_sources', 'source_name', 'source_id', data.get('source')) or 'SRC001' # Default Website
        status_id = _get_id_by_name(cursor, 'lead_status', 'status_name', 'status_id', data.get('status')) or 'STAT001' # Default New
        emp_id = _get_employee_id_by_name(cursor, data.get('assigned_to')) or 'EMP001' # Default Admin

        # 3. Handle Project (Append to Description)
        description = data.get('description', '')
        project = data.get('project')
        if project:
            if description:
                description = f"Project: {project} | {description}"
            else:
                description = f"Project: {project}"

        # 4. Create Lead
        new_lead_id = _generate_id(cursor, 'leads', 'lead_id', 'L') # Generating L001, L002...
        query = """
        INSERT INTO leads (lead_id, customer_id, source_id, status_id, emp_id, lead_description, created_on, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
        """
        cursor.execute(query, (new_lead_id, customer_id, source_id, status_id, emp_id, description, datetime.utcnow()))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Transaction failed: {e}")
        raise e
    finally:
        conn.close()
    
    return new_lead_id

def delete_lead(lead_id):
    """Deletes a lead and its dependent records (call logs, campaigns)."""
    conn = get_db_connection()
    if not conn: return False
    try:
        cursor = conn.cursor()
        
        # Delete dependent records first to avoid foreign key constraints
        cursor.execute("DELETE FROM call_log WHERE lead_id = %s", (lead_id,))
        cursor.execute("DELETE FROM campaign WHERE lead_id = %s", (lead_id,))
        
        # Delete the lead
        cursor.execute("DELETE FROM leads WHERE lead_id = %s", (lead_id,))
        
        conn.commit()
        return True
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"Error deleting lead {lead_id}: {e}")
        return False
    finally:
        conn.close()

def get_all_employees():
    """Fetches all employees from the database."""
    conn = get_db_connection()
    employees = []
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT emp_first_name, emp_last_name FROM employee ORDER BY emp_first_name"
        cursor.execute(query)
        result = cursor.fetchall()
        
        # Format names
        for row in result:
            name = f"{row['emp_first_name']} {row['emp_last_name'] or ''}".strip()
            employees.append(name)
            
    except mysql.connector.Error as e:
        print(f"Error fetching employees: {e}")
    finally:
        conn.close()
    return employees

def get_all_sources():
    """Fetches all lead sources from the database."""
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT source_name FROM lead_sources WHERE is_active = 1 ORDER BY source_name")
        return [row['source_name'] for row in cursor.fetchall()]
    except mysql.connector.Error as e:
        print(f"Error fetching sources: {e}")
        return []
    finally:
        conn.close()

def get_all_statuses():
    """Fetches all lead statuses from the database."""
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT status_name FROM lead_status WHERE is_active = 1 ORDER BY status_name")
        return [row['status_name'] for row in cursor.fetchall()]
    except mysql.connector.Error as e:
        print(f"Error fetching statuses: {e}")
        return []
    finally:
        conn.close()

def get_all_projects():
    """Fetches all active projects from the database."""
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT project_name FROM project_registration WHERE status = 'Active' ORDER BY project_name")
        return [row['project_name'] for row in cursor.fetchall()]
    except mysql.connector.Error as e:
        print(f"Error fetching projects: {e}")
        return []
    finally:
        conn.close()
