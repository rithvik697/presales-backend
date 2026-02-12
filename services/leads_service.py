from data import leads_data

# init_leads_db removed as schema is now static/managed by init_db.py

def fetch_all_leads(filters=None):
    """Retrieves all leads from the data layer with optional filtering."""
    return leads_data.get_all_leads(filters)

def fetch_lead_by_id(lead_id):
    """Retrieves a specific lead by ID."""
    return leads_data.get_lead_by_id(lead_id)

def add_new_lead(data):
    """
    Validates and adds a new lead.
    Expected data: name, phone (required).
    Optional: email, source, status, assigned_to, description.
    """
    if not data.get('name') or not data.get('phone'):
        raise ValueError("Name and Phone are required fields.")
    
    return leads_data.create_lead(data)

def modify_lead(lead_id, data):
    """Updates an existing lead."""
    return leads_data.update_lead(lead_id, data)

def remove_lead(lead_id):
    """Deletes a lead."""
    return leads_data.delete_lead(lead_id)

def fetch_all_employees():
    """Retrieves all employees."""
    return leads_data.get_all_employees()

def update_existing_lead(lead_id, data):
    """Updates an existing lead."""
    return leads_data.update_lead(lead_id, data)
