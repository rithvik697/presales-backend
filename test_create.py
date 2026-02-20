import mysql.connector
from db import DB_CONFIG
from data import leads_data

def test_create():
    data = {
        'name': 'Test User',
        'phone': '+91 1234567890',
        'email': 'test@example.com',
        'project': 'Amity',
        'source': 'Google',
        'status': 'New',
        'assigned_to': 'Admin',
        'description': 'Test description',
        'alternate_phone': '9876543210',
        'profession': 'Engineer'
    }
    try:
        new_id = leads_data.create_lead(data)
        print(f"Success! ID: {new_id}")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    test_create()
