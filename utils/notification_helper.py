from db import get_db


def notify_admins(title, message, object_name, object_id):

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT emp_id
        FROM employee
        WHERE role_id = 'ADMIN'
        AND emp_status = 'Active'
    """)

    admins = cursor.fetchall()

    for admin in admins:

        cursor.execute("""
            INSERT INTO notifications
            (emp_id, title, message, object_name, object_id, is_read, created_on)
            VALUES (%s, %s, %s, %s, %s, 0, NOW())
        """, (admin[0], title, message, object_name, object_id))

    conn.commit()

    cursor.close()
    conn.close()
    
def lead_created_template(lead_id, created_by):

    return {
        "title": "New Lead Created",
        "message": f"Lead {lead_id} was created by {created_by}."
    }