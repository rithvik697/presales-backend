from db import get_db


def create_notification(emp_id, title, message, object_name=None, object_id=None):

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO notifications
        (emp_id, title, message, object_name, object_id)
        VALUES (%s,%s,%s,%s,%s)
    """, (emp_id, title, message, object_name, object_id))

    conn.commit()

    cursor.close()
    conn.close()