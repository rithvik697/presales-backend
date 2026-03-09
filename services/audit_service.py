from db import get_db

def log_audit(object_name, object_id, property_name, old_value, new_value, modified_by, action_type):
    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor()

        query = """
            INSERT INTO audit_trail
            (object_name, object_id, property_name, old_value, new_value, modified_by, action_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """

        values = (
            object_name,
            str(object_id),  # ensure string since column is VARCHAR
            property_name,
            old_value,
            new_value,
            modified_by,
            action_type
        )

        cursor.execute(query, values)
        conn.commit()

        print(f"AUDIT LOG INSERTED → {object_name} | {object_id} | {action_type}")

    except Exception as e:
        print("AUDIT LOG ERROR:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()