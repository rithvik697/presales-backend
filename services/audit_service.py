from db import get_db


# --------------------------------
# INSERT AUDIT LOG (Already Working)
# --------------------------------
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


# --------------------------------
# FETCH AUDIT LOGS (For Frontend)
# --------------------------------
def get_audit_logs():

    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                a.audit_id,
                a.object_name,
                a.object_id,
                a.property_name,

                CASE
                    WHEN a.property_name = 'status_id' THEN ls_old.status_name
                    WHEN a.property_name = 'source_id' THEN src_old.source_name
                    WHEN a.property_name = 'emp_id' THEN CONCAT(emp_old.emp_first_name,' ',emp_old.emp_last_name)
                    ELSE a.old_value
                END AS old_value,

                CASE
                    WHEN a.property_name = 'status_id' THEN ls_new.status_name
                    WHEN a.property_name = 'source_id' THEN src_new.source_name
                    WHEN a.property_name = 'emp_id' THEN CONCAT(emp_new.emp_first_name,' ',emp_new.emp_last_name)
                    ELSE a.new_value
                END AS new_value,

                e.username AS modified_by,
                a.modified_on,
                a.action_type

            FROM audit_trail a

            LEFT JOIN employee e
            ON a.modified_by = e.emp_id

            LEFT JOIN lead_status ls_old
            ON a.old_value = ls_old.status_id

            LEFT JOIN lead_status ls_new
            ON a.new_value = ls_new.status_id

            LEFT JOIN lead_sources src_old
            ON a.old_value = src_old.source_id

            LEFT JOIN lead_sources src_new
            ON a.new_value = src_new.source_id

            LEFT JOIN employee emp_old
            ON a.old_value = emp_old.emp_id

            LEFT JOIN employee emp_new
            ON a.new_value = emp_new.emp_id

            ORDER BY a.modified_on DESC
        """

        cursor.execute(query)
        logs = cursor.fetchall()

        return logs

    except Exception as e:
        print("FETCH AUDIT LOG ERROR:", e)
        return []

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()