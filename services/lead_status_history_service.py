"""
Service: Lead Status History
Tracks every status change a lead goes through.
Table: lead_status_history

"Activity" and "Status" are the same concept in this app.
The UI may say "Activity" but the DB column is status.
"""

from db import get_db


def get_history_by_lead(lead_id):
    """Fetch full status change history for a lead, newest first."""
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT
                h.history_id,
                h.lead_id,
                h.old_status_id,
                h.new_status_id,
                h.remarks,
                h.changed_by,
                h.changed_at,
                os.status_name  AS old_status_name,
                ns.status_name  AS new_status_name,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS changed_by_name
            FROM lead_status_history h
            LEFT JOIN lead_status os ON os.status_id = h.old_status_id
            LEFT JOIN lead_status ns ON ns.status_id = h.new_status_id
            LEFT JOIN employee e ON e.emp_id = h.changed_by
            WHERE h.lead_id = %s
            ORDER BY h.changed_at DESC
        """, (lead_id,))
        rows = cursor.fetchall()

        for row in rows:
            if row.get('changed_at'):
                row['changed_at'] = row['changed_at'].isoformat()

        return rows
    finally:
        cursor.close()
        conn.close()


def get_history_entry(history_id):
    """Fetch a single history entry."""
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT
                h.history_id,
                h.lead_id,
                h.old_status_id,
                h.new_status_id,
                h.remarks,
                h.changed_by,
                h.changed_at,
                os.status_name AS old_status_name,
                ns.status_name AS new_status_name,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS changed_by_name
            FROM lead_status_history h
            LEFT JOIN lead_status os ON os.status_id = h.old_status_id
            LEFT JOIN lead_status ns ON ns.status_id = h.new_status_id
            LEFT JOIN employee e ON e.emp_id = h.changed_by
            WHERE h.history_id = %s
        """, (history_id,))
        row = cursor.fetchone()
        if row and row.get('changed_at'):
            row['changed_at'] = row['changed_at'].isoformat()
        return row
    finally:
        cursor.close()
        conn.close()


def create_history(lead_id, data, emp_id):
    """
    Log a status change and update the lead's current status.

    Steps:
      1. Read lead's current status_id → becomes old_status_id
      2. Insert row into lead_status_history
      3. Update leads.status_id to the new status

    Args:
        lead_id:  str (VARCHAR 150)
        data:     dict with new_status_id (required), remarks (optional)
        emp_id:   str — employee ID from JWT token
    """
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        # 1. Get current status
        cursor.execute(
            "SELECT status_id FROM leads WHERE lead_id = %s",
            (lead_id,)
        )
        lead = cursor.fetchone()
        if not lead:
            raise ValueError(f"Lead {lead_id} not found")

        old_status_id = lead.get('status_id')
        new_status_id = data.get('new_status_id')

        # 2. Insert history row
        cursor.execute("""
            INSERT INTO lead_status_history
                (lead_id, old_status_id, new_status_id, remarks, changed_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            lead_id,
            old_status_id,
            new_status_id,
            data.get('remarks', ''),
            emp_id
        ))
        new_id = cursor.lastrowid

        # 3. Update lead's current status
        cursor.execute("""
            UPDATE leads
            SET status_id = %s, modified_by = %s, modified_on = NOW()
            WHERE lead_id = %s
        """, (new_status_id, emp_id, lead_id))

        conn.commit()
        return get_history_entry(new_id)

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def update_history(history_id, data, emp_id):
    """
    Edit a history entry (remarks only — you shouldn't rewrite status changes).
    Does NOT move the lead's pipeline.
    """
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE lead_status_history
            SET remarks = %s
            WHERE history_id = %s
        """, (data.get('remarks', ''), history_id))
        conn.commit()
        return get_history_entry(history_id)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def delete_history(history_id):
    """Hard-delete a history entry. Admin only."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM lead_status_history WHERE history_id = %s",
            (history_id,)
        )
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def get_status_options():
    """Return all active statuses for the dropdown."""
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT status_id, status_name, status_category
            FROM lead_status
            WHERE is_active = 1
            ORDER BY status_name ASC
        """)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()