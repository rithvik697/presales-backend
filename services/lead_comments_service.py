from db import get_db


def _ensure_lead_comments_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lead_comments (
            comment_id INT AUTO_INCREMENT PRIMARY KEY,
            lead_id VARCHAR(20) NOT NULL,
            comment_text TEXT NOT NULL,
            created_by VARCHAR(20) NOT NULL,
            created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)


def create_lead_comment(lead_id, comment_text, created_by):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        _ensure_lead_comments_table(cursor)

        cursor.execute(
            "SELECT lead_id FROM leads WHERE lead_id = %s AND is_active = 1",
            (lead_id,)
        )
        lead = cursor.fetchone()

        if not lead:
            raise ValueError(f"Lead {lead_id} not found")

        cursor.execute("""
            INSERT INTO lead_comments
                (lead_id, comment_text, created_by)
            VALUES (%s, %s, %s)
        """, (
            lead_id,
            comment_text,
            created_by
        ))

        comment_id = cursor.lastrowid
        conn.commit()

        cursor.execute("""
            SELECT
                c.comment_id,
                c.lead_id,
                c.comment_text,
                c.created_by,
                c.created_on,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS created_by_name
            FROM lead_comments c
            LEFT JOIN employee e ON e.emp_id = c.created_by
            WHERE c.comment_id = %s
        """, (comment_id,))

        row = cursor.fetchone()

        if row and row.get('created_on'):
            row['created_on'] = row['created_on'].isoformat()
        if row and row.get('created_by_name'):
            row['created_by_name'] = row['created_by_name'].strip()

        return row

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def get_comments_by_lead(lead_id):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        _ensure_lead_comments_table(cursor)

        cursor.execute("""
            SELECT
                c.comment_id,
                c.lead_id,
                c.comment_text,
                c.created_by,
                c.created_on,
                CONCAT(e.emp_first_name, ' ', COALESCE(e.emp_last_name, '')) AS created_by_name
            FROM lead_comments c
            LEFT JOIN employee e ON e.emp_id = c.created_by
            WHERE c.lead_id = %s
            ORDER BY c.created_on DESC
        """, (lead_id,))

        rows = cursor.fetchall()

        for row in rows:
            if row.get('created_on'):
                row['created_on'] = row['created_on'].isoformat()
            if row.get('created_by_name'):
                row['created_by_name'] = row['created_by_name'].strip()

        return rows

    finally:
        cursor.close()
        conn.close()
        