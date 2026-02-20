from datetime import datetime
from db import get_db


def start_call_service(lead_id, emp_id):
    db = get_db()
    cursor = db.cursor()

    try:
        call_time = datetime.now()

        cursor.execute("""
            INSERT INTO call_log
            (lead_id, emp_id, call_time, call_status, call_source)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            lead_id,
            emp_id,
            call_time,
            "Connected",   # MUST match ENUM
            "CRM"
        ))

        db.commit()
        return cursor.lastrowid

    finally:
        cursor.close()
        db.close()


def end_call_service(call_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        # Fetch call start time and duration
        cursor.execute("""
            SELECT call_time, call_duration
            FROM call_log
            WHERE call_id = %s
        """, (call_id,))
        call = cursor.fetchone()

        if not call:
            return None  # Call not found

        if call["call_duration"] is not None:
            return "ALREADY_ENDED"

        end_time = datetime.now()
        duration = int((end_time - call["call_time"]).total_seconds())

        cursor.execute("""
            UPDATE call_log
            SET
                call_duration = %s,
                call_status = %s
            WHERE call_id = %s
        """, (
            duration,
            "Connected",   # or "Completed" ONLY if ENUM allows it
            call_id
        ))

        db.commit()
        return duration

    finally:
        cursor.close()
        db.close()


def get_call_logs_service():
    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                call_id,
                lead_id,
                emp_id,
                call_time,
                call_duration,
                call_status,
                call_source,
                created_at
            FROM call_log
            ORDER BY call_time DESC
        """)
        return cursor.fetchall()

    finally:
        cursor.close()
        db.close()

def get_call_logs_for_ui():
    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, '')) AS userName,
                CONCAT(cust.customer_first_name, ' ', IFNULL(cust.customer_last_name, '')) AS leadName,
                cust.phone_num AS phoneNumber,
                c.call_source AS callType,
                c.call_status AS callStatus,
                CASE
                    WHEN c.call_duration IS NULL THEN '-'
                    ELSE CONCAT(
                        FLOOR(c.call_duration / 60), 'm ',
                        MOD(c.call_duration, 60), 's'
                    )
                END AS callDuration,
                c.call_time AS callTime,
                l.lead_description AS remarks
            FROM call_log c
            JOIN employee e ON c.emp_id = e.emp_id
            JOIN leads l ON c.lead_id = l.lead_id
            LEFT JOIN customer cust ON l.customer_id = cust.customer_id
            ORDER BY c.call_time DESC
        """)
        return cursor.fetchall()

    finally:
        cursor.close()
        db.close()

