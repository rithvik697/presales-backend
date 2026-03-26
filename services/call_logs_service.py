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
                CONCAT_WS(' ',
                    e.emp_first_name,
                    e.emp_middle_name,
                    e.emp_last_name
                ) AS userName,

                CONCAT_WS(' ',
                    cu.customer_first_name,
                    cu.customer_middle_name,
                    cu.customer_last_name
                ) AS leadName,

                cu.phone_num AS phoneNumber,
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
                l.lead_description AS remarks,
                c.recording_url AS recordingUrl

            FROM call_log c
            LEFT JOIN employee e ON c.emp_id = e.emp_id
            LEFT JOIN leads l ON c.lead_id = l.lead_id
            LEFT JOIN customer cu ON l.customer_id = cu.customer_id
            ORDER BY c.call_time DESC
        """)

        return cursor.fetchall()

    finally:
        cursor.close()
        db.close()


def get_call_logs_for_lead_ui(lead_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                c.call_id AS callId,
                DATE(c.call_time) AS callDate,
                CAST(TIME(c.call_time) AS CHAR) AS startTime,
                CASE
                    WHEN c.call_duration IS NULL THEN NULL
                    ELSE CAST(ADDTIME(TIME(c.call_time), SEC_TO_TIME(c.call_duration)) AS CHAR)
                END AS endTime,
                CASE
                    WHEN c.call_duration IS NULL THEN '-'
                    ELSE CONCAT(
                        FLOOR(c.call_duration / 60), ' min ',
                        MOD(c.call_duration, 60), ' sec'
                    )
                END AS duration,
                c.call_status AS callStatus,
                c.call_source AS callSource,
                c.call_time AS callTime,
                CONCAT_WS(' ',
                    e.emp_first_name,
                    e.emp_middle_name,
                    e.emp_last_name
                ) AS madeBy,
                l.lead_description AS remarks,
                c.recording_url AS recordingUrl
            FROM call_log c
            LEFT JOIN employee e ON c.emp_id = e.emp_id
            LEFT JOIN leads l ON c.lead_id = l.lead_id
            WHERE c.lead_id = %s
            ORDER BY c.call_time DESC
        """, (lead_id,))

        return cursor.fetchall()

    finally:
        cursor.close()
        db.close()


def create_manual_call_log(data):
    """Create a manual call log entry."""
    db = get_db()
    cursor = db.cursor()

    try:
        cursor.execute("""
            INSERT INTO call_log
            (lead_id, emp_id, call_time, call_duration, call_status, call_source, remarks)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get("lead_id"),
            data.get("emp_id"),
            data.get("call_time", datetime.now()),
            data.get("call_duration"),
            data.get("call_status", "Connected"),
            data.get("call_source", "Manual"),
            data.get("remarks")
        ))

        db.commit()
        return cursor.lastrowid

    finally:
        cursor.close()
        db.close()


def update_call_log(call_id, data):
    """Update an existing call log entry."""
    db = get_db()
    cursor = db.cursor()

    try:
        cursor.execute("""
            UPDATE call_log
            SET call_status = %s,
                call_duration = %s,
                remarks = %s
            WHERE call_id = %s
        """, (
            data.get("call_status"),
            data.get("call_duration"),
            data.get("remarks"),
            call_id
        ))

        db.commit()
        return cursor.rowcount > 0

    finally:
        cursor.close()
        db.close()


def delete_call_log(call_id):
    """Delete a call log entry."""
    db = get_db()
    cursor = db.cursor()

    try:
        cursor.execute("DELETE FROM call_log WHERE call_id = %s", (call_id,))
        db.commit()
        return cursor.rowcount > 0

    finally:
        cursor.close()
        db.close()

