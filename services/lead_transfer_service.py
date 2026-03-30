from db import get_db
from services.audit_service import log_audit
from services.notification_service import create_notification


def _ensure_transfer_log_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lead_transfer_log (
            transfer_id INT AUTO_INCREMENT PRIMARY KEY,
            from_emp_id VARCHAR(150) NOT NULL,
            to_emp_id VARCHAR(150) NOT NULL,
            from_project_id VARCHAR(150) NULL,
            from_source_id VARCHAR(150) NULL,
            from_status_id VARCHAR(150) NULL,
            to_project_id VARCHAR(150) NULL,
            to_source_id VARCHAR(150) NULL,
            to_status_id VARCHAR(150) NULL,
            date_type VARCHAR(30) NULL,
            from_date DATE NULL,
            to_date DATE NULL,
            lead_count INT NOT NULL,
            created_by VARCHAR(150) NOT NULL,
            created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _ensure_column(cursor, "lead_transfer_log", "from_project_id", "ALTER TABLE lead_transfer_log ADD COLUMN from_project_id VARCHAR(150) NULL")
    _ensure_column(cursor, "lead_transfer_log", "from_source_id", "ALTER TABLE lead_transfer_log ADD COLUMN from_source_id VARCHAR(150) NULL")
    _ensure_column(cursor, "lead_transfer_log", "from_status_id", "ALTER TABLE lead_transfer_log ADD COLUMN from_status_id VARCHAR(150) NULL")
    _ensure_column(cursor, "lead_transfer_log", "to_project_id", "ALTER TABLE lead_transfer_log ADD COLUMN to_project_id VARCHAR(150) NULL")
    _ensure_column(cursor, "lead_transfer_log", "to_source_id", "ALTER TABLE lead_transfer_log ADD COLUMN to_source_id VARCHAR(150) NULL")
    _ensure_column(cursor, "lead_transfer_log", "to_status_id", "ALTER TABLE lead_transfer_log ADD COLUMN to_status_id VARCHAR(150) NULL")
    _ensure_column(cursor, "lead_transfer_log", "date_type", "ALTER TABLE lead_transfer_log ADD COLUMN date_type VARCHAR(30) NULL")
    _ensure_column(cursor, "lead_transfer_log", "from_date", "ALTER TABLE lead_transfer_log ADD COLUMN from_date DATE NULL")
    _ensure_column(cursor, "lead_transfer_log", "to_date", "ALTER TABLE lead_transfer_log ADD COLUMN to_date DATE NULL")


def _ensure_column(cursor, table_name, column_name, alter_sql):
    cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
    if not cursor.fetchone():
        cursor.execute(alter_sql)


def _validate_sales_exec(cursor, emp_id, require_active):
    cursor.execute("""
        SELECT emp_id, role_id, emp_status
        FROM employee
        WHERE emp_id = %s
    """, (emp_id,))
    employee = cursor.fetchone()

    if not employee:
        raise ValueError("Employee not found")
    if employee["role_id"] != "SALES_EXEC":
        raise ValueError("Leads can only be transferred from a sales executive")
    if require_active and employee["emp_status"] != "Active":
        raise ValueError("Target employee must be active")

    return employee


def _validate_transfer_target(cursor, emp_id, require_active):
    cursor.execute("""
        SELECT emp_id, role_id, emp_status
        FROM employee
        WHERE emp_id = %s
    """, (emp_id,))
    employee = cursor.fetchone()

    if not employee:
        raise ValueError("Target employee not found")
    if employee["role_id"] not in {"SALES_EXEC", "ADMIN"}:
        raise ValueError("Leads can only be transferred to an active sales executive or admin")
    if require_active and employee["emp_status"] != "Active":
        raise ValueError("Target employee must be active")

    return employee


def _build_lead_transfer_filters(filters):
    query = """
        FROM leads l
        WHERE l.is_active = 1
          AND l.emp_id = %s
    """
    params = [filters["from_emp_id"]]

    if filters.get("from_project_id"):
        query += " AND l.project_id = %s"
        params.append(filters["from_project_id"])

    if filters.get("from_source_id"):
        query += " AND l.source_id = %s"
        params.append(filters["from_source_id"])

    if filters.get("from_status_id"):
        query += " AND l.status_id = %s"
        params.append(filters["from_status_id"])

    date_type = filters.get("date_type")
    if date_type == "modified_on":
        date_column = "DATE(COALESCE(l.modified_on, l.created_on))"
    else:
        date_column = "DATE(l.created_on)"

    if filters.get("from_date"):
        query += f" AND {date_column} >= %s"
        params.append(filters["from_date"])

    if filters.get("to_date"):
        query += f" AND {date_column} <= %s"
        params.append(filters["to_date"])

    return query, params


def preview_lead_transfer(
    from_emp_id,
    from_project_id=None,
    from_source_id=None,
    from_status_id=None,
    date_type=None,
    from_date=None,
    to_date=None
):
    conn = get_db()
    if not conn:
        raise Exception("DB connection failed")

    try:
        cursor = conn.cursor(dictionary=True)
        _validate_sales_exec(cursor, from_emp_id, require_active=False)

        filter_query, params = _build_lead_transfer_filters({
            "from_emp_id": from_emp_id,
            "from_project_id": from_project_id,
            "from_source_id": from_source_id,
            "from_status_id": from_status_id,
            "date_type": date_type,
            "from_date": from_date,
            "to_date": to_date,
        })

        cursor.execute(f"SELECT COUNT(*) AS lead_count {filter_query}", tuple(params))
        result = cursor.fetchone() or {"lead_count": 0}
        return {"lead_count": result["lead_count"]}
    finally:
        conn.close()


def transfer_leads(
    from_emp_id,
    to_emp_id,
    actor_id,
    from_project_id=None,
    from_source_id=None,
    from_status_id=None,
    to_project_id=None,
    to_source_id=None,
    to_status_id=None,
    date_type=None,
    from_date=None,
    to_date=None,
    limit=None
):
    conn = get_db()
    if not conn:
        raise Exception("DB connection failed")

    try:
        cursor = conn.cursor(dictionary=True)
        _ensure_transfer_log_table(cursor)

        _validate_sales_exec(cursor, from_emp_id, require_active=False)
        _validate_transfer_target(cursor, to_emp_id, require_active=True)

        if from_emp_id == to_emp_id:
            raise ValueError("From employee and to employee cannot be the same")

        filter_query, params = _build_lead_transfer_filters({
            "from_emp_id": from_emp_id,
            "from_project_id": from_project_id,
            "from_source_id": from_source_id,
            "from_status_id": from_status_id,
            "date_type": date_type,
            "from_date": from_date,
            "to_date": to_date,
        })

        select_query = f"""
            SELECT l.lead_id, l.project_id, l.source_id, l.status_id
            {filter_query}
            ORDER BY l.created_on ASC
        """

        if limit is not None:
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                raise ValueError("Transfer count must be a valid number")
            if limit < 1:
                raise ValueError("Transfer count must be greater than 0")
            select_query += " LIMIT %s"
            params = params + [limit]

        cursor.execute(select_query, tuple(params))
        leads = cursor.fetchall()

        if not leads:
            raise ValueError("No leads available for transfer with the selected filters")

        lead_ids = [row["lead_id"] for row in leads]

        for lead in leads:
            lead_id = lead["lead_id"]
            cursor.execute("""
                UPDATE leads
                SET emp_id = %s,
                    project_id = COALESCE(%s, project_id),
                    source_id = COALESCE(%s, source_id),
                    status_id = COALESCE(%s, status_id),
                    modified_on = NOW(),
                    modified_by = %s
                WHERE lead_id = %s
            """, (to_emp_id, to_project_id, to_source_id, to_status_id, actor_id, lead_id))

            log_audit("Leads", lead_id, "emp_id", from_emp_id, to_emp_id, actor_id, "UPDATE")

            if to_project_id and to_project_id != lead["project_id"]:
                log_audit("Leads", lead_id, "project_id", lead["project_id"], to_project_id, actor_id, "UPDATE")

            if to_source_id and to_source_id != lead["source_id"]:
                log_audit("Leads", lead_id, "source_id", lead["source_id"], to_source_id, actor_id, "UPDATE")

            if to_status_id and to_status_id != lead["status_id"]:
                log_audit("Leads", lead_id, "status_id", lead["status_id"], to_status_id, actor_id, "UPDATE")
                cursor.execute("""
                    INSERT INTO lead_status_history
                    (lead_id, old_status_id, new_status_id, remarks, changed_by)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    lead_id,
                    lead["status_id"],
                    to_status_id,
                    "Status updated during lead transfer",
                    actor_id
                ))

        cursor.execute("""
            INSERT INTO lead_transfer_log
                (
                    from_emp_id, to_emp_id,
                    from_project_id, from_source_id, from_status_id,
                    to_project_id, to_source_id, to_status_id,
                    date_type, from_date, to_date,
                    lead_count, created_by
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            from_emp_id,
            to_emp_id,
            from_project_id,
            from_source_id,
            from_status_id,
            to_project_id,
            to_source_id,
            to_status_id,
            date_type,
            from_date,
            to_date,
            len(lead_ids),
            actor_id
        ))

        transfer_id = cursor.lastrowid
        conn.commit()

        if to_emp_id != actor_id:
            create_notification(
                to_emp_id,
                "Lead Transfer",
                f"{len(lead_ids)} lead(s) have been transferred to you",
                "Leads",
                lead_ids[0]
            )

        return {
            "transfer_id": transfer_id,
            "lead_count": len(lead_ids),
            "lead_ids": lead_ids
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_lead_transfer_history():
    conn = get_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        _ensure_transfer_log_table(cursor)
        cursor.execute("""
            SELECT
                t.transfer_id,
                t.lead_count,
                t.created_on,
                t.from_emp_id,
                t.to_emp_id,
                t.created_by,
                t.date_type,
                t.from_date,
                t.to_date,
                TRIM(CONCAT(f.emp_first_name, ' ', IFNULL(f.emp_last_name, ''))) AS from_employee_name,
                TRIM(CONCAT(te.emp_first_name, ' ', IFNULL(te.emp_last_name, ''))) AS to_employee_name,
                TRIM(CONCAT(cb.emp_first_name, ' ', IFNULL(cb.emp_last_name, ''))) AS created_by_name,
                fp.project_name AS from_project_name,
                fs.source_name AS from_source_name,
                fls.status_name AS from_status_name,
                tp.project_name AS to_project_name,
                ts.source_name AS to_source_name,
                tls.status_name AS to_status_name
            FROM lead_transfer_log t
            LEFT JOIN employee f ON t.from_emp_id = f.emp_id
            LEFT JOIN employee te ON t.to_emp_id = te.emp_id
            LEFT JOIN employee cb ON t.created_by = cb.emp_id
            LEFT JOIN project_registration fp ON t.from_project_id = fp.project_id
            LEFT JOIN lead_sources fs ON t.from_source_id = fs.source_id
            LEFT JOIN lead_status fls ON t.from_status_id = fls.status_id
            LEFT JOIN project_registration tp ON t.to_project_id = tp.project_id
            LEFT JOIN lead_sources ts ON t.to_source_id = ts.source_id
            LEFT JOIN lead_status tls ON t.to_status_id = tls.status_id
            ORDER BY t.created_on DESC
        """)
        return cursor.fetchall()
    finally:
        conn.close()
