from db import get_db
from services.audit_service import log_audit


def _ensure_mapping_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee_project_mapping (
            mapping_id INT AUTO_INCREMENT PRIMARY KEY,
            emp_id VARCHAR(150) NOT NULL,
            project_id VARCHAR(150) NOT NULL,
            created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(150) NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            UNIQUE KEY uq_employee_project (emp_id, project_id),
            CONSTRAINT fk_employee_project_mapping_emp
                FOREIGN KEY (emp_id) REFERENCES employee(emp_id),
            CONSTRAINT fk_employee_project_mapping_project
                FOREIGN KEY (project_id) REFERENCES project_registration(project_id)
        )
    """)


def get_project_assignments():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        _ensure_mapping_table(cursor)
        cursor.execute("""
            SELECT
                m.mapping_id,
                m.emp_id,
                m.project_id,
                m.created_on,
                m.created_by,
                TRIM(CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, ''))) AS employee_name,
                p.project_name
            FROM employee_project_mapping m
            JOIN employee e ON m.emp_id = e.emp_id
            JOIN project_registration p ON m.project_id = p.project_id
            WHERE m.is_active = 1
            ORDER BY p.project_name ASC, employee_name ASC
        """)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def create_project_assignment(project_id, emp_id, created_by):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        _ensure_mapping_table(cursor)

        cursor.execute("""
            SELECT emp_id, emp_status, role_id
            FROM employee
            WHERE emp_id = %s
        """, (emp_id,))
        employee = cursor.fetchone()

        if not employee:
            raise ValueError("Employee not found")
        if employee["emp_status"] != "Active":
            raise ValueError("Only active employees can be assigned to a project")
        if employee["role_id"] != "SALES_EXEC":
            raise ValueError("Only sales executives can be assigned to projects")

        cursor.execute("""
            SELECT project_id
            FROM project_registration
            WHERE project_id = %s
        """, (project_id,))
        project = cursor.fetchone()

        if not project:
            raise ValueError("Project not found")

        cursor.execute("""
            SELECT mapping_id, is_active
            FROM employee_project_mapping
            WHERE emp_id = %s AND project_id = %s
        """, (emp_id, project_id))
        existing = cursor.fetchone()

        if existing and existing["is_active"] == 1:
            raise ValueError("This employee is already mapped to the selected project")

        if existing and existing["is_active"] == 0:
            cursor.execute("""
                UPDATE employee_project_mapping
                SET is_active = 1,
                    created_by = %s,
                    created_on = NOW()
                WHERE mapping_id = %s
            """, (created_by, existing["mapping_id"]))
            mapping_id = existing["mapping_id"]
        else:
            cursor.execute("""
                INSERT INTO employee_project_mapping
                    (emp_id, project_id, created_by, is_active)
                VALUES (%s, %s, %s, 1)
            """, (emp_id, project_id, created_by))
            mapping_id = cursor.lastrowid

        conn.commit()

        log_audit(
            object_name="employee_project_mapping",
            object_id=str(mapping_id),
            property_name="PROJECT_ASSIGNMENT_CREATED",
            old_value=None,
            new_value=f"{emp_id}:{project_id}",
            modified_by=created_by,
            action_type="INSERT"
        )

        cursor.execute("""
            SELECT
                m.mapping_id,
                m.emp_id,
                m.project_id,
                m.created_on,
                m.created_by,
                TRIM(CONCAT(e.emp_first_name, ' ', IFNULL(e.emp_last_name, ''))) AS employee_name,
                p.project_name
            FROM employee_project_mapping m
            JOIN employee e ON m.emp_id = e.emp_id
            JOIN project_registration p ON m.project_id = p.project_id
            WHERE m.mapping_id = %s
        """, (mapping_id,))
        return cursor.fetchone()

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def delete_project_assignment(mapping_id, deleted_by):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    try:
        _ensure_mapping_table(cursor)
        cursor.execute("""
            SELECT mapping_id, emp_id, project_id, is_active
            FROM employee_project_mapping
            WHERE mapping_id = %s
        """, (mapping_id,))
        mapping = cursor.fetchone()

        if not mapping:
            return False
        if mapping["is_active"] == 0:
            return True

        cursor.execute("""
            UPDATE employee_project_mapping
            SET is_active = 0
            WHERE mapping_id = %s
        """, (mapping_id,))
        conn.commit()

        log_audit(
            object_name="employee_project_mapping",
            object_id=str(mapping_id),
            property_name="PROJECT_ASSIGNMENT_DELETED",
            old_value=f"{mapping['emp_id']}:{mapping['project_id']}",
            new_value=None,
            modified_by=deleted_by,
            action_type="DELETE"
        )

        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
