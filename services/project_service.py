from db import get_db
import uuid
import logging

logger = logging.getLogger(__name__)

class ProjectService:

    # -----------------------------
    # Create Project
    # -----------------------------
    def create_project(self, data):
        db = get_db()
        cursor = db.cursor()
        try:
            project_id = data.get("project_id") or str(uuid.uuid4())

            sql = """
                INSERT INTO project_registration (
                    project_id,
                    project_name,
                    project_type,
                    location,
                    address_line_1,
                    city,
                    state,
                    pincode,
                    total_area,
                    number_of_units,
                    rera_number,
                    status,
                    created_by,
                    created_on
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """

            values = (
                project_id,
                data.get("project_name"),
                data.get("project_type"),
                data.get("location"),
                data.get("address_line_1"),
                data.get("city"),
                data.get("state"),
                data.get("pincode"),
                data.get("total_area"),
                data.get("number_of_units"),
                data.get("rera_number"),
                data.get("status", "NEW"),
                data.get("created_by")
            )

            cursor.execute(sql, values)
            db.commit()

            logger.info(f"Project {project_id} created")
            return project_id

        except Exception as e:
            db.rollback()
            logger.error(e)
            raise
        finally:
            db.close()

    # -----------------------------
    # Get All Projects (LIST VIEW)
    # -----------------------------
    def get_all_projects(self):
        """
        Used for listing projects (dashboard / table view)
        """
        db = get_db()
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT
                project_id,
                project_name,
                project_type,
                location,
                city,
                state,
                status,
                created_on
            FROM project_registration
            ORDER BY created_on DESC
        """)

        projects = cursor.fetchall()
        db.close()
        return projects

    # -----------------------------
    # Get Project By ID (DETAIL VIEW)
    # -----------------------------
    def get_project_by_id(self, project_id):
        db = get_db()
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT *
            FROM project_registration
            WHERE project_id = %s
        """, (project_id,))

        project = cursor.fetchone()
        db.close()

        return project  # controller handles 404

    # -----------------------------
    # Update Project
    # -----------------------------
    def update_project(self, project_id, data):
        db = get_db()
        cursor = db.cursor()
        try:
            fields = []
            values = []

            for key, value in data.items():
                fields.append(f"{key} = %s")
                values.append(value)

            if not fields:
                return {"message": "Nothing to update"}

            sql = f"""
                UPDATE project_registration
                SET {", ".join(fields)}, modified_on = NOW()
                WHERE project_id = %s
            """

            values.append(project_id)
            cursor.execute(sql, values)
            db.commit()

            return {"message": "Project updated successfully"}

        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    # -----------------------------
    # Update Project Status
    # -----------------------------
    def update_project_status(self, project_id, status):
        db = get_db()
        cursor = db.cursor()
        try:
            cursor.execute("""
                UPDATE project_registration
                SET status = %s, modified_on = NOW()
                WHERE project_id = %s
            """, (status, project_id))

            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


project_service = ProjectService()
