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

            status = data.get("status")
            rera_number = data.get("rera_number")
            ALLOWED_PROJECT_TYPES = ["Villa", "Apartment"]

            project_type = data.get("project_type")

            if project_type not in ALLOWED_PROJECT_TYPES:
                raise ValueError("Invalid project type")

            # Allowed ENUM values (must match DB ENUM exactly)
            ALLOWED_STATUSES = ["RERA_APPROVED", "COMPLETED", "PRE_LAUNCH"]

            if status not in ALLOWED_STATUSES:
                raise ValueError("Invalid project status")

            # Conditional RERA rule
            if status in ["RERA_APPROVED", "COMPLETED"] and not rera_number:
                raise ValueError("RERA number is required for approved or completed projects")
            


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
                data.get("status"),
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
            # 🚨 Prevent status update from this API
            if "status" in data:
                raise ValueError("Use update_project_status API to update status")

            # 🚨 Validate project_type if being updated
            ALLOWED_PROJECT_TYPES = ["Villa", "Apartment"]
            if "project_type" in data:
                if data["project_type"] not in ALLOWED_PROJECT_TYPES:
                    raise ValueError("Invalid project type")
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
     cursor = db.cursor(dictionary=True)
     try:
        ALLOWED_STATUSES = ["RERA_APPROVED", "COMPLETED", "PRE_LAUNCH"]

        if status not in ALLOWED_STATUSES:
            raise ValueError("Invalid project status")

        # Fetch existing project
        cursor.execute("""
            SELECT rera_number
            FROM project_registration
            WHERE project_id = %s
        """, (project_id,))
        project = cursor.fetchone()

        if not project:
            raise ValueError("Project not found")

        # Enforce RERA rule
        if status in ["RERA_APPROVED", "COMPLETED"] and not project["rera_number"]:
            raise ValueError("RERA number required before approving/completing project")

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