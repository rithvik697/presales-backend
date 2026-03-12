from db import get_db
import traceback

def build_filters(start_date, end_date, project_id=None, user_id=None):
    condition = ""
    params = []
    if start_date and end_date:
        condition += " AND l.created_on BETWEEN %s AND %s "
        # Adjust end_date to end of day if it's just a date
        end_date_time = f"{end_date} 23:59:59" if len(end_date) == 10 else end_date
        params.extend([start_date, end_date_time])
        
    if project_id:
        condition += " AND l.project_id = %s "
        params.append(project_id)
        
    if user_id:
        condition += " AND l.emp_id = %s "
        params.append(user_id)
        
    return condition, params

def get_weekly_leads(start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
        if not (start_date and end_date):
            date_cond += " AND l.created_on >= DATE_SUB(NOW(), INTERVAL 7 DAY) "
        
        query = f"""
            SELECT DATE(l.created_on) as date, COUNT(*) as leads
            FROM leads l
            WHERE 1=1 {date_cond}
            GROUP BY DATE(l.created_on)
            ORDER BY date;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        for row in result:
            if row.get('date'):
                row['date'] = str(row['date'])
                
        return {"success": True, "data": result}
    except Exception as e:
        print(f"Error in get_weekly_leads: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_monthly_leads(start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
        if not (start_date and end_date):
            date_cond += " AND l.created_on >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
            
        query = f"""
            SELECT DATE(l.created_on) as date, COUNT(*) as leads
            FROM leads l
            WHERE 1=1 {date_cond}
            GROUP BY DATE(l.created_on)
            ORDER BY date;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        for row in result:
            if row.get('date'):
                row['date'] = str(row['date'])
                
        return {"success": True, "data": result}
    except Exception as e:
        print(f"Error in get_monthly_leads: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_annual_leads(start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
        if not (start_date and end_date):
            date_cond += " AND YEAR(l.created_on) = YEAR(CURDATE()) "
            
        query = f"""
            SELECT MONTH(l.created_on) as month, COUNT(*) as leads
            FROM leads l
            WHERE 1=1 {date_cond}
            GROUP BY MONTH(l.created_on)
            ORDER BY month;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        return {"success": True, "data": result}
    except Exception as e:
        print(f"Error in get_annual_leads: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_leads_by_status(start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
            
        query = f"""
            SELECT s.status_name, COUNT(l.lead_id) as leads
            FROM leads l
            LEFT JOIN lead_status s ON l.status_id = s.status_id
            WHERE 1=1 {date_cond}
            GROUP BY s.status_name;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        return {"success": True, "data": result}
    except Exception as e:
        print(f"Error in get_leads_by_status: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_user_performance(start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
        
        query = f"""
            SELECT 
                e.emp_id,
                COALESCE(e.emp_first_name, 'Unassigned') as user_name,
                SUM(CASE WHEN ls.status_name = 'Converted' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE 
                    WHEN ls.status_name IN ('New Enquiry', 'Follow-up') 
                    THEN 1 ELSE 0 
                END) AS ongoing,
                SUM(CASE 
                    WHEN ls.status_name IN ('Spam', 'Low Budget', 'OOS', 'Old Lead') 
                    THEN 1 ELSE 0 
                END) AS failed
            FROM leads l
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN lead_status ls ON l.status_id = ls.status_id
            WHERE 1=1 {date_cond}
            GROUP BY e.emp_id, COALESCE(e.emp_first_name, 'Unassigned');
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        # Format the result to guarantee integers and handle nulls
        formatted_result = []
        for row in result:
            formatted_result.append({
                "emp_id": row.get("emp_id"),
                "user_name": row["user_name"],
                "completed": int(row["completed"] or 0),
                "ongoing": int(row["ongoing"] or 0),
                "failed": int(row["failed"] or 0)
            })
            
        return {"success": True, "data": formatted_result}
    except Exception as e:
        print(f"Error in get_user_performance: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_reports_summary(start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
        
        queries = {
            "total_leads": f"SELECT COUNT(*) as count FROM leads l WHERE 1=1 {date_cond}",
            "active_leads": f"SELECT COUNT(*) as count FROM leads l WHERE l.is_active = 1 {date_cond}",
            "closed_leads": f"SELECT COUNT(*) as count FROM leads l JOIN lead_status s ON l.status_id = s.status_id WHERE s.status_name = 'Converted' {date_cond}",
            "lost_leads": f"SELECT COUNT(*) as count FROM leads l JOIN lead_status s ON l.status_id = s.status_id WHERE s.status_name = 'Lost' {date_cond}",
            "today_leads": f"SELECT COUNT(*) as count FROM leads l WHERE DATE(l.created_on) = CURDATE() {date_cond}"
        }
        
        summary = {}
        for key, q in queries.items():
            cursor.execute(q, tuple(params))
            res = cursor.fetchone()
            summary[key] = res['count'] if res else 0
            
        return {"success": True, "data": summary}
    except Exception as e:
        print(f"Error in get_reports_summary: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_active_leads_for_download():
    # We return a tuple representing (columns, rows) where rows is an iterable/generator
    try:
        conn = get_db()
        cursor = conn.cursor()
        query = """
            SELECT 
                l.lead_id, 
                l.lead_description, 
                e.emp_first_name, 
                p.project_name, 
                l.created_on
            FROM leads l
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            WHERE l.is_active = 1
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        return {"success": True, "columns": ["Lead ID", "Description", "Employee", "Project", "Created On"], "data": rows}
    except Exception as e:
        print(f"Error in get_active_leads_for_download: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_daily_log(project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        # We only pass None for dates since we force today's date in query
        date_cond, params = build_filters(None, None, project_id, user_id)
        
        query = f"""
            SELECT 
                l.lead_id,
                l.lead_description,
                l.created_on,
                COALESCE(e.emp_first_name, 'Unassigned') as employee_name,
                COALESCE(p.project_name, 'Unknown') as project_name,
                ls.status_name as label
            FROM leads l
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            LEFT JOIN lead_status ls ON l.status_id = ls.status_id
            WHERE DATE(l.created_on) = CURDATE() {date_cond}
            ORDER BY l.created_on DESC
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        # Format the date explicitly
        for row in result:
             if row.get('created_on'):
                 row['created_on'] = str(row['created_on'])

        return {"success": True, "data": result}
    except Exception as e:
        import traceback
        print(f"Error in get_daily_log: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
