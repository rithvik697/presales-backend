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
                COALESCE(e.emp_first_name, 'Unassigned') AS user_name,
                COUNT(l.lead_id) AS total_assigned,
                SUM(CASE WHEN ls.status_name = 'Site Visit Done' THEN 1 ELSE 0 END) AS site_visit_done,
                SUM(CASE WHEN ls.status_name = 'Office Visit Done' THEN 1 ELSE 0 END) AS office_visit_done,
                SUM(CASE WHEN ls.status_name NOT IN ('Site Visit Done', 'Office Visit Done', 'Deal Closed', 'Spam', 'Low Budget', 'OOS', 'Old Lead', 'Not Answered', 'Not Interested') THEN 1 ELSE 0 END) AS pipeline,
                SUM(CASE WHEN ls.status_name = 'Deal Closed' THEN 1 ELSE 0 END) AS deals_closed,
                SUM(CASE WHEN ls.status_name IN ('Spam', 'Low Budget', 'OOS', 'Old Lead', 'Not Answered', 'Not Interested') THEN 1 ELSE 0 END) AS spam

            FROM leads l
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN lead_status ls ON l.status_id = ls.status_id

            WHERE 1=1 {date_cond}

            GROUP BY e.emp_id, user_name
            ORDER BY user_name;
        """

        cursor.execute(query, tuple(params))
        result = cursor.fetchall()

        formatted_result = []
        for row in result:
            formatted_result.append({
                "emp_id": row.get("emp_id"),
                "user_name": row["user_name"],
                "total_assigned": int(row["total_assigned"] or 0),
                "site_visit_done": int(row["site_visit_done"] or 0),
                "office_visit_done": int(row["office_visit_done"] or 0),
                "pipeline": int(row["pipeline"] or 0),
                "deals_closed": int(row["deals_closed"] or 0),
                "spam": int(row["spam"] or 0)
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
            "total_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                WHERE 1=1 {date_cond}
                """,

            "active_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                JOIN lead_status s ON l.status_id = s.status_id
                WHERE s.status_name IN (
                    'New Enquiry', 'Phone Call', 'WhatsApp', 'Offline Lead', 'NRI',
                    'Expected Site Visit', 'Site Visit Done',
                    'Office Visit Done', 'Pipeline'
                ) {date_cond}
                """,

            "closed_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                JOIN lead_status s ON l.status_id = s.status_id
                WHERE s.status_name = 'Deal Closed' {date_cond}
                """,

            "lost_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                JOIN lead_status s ON l.status_id = s.status_id
                WHERE s.status_name IN ('Spam','Low Budget','OOS','Old Lead') {date_cond}
                """,

            "today_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                WHERE DATE(l.created_on) = CURDATE() {date_cond}
                """
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

def get_summary_leads(summary_type, start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
        
        if summary_type == 'Active':
            date_cond += " AND ls.status_name IN ('New Enquiry', 'Phone Call', 'WhatsApp', 'Offline Lead', 'NRI', 'Expected Site Visit', 'Site Visit Done', 'Office Visit Done', 'Pipeline')"
        elif summary_type == 'Closed':
            date_cond += " AND ls.status_name = 'Deal Closed'"
        elif summary_type == 'Lost':
            date_cond += " AND ls.status_name IN ('Spam','Low Budget','OOS','Old Lead')"
        elif summary_type == 'Today':
            date_cond += " AND DATE(l.created_on) = CURDATE()"
            
        query = f"""
            SELECT 
                l.lead_id,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as lead_name,
                l.lead_description,
                COALESCE(e.emp_first_name, 'Unassigned') as employee_name,
                COALESCE(p.project_name, 'Unknown') as project_name,
                ls.status_name as label,
                l.created_on
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            LEFT JOIN lead_status ls ON l.status_id = ls.status_id
            WHERE 1=1 {date_cond}
            ORDER BY l.created_on DESC
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        for row in result:
             if row.get('created_on'):
                 row['created_on'] = str(row['created_on'])

        return {"success": True, "data": result}
    except Exception as e:
        import traceback
        print(f"Error in get_summary_leads: {traceback.format_exc()}")
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
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as lead_name,
                l.lead_description, 
                e.emp_first_name, 
                p.project_name, 
                l.created_on
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            WHERE l.is_active = 1
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        return {"success": True, "columns": ["Lead ID", "Lead Name", "Description", "Employee", "Project", "Created On"], "data": rows}
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

def get_user_leads_export(emp_id, activity, start_date=None, end_date=None, project_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, emp_id)
        
        if activity == 'Site Visit Done':
            date_cond += " AND ls.status_name = 'Site Visit Done' "
        elif activity == 'Office Visit Done':
            date_cond += " AND ls.status_name = 'Office Visit Done' "
        elif activity == 'Deal Closed' or activity == 'Deals Closed':
            date_cond += " AND ls.status_name = 'Deal Closed' "
        elif activity == 'Pipeline':
            date_cond += " AND ls.status_name NOT IN ('Site Visit Done', 'Office Visit Done', 'Deal Closed', 'Spam', 'Low Budget', 'OOS', 'Old Lead', 'Not Answered', 'Not Interested') "
        elif activity == 'Spam':
            date_cond += " AND ls.status_name IN ('Spam', 'Low Budget', 'OOS', 'Old Lead', 'Not Answered', 'Not Interested') "
            
        query = f"""
            SELECT 
                l.lead_id,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as lead_name,
                l.lead_description,
                COALESCE(p.project_name, 'Unknown') as project_name,
                l.created_on,
                ls.status_name
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN lead_status ls ON l.status_id = ls.status_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            WHERE 1=1 {date_cond}
            ORDER BY l.created_on DESC
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        for row in result:
             if row.get('created_on'):
                 row['created_on'] = str(row['created_on'])

        return {"success": True, "data": result}
    except Exception as e:
        import traceback
        print(f"Error in get_user_leads_export: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_weekly_report_log(start_date=None, end_date=None, project_id=None, user_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id)
        if not (start_date and end_date):
            date_cond += " AND l.created_on >= DATE_SUB(NOW(), INTERVAL 7 DAY) "
            
        query = f"""
            SELECT 
                l.created_on,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as customer_name,
                COALESCE(p.project_name, 'Unknown') as project_name,
                COALESCE(ls.source_name, 'Web') as source_name,
                COALESCE(e.emp_first_name, 'Unassigned') as employee_name
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            LEFT JOIN lead_sources ls ON l.source_id = ls.source_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            WHERE 1=1 {date_cond}
            ORDER BY l.created_on DESC
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        for row in result:
             if row.get('created_on'):
                 row['created_on'] = str(row['created_on'])

        return {"success": True, "data": result}
    except Exception as e:
        import traceback
        print(f"Error in get_weekly_report_log: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_monthly_performance_report(target_month=None, target_year=None, project_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        # Build base filters
        base_cond = ""
        params = []
        if project_id:
            base_cond += " AND l.project_id = %s "
            params.append(project_id)
            
        import datetime
        now = datetime.datetime.now()
        year = int(target_year) if target_year else now.year
        month = int(target_month) if target_month else now.month
        
        curr_date = datetime.date(year, month, 1)
        import calendar
        prev_date = curr_date - datetime.timedelta(days=1)
        prev_month = prev_date.month
        prev_year = prev_date.year
        
        # Helper to run aggregates for a specific month/year
        def get_aggregates(m, y):
            cond = base_cond + f" AND MONTH(l.created_on) = {m} AND YEAR(l.created_on) = {y} "
            
            # --- OVERALL QUERIES ---
            # Total Leads Received
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l WHERE 1=1 {cond}", tuple(params))
            total_leads = cursor.fetchone()['cnt']
            
            # Test Leads
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_status ls ON l.status_id = ls.status_id WHERE 1=1 {cond} AND LOWER(ls.status_name) LIKE '%test%'", tuple(params))
            test_leads = cursor.fetchone()['cnt']
            
            # Site Visit Done
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_status ls ON l.status_id = ls.status_id WHERE 1=1 {cond} AND ls.status_name = 'Site Visit Done'", tuple(params))
            site_visits = cursor.fetchone()['cnt']
            
            # Not Enquired/Spam
            cursor.execute(f"""SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_status ls ON l.status_id = ls.status_id 
                               WHERE 1=1 {cond} AND ls.status_name IN ('Spam', 'Low Budget', 'OOS', 'Old Lead', 'Not Answered')""", tuple(params))
            spam = cursor.fetchone()['cnt']
            
            # Not Interested
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_status ls ON l.status_id = ls.status_id WHERE 1=1 {cond} AND ls.status_name = 'Not Interested'", tuple(params))
            not_interested = cursor.fetchone()['cnt']
            
            # Walk-ins (incl digital) (Assuming source name contains 'walk' or 'digital')
            cursor.execute(f"""SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_sources src ON l.source_id = src.source_id 
                               WHERE 1=1 {cond} AND (LOWER(src.source_name) LIKE '%walk-in%' OR LOWER(src.source_name) LIKE '%digital%')""", tuple(params))
            walkins = cursor.fetchone()['cnt']
            
            # mcube (IVR) (Assuming source contains mcube or ivr)
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_sources src ON l.source_id = src.source_id WHERE 1=1 {cond} AND (LOWER(src.source_name) LIKE '%mcube%' OR LOWER(src.source_name) LIKE '%ivr%')", tuple(params))
            mcube = cursor.fetchone()['cnt']
            
            # Deal Closed
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_status ls ON l.status_id = ls.status_id WHERE 1=1 {cond} AND ls.status_name = 'Deal Closed'", tuple(params))
            deal_closed = cursor.fetchone()['cnt']
            
            # Calls Attempted (Requires joining call_log with leads using project filter if needed)
            call_cond = ""
            call_params = []
            if project_id:
                call_cond += " AND l.project_id = %s "
                call_params.append(project_id)
                
            cursor.execute(f"""
                SELECT COUNT(*) as cnt 
                FROM call_log c 
                LEFT JOIN leads l ON c.lead_id = l.lead_id 
                WHERE MONTH(c.created_at) = {m} AND YEAR(c.created_at) = {y} {call_cond}
            """, tuple(call_params))
            calls_attempted = cursor.fetchone()['cnt']
            
            # --- INDIVIDUAL QUERIES ---
            cursor.execute(f"""
                SELECT e.emp_id, e.emp_first_name, 
                       COUNT(l.lead_id) as leads_received,
                       SUM(CASE WHEN ls.status_name = 'Site Visit Done' THEN 1 ELSE 0 END) as site_visits,
                       SUM(CASE WHEN ls.status_name = 'Deal Closed' THEN 1 ELSE 0 END) as deals_closed
                FROM employee e
                LEFT JOIN leads l ON e.emp_id = l.emp_id AND MONTH(l.created_on) = {m} AND YEAR(l.created_on) = {y}
                LEFT JOIN lead_status ls ON l.status_id = ls.status_id
                WHERE e.emp_status = 'Active'
                GROUP BY e.emp_id
            """)
            emp_stats = cursor.fetchall()
            
            call_stats_query = f"""
                SELECT e.emp_id, COUNT(c.call_id) as calls_attempted
                FROM employee e
                LEFT JOIN call_log c ON e.emp_id = c.emp_id AND MONTH(c.created_at) = {m} AND YEAR(c.created_at) = {y}
                LEFT JOIN leads l ON c.lead_id = l.lead_id
                WHERE e.emp_status = 'Active' {call_cond}
                GROUP BY e.emp_id
            """
            cursor.execute(call_stats_query, tuple(call_params))
            emp_call_stats = {row['emp_id']: row['calls_attempted'] for row in cursor.fetchall()}
            
            # Merge individual stats
            individuals = {}
            for stat in emp_stats:
                emp_id = stat['emp_id']
                if not emp_id: continue
                # filter out admins/superadmins if needed, for now returning all active users with role
                individuals[emp_id] = {
                    "name": str(stat['emp_first_name']).upper(),
                    "leads_received": stat['leads_received'] or 0,
                    "site_visits": stat['site_visits'] or 0,
                    "deals_closed": stat['deals_closed'] or 0,
                    "calls_attempted": emp_call_stats.get(emp_id, 0)
                }

            return {
                "overall": {
                    "leads_received": total_leads,
                    "test_leads": test_leads,
                    "site_visits": site_visits,
                    "spam": spam,
                    "not_interested": not_interested,
                    "walkins": walkins,
                    "mcube": mcube,
                    "calls_attempted": calls_attempted,
                    "deal_closed": deal_closed
                },
                "individuals": individuals
            }

        curr_data = get_aggregates(month, year)
        prev_data = get_aggregates(prev_month, prev_year)
        
        # Calculate highlights from curr_data only
        highest_calls = {"name": "", "count": -1}
        lowest_calls = {"name": "", "count": float('inf')}
        highest_visits = {"name": "", "count": -1}
        lowest_visits = {"name": "", "count": float('inf')}
        
        for emp_id, data in curr_data['individuals'].items():
            # Skip users with absolute zero interactions to prevent pure empty accounts skewing lowest stats
            if data['leads_received'] == 0 and data['calls_attempted'] == 0:
                continue
                
            if data['calls_attempted'] > highest_calls['count']:
                highest_calls = {"name": data['name'], "count": data['calls_attempted']}
            if data['calls_attempted'] < lowest_calls['count']:
                lowest_calls = {"name": data['name'], "count": data['calls_attempted']}
                
            if data['site_visits'] > highest_visits['count']:
                highest_visits = {"name": data['name'], "count": data['site_visits']}
            if data['site_visits'] < lowest_visits['count']:
                lowest_visits = {"name": data['name'], "count": data['site_visits']}

        # Fallbacks
        if lowest_calls['count'] == float('inf'): lowest_calls['count'] = 0
        if lowest_visits['count'] == float('inf'): lowest_visits['count'] = 0

        result = {
            "month_name": calendar.month_name[month],
            "year": year,
            "prev_month_name": calendar.month_name[prev_month],
            "current": curr_data,
            "previous": prev_data,
            "highlights": {
                "highest_calls": highest_calls,
                "lowest_calls": lowest_calls,
                "highest_visits": highest_visits,
                "lowest_visits": lowest_visits
            }
        }
        
        return {"success": True, "data": result}
    except Exception as e:
        import traceback
        print(f"Error in get_monthly_performance_report: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
