from db import get_db
import traceback

def build_filters(start_date, end_date, project_id=None, user_id=None, source_id=None, status_id=None):
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

    if source_id:
        condition += " AND l.source_id = %s "
        params.append(source_id)

    if status_id:
        condition += " AND l.status_id = %s "
        params.append(status_id)
        
    return condition, params

def get_weekly_leads(start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        import datetime
        now = datetime.datetime.now().date()
        
        if start_date and end_date:
            date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)
            start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        else:
            # Current Week (Monday to Sunday)
            start = now - datetime.timedelta(days=now.weekday())
            end = start + datetime.timedelta(days=6)
            date_cond, params = build_filters(str(start), str(end), project_id, user_id, source_id, status_id)
        
        query = f"""
            SELECT DATE(l.created_on) as date, COUNT(*) as leads
            FROM leads l
            WHERE 1=1 {date_cond}
            GROUP BY DATE(l.created_on)
            ORDER BY date;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        # Padding
        data_map = {str(row['date']): row['leads'] for row in result if row.get('date')}
        full_result = []
        curr = start
        while curr <= end:
            curr_str = str(curr)
            full_result.append({
                "date": curr_str,
                "leads": data_map.get(curr_str, 0)
            })
            curr += datetime.timedelta(days=1)
                
        return {"success": True, "data": full_result}
    except Exception as e:
        print(f"Error in get_weekly_leads: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_daily_leads_hourly(project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        # Today's date filter
        date_cond = " AND DATE(l.created_on) = CURDATE() "
        params = []
        
        if project_id:
            date_cond += " AND l.project_id = %s "
            params.append(project_id)
        if user_id:
            date_cond += " AND l.emp_id = %s "
            params.append(user_id)
        if source_id:
            date_cond += " AND l.source_id = %s "
            params.append(source_id)
        if status_id:
            date_cond += " AND l.status_id = %s "
            params.append(status_id)
            
        query = f"""
            SELECT HOUR(l.created_on) as hour, COUNT(*) as leads
            FROM leads l
            WHERE 1=1 {date_cond}
            GROUP BY HOUR(l.created_on)
            ORDER BY hour;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        # Fill in missing hours
        full_result = []
        hours_map = {row['hour']: row['leads'] for row in result}
        for h in range(24):
            full_result.append({
                "hour": f"{h:02d}:00",
                "leads": hours_map.get(h, 0)
            })
            
        return {"success": True, "data": full_result}
    except Exception as e:
        print(f"Error in get_daily_leads_hourly: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'conn' in locals() and conn: conn.close()

def get_monthly_leads(start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        import datetime
        now = datetime.datetime.now().date()
        
        if start_date and end_date:
            date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)
            start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        else:
            # Full Calendar Month
            start = now.replace(day=1)
            import calendar
            _, last_day = calendar.monthrange(now.year, now.month)
            end = now.replace(day=last_day)
            date_cond, params = build_filters(str(start), str(end), project_id, user_id, source_id, status_id)
            
        query = f"""
            SELECT DATE(l.created_on) as date, COUNT(*) as leads
            FROM leads l
            WHERE 1=1 {date_cond}
            GROUP BY DATE(l.created_on)
            ORDER BY date;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        # Padding
        data_map = {str(row['date']): row['leads'] for row in result if row.get('date')}
        full_result = []
        curr = start
        while curr <= end:
            curr_str = str(curr)
            full_result.append({
                "date": curr_str,
                "leads": data_map.get(curr_str, 0)
            })
            curr += datetime.timedelta(days=1)
            
        return {"success": True, "data": full_result}
    except Exception as e:
        print(f"Error in get_monthly_leads: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_annual_leads(start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        import datetime
        now = datetime.datetime.now().date()
        
        if start_date and end_date:
            date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)
            fy_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        else:
            # Financial Year (April 1st to March 31st)
            if now.month < 4:
                fy_start_year = now.year - 1
            else:
                fy_start_year = now.year
            fy_start_date = datetime.date(fy_start_year, 4, 1)
            fy_end_date = datetime.date(fy_start_year + 1, 3, 31)
            date_cond, params = build_filters(str(fy_start_date), str(fy_end_date), project_id, user_id, source_id, status_id)
            
        query = f"""
            SELECT MONTH(l.created_on) as month, YEAR(l.created_on) as year, COUNT(*) as leads
            FROM leads l
            WHERE 1=1 {date_cond}
            GROUP BY YEAR(l.created_on), MONTH(l.created_on)
            ORDER BY year, month;
        """
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
        
        # Build 12-month map based on (month, year)
        data_map = {(row['month'], row['year']): row['leads'] for row in result}
        
        full_result = []
        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        # Starts from April of fy_start_year
        curr_month = 4
        curr_year = fy_start_date.year
        for _ in range(12):
            leads = data_map.get((curr_month, curr_year), 0)
            full_result.append({
                "month": month_names[curr_month-1],
                "year": curr_year,
                "leads": leads
            })
            curr_month += 1
            if curr_month > 12:
                curr_month = 1
                curr_year += 1
            
        return {"success": True, "data": full_result}
    except Exception as e:
        print(f"Error in get_annual_leads: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_leads_by_status(start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)
            
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

def get_user_performance(start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)

        # Query ALL statuses from current leads (for general status counts)
        query = f"""
            SELECT 
                e.emp_id,
                COALESCE(e.emp_first_name, 'Unassigned') AS user_name,
                ls.status_name,
                COUNT(l.lead_id) as count
            FROM leads l
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN lead_status ls ON l.status_id = ls.status_id
            WHERE 1=1 {date_cond}
            GROUP BY e.emp_id, user_name, ls.status_name
            ORDER BY user_name;
        """

        cursor.execute(query, tuple(params))
        result = cursor.fetchall()

        # Group data by employee
        performance_map = {}
        for row in result:
            emp_id = row['emp_id']
            if emp_id not in performance_map:
                performance_map[emp_id] = {
                    "emp_id": emp_id,
                    "user_name": row['user_name'],
                    "total_assigned": 0,
                    "status_counts": {}
                }
            
            status_name = row['status_name'] or 'Unknown'
            count = int(row['count'] or 0)
            performance_map[emp_id]["status_counts"][status_name] = count
            performance_map[emp_id]["total_assigned"] += count

        # ─── IMMUTABLE HISTORICAL COUNTS ────────────────────────────────────────
        # Site Visit Done and Deal Closed are queried from lead_status_history
        # so they remain accurate even after the lead's status is changed later.
        history_date_cond = "WHERE 1=1"
        history_params = []
        if start_date:
            history_date_cond += " AND DATE(h.changed_at) >= %s"
            history_params.append(start_date)
        if end_date:
            history_date_cond += " AND DATE(h.changed_at) <= %s"
            history_params.append(end_date)
        if user_id:
            history_date_cond += " AND l.emp_id = %s"
            history_params.append(user_id)
        if project_id:
            history_date_cond += " AND l.project_id = %s"
            history_params.append(project_id)
        if source_id:
            history_date_cond += " AND l.source_id = %s"
            history_params.append(source_id)

        history_query = f"""
            SELECT
                l.emp_id,
                COALESCE(e.emp_first_name, 'Unassigned') AS user_name,
                ns.status_name,
                COUNT(DISTINCT h.lead_id) AS count
            FROM lead_status_history h
            JOIN leads l ON h.lead_id = l.lead_id
            JOIN lead_status ns ON h.new_status_id = ns.status_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            {history_date_cond}
            AND ns.status_name IN ('Site Visit Done', 'Deal Closed')
            GROUP BY l.emp_id, user_name, ns.status_name
        """
        cursor.execute(history_query, tuple(history_params))
        history_rows = cursor.fetchall()

        for row in history_rows:
            emp_id = row['emp_id']
            status_name = row['status_name']
            count = int(row['count'] or 0)
            if emp_id not in performance_map:
                performance_map[emp_id] = {
                    "emp_id": emp_id,
                    "user_name": row['user_name'],
                    "total_assigned": 0,
                    "status_counts": {}
                }
            # Override with immutable history-based count
            performance_map[emp_id]["status_counts"][status_name] = count
        # ────────────────────────────────────────────────────────────────────────

        return {"success": True, "data": list(performance_map.values())}

    except Exception as e:
        print(f"Error in get_user_performance: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_fy_date_range():
    """Returns (fy_start, today) strings for the current Financial Year (Apr 1)."""
    import datetime
    today = datetime.date.today()
    fy_start_year = today.year if today.month >= 4 else today.year - 1
    fy_start = datetime.date(fy_start_year, 4, 1)
    return str(fy_start), str(today)


def get_reports_summary(start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        # Default to Financial Year if no dates given (REMOVED)

        date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)

        # ── Immutable Deal Closed count from lead_status_history ──────────────
        # Build parallel conditions for the history table (uses changed_at, not created_on)
        hist_cond = " WHERE 1=1"
        hist_params = []
        if start_date:
            hist_cond += " AND DATE(h.changed_at) >= %s"
            hist_params.append(start_date)
        if end_date:
            end_date_time = f"{end_date} 23:59:59" if len(end_date) == 10 else end_date
            hist_cond += " AND h.changed_at <= %s"
            hist_params.append(end_date_time)
        if project_id:
            hist_cond += " AND l.project_id = %s"
            hist_params.append(project_id)
        if user_id:
            hist_cond += " AND l.emp_id = %s"
            hist_params.append(user_id)
        if source_id:
            hist_cond += " AND l.source_id = %s"
            hist_params.append(source_id)

        cursor.execute(f"""
            SELECT COUNT(DISTINCT h.lead_id) as count
            FROM lead_status_history h
            JOIN leads l ON h.lead_id = l.lead_id
            JOIN lead_status ns ON h.new_status_id = ns.status_id
            {hist_cond}
            AND ns.status_name = 'Deal Closed'
        """, tuple(hist_params))
        closed_leads_count = (cursor.fetchone() or {}).get('count', 0)
        # ─────────────────────────────────────────────────────────────────────

        queries = {
            "total_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                JOIN lead_status s ON l.status_id = s.status_id
                WHERE s.status_name NOT IN ('Spam', 'Testing', 'Not interested') {date_cond}
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

            "lost_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                JOIN lead_status s ON l.status_id = s.status_id
                WHERE s.status_name IN ('Spam','Low Budget','OOS','Old Lead') {date_cond}
                """,

            "today_leads": f"""
                SELECT COUNT(*) as count
                FROM leads l
                JOIN lead_status s ON l.status_id = s.status_id
                WHERE DATE(l.created_on) = CURDATE()
                AND s.status_name NOT IN ('Spam', 'Testing', 'Not interested') {date_cond}
                """
        }

        summary = {}
        for key, q in queries.items():
            cursor.execute(q, tuple(params))
            res = cursor.fetchone()
            summary[key] = res['count'] if res else 0

        # Inject immutable closed count
        summary['closed_leads'] = closed_leads_count

        return {"success": True, "data": summary}
    except Exception as e:
        print(f"Error in get_reports_summary: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_summary_leads(summary_type, start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        # Default to Financial Year if no dates given (REMOVED)

        # ── 'Closed' uses lead_status_history for immutable deal-closed records ──
        if summary_type == 'Closed':
            hist_cond = "WHERE 1=1"
            hist_params = []
            if start_date:
                hist_cond += " AND DATE(h.changed_at) >= %s"
                hist_params.append(start_date)
            if end_date:
                end_date_time = f"{end_date} 23:59:59" if len(end_date) == 10 else end_date
                hist_cond += " AND h.changed_at <= %s"
                hist_params.append(end_date_time)
            if project_id:
                hist_cond += " AND l.project_id = %s"
                hist_params.append(project_id)
            if user_id:
                hist_cond += " AND l.emp_id = %s"
                hist_params.append(user_id)
            if source_id:
                hist_cond += " AND l.source_id = %s"
                hist_params.append(source_id)

            query = f"""
                SELECT
                    h.lead_id,
                    CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) AS lead_name,
                    l.lead_description,
                    COALESCE(e.emp_first_name, 'Unassigned') AS employee_name,
                    COALESCE(p.project_name, 'Unknown') AS project_name,
                    'Deal Closed' AS label,
                    h.changed_at AS created_on,
                    curr_s.status_name AS current_status
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                LEFT JOIN lead_status curr_s ON l.status_id = curr_s.status_id
                LEFT JOIN customer c ON l.customer_id = c.customer_id
                LEFT JOIN employee e ON l.emp_id = e.emp_id
                LEFT JOIN project_registration p ON l.project_id = p.project_id
                {hist_cond}
                AND ns.status_name = 'Deal Closed'
                ORDER BY h.changed_at DESC
            """
            cursor.execute(query, tuple(hist_params))
            result = cursor.fetchall()
            for row in result:
                if row.get('created_on'):
                    row['created_on'] = str(row['created_on'])
            return {"success": True, "data": result}
        # ─────────────────────────────────────────────────────────────────────

        date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)

        if summary_type == 'Active':
            date_cond += " AND ls.status_name IN ('New Enquiry', 'Phone Call', 'WhatsApp', 'Offline Lead', 'NRI', 'Expected Site Visit', 'Site Visit Done', 'Office Visit Done', 'Pipeline')"
        elif summary_type == 'Lost':
            date_cond += " AND ls.status_name IN ('Spam','Low Budget','OOS','Old Lead')"
        elif summary_type == 'Today' or summary_type == 'Total':
            date_cond += " AND ls.status_name NOT IN ('Spam', 'Testing', 'Not interested')"
            if summary_type == 'Today':
                date_cond += " AND DATE(l.created_on) = CURDATE()"

        query = f"""
            SELECT
                l.lead_id,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as lead_name,
                l.lead_description,
                COALESCE(e.emp_first_name, 'Unassigned') as employee_name,
                COALESCE(p.project_name, 'Unknown') as project_name,
                ls.status_name as label,
                ls.status_name as current_status,
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

def get_daily_log(project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        # We only pass None for dates since we force today's date in query
        date_cond, params = build_filters(None, None, project_id, user_id, source_id, status_id)
        
        query = f"""
            SELECT 
                l.lead_id,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as lead_name,
                l.created_on,
                COALESCE(e.emp_first_name, 'Unassigned') as employee_name,
                COALESCE(p.project_name, 'Unknown') as project_name,
                ls.status_name as label
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
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

def get_user_leads_export(emp_id, activity, start_date=None, end_date=None, project_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        date_cond, params = build_filters(start_date, end_date, project_id, emp_id, source_id, status_id)
        
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

def get_weekly_report_log(start_date=None, end_date=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        import datetime
        now = datetime.datetime.now().date()
        
        if start_date and end_date:
            date_cond, params = build_filters(start_date, end_date, project_id, user_id, source_id, status_id)
        else:
            # Current Week (Monday to Sunday)
            start = now - datetime.timedelta(days=now.weekday())
            end = start + datetime.timedelta(days=6)
            date_cond, params = build_filters(str(start), str(end), project_id, user_id, source_id, status_id)
            
        query = f"""
            SELECT 
                l.created_on,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as customer_name,
                COALESCE(p.project_name, 'Unknown') as project_name,
                COALESCE(src.source_name, 'Web') as source_name,
                COALESCE(e.emp_first_name, 'Unassigned') as employee_name,
                ls.status_name as status
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            LEFT JOIN lead_sources src ON l.source_id = src.source_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
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
        print(f"Error in get_weekly_report_log: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_monthly_report_log(month=None, year=None, project_id=None, user_id=None, source_id=None, status_id=None):
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        import datetime
        now = datetime.datetime.now().date()
        
        if month and year:
            target_month = int(month)
            target_year = int(year)
            date_cond = f" AND MONTH(l.created_on) = {target_month} AND YEAR(l.created_on) = {target_year} "
            params = []
        else:
            # Full Current Month
            start = now.replace(day=1)
            import calendar
            _, last_day = calendar.monthrange(now.year, now.month)
            end = now.replace(day=last_day)
            date_cond, params = build_filters(str(start), str(end), project_id, user_id, source_id, status_id)
            
        query = f"""
            SELECT 
                l.created_on,
                l.lead_description as lead_name,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) as customer_name,
                COALESCE(p.project_name, 'Unknown') as project_name,
                COALESCE(src.source_name, 'Web') as source_name,
                COALESCE(e.emp_first_name, 'Unassigned') as employee_name,
                ls.status_name as status
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            LEFT JOIN lead_sources src ON l.source_id = src.source_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
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
        print(f"Error in get_monthly_report_log: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'conn' in locals() and conn: conn.close()

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
                       COUNT(l.lead_id) as leads_received
                FROM employee e
                LEFT JOIN leads l ON e.emp_id = l.emp_id AND MONTH(l.created_on) = {m} AND YEAR(l.created_on) = {y}
                WHERE e.emp_status = 'Active'
                GROUP BY e.emp_id
            """)
            emp_stats = cursor.fetchall()

            # Immutable site_visits and deals_closed from lead_status_history
            cursor.execute(f"""
                SELECT l.emp_id,
                       SUM(CASE WHEN ns.status_name = 'Site Visit Done' THEN 1 ELSE 0 END) as site_visits,
                       SUM(CASE WHEN ns.status_name = 'Deal Closed' THEN 1 ELSE 0 END) as deals_closed
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                WHERE MONTH(h.changed_at) = {m} AND YEAR(h.changed_at) = {y}
                  AND ns.status_name IN ('Site Visit Done', 'Deal Closed')
                GROUP BY l.emp_id
            """)
            hist_visit_map = {row['emp_id']: row for row in cursor.fetchall()}
            
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
                hist = hist_visit_map.get(emp_id, {})
                individuals[emp_id] = {
                    "name": str(stat['emp_first_name']).upper(),
                    "leads_received": stat['leads_received'] or 0,
                    "site_visits": int(hist.get('site_visits') or 0),
                    "deals_closed": int(hist.get('deals_closed') or 0),
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


def get_daily_site_visits():
    """Returns today's 'Site Visit Done' events grouped by employee, queried from lead_status_history."""
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT
                COALESCE(e.emp_first_name, 'Unassigned') AS employee_name,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) AS lead_name,
                h.lead_id,
                COALESCE(p.project_name, 'Unknown') AS project_name,
                h.changed_at AS visit_time
            FROM lead_status_history h
            JOIN leads l ON h.lead_id = l.lead_id
            JOIN lead_status ns ON h.new_status_id = ns.status_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            WHERE ns.status_name = 'Site Visit Done'
              AND DATE(h.changed_at) = CURDATE()
            ORDER BY employee_name, h.changed_at DESC
        """)
        rows = cursor.fetchall()
        for row in rows:
            if row.get('visit_time'):
                row['visit_time'] = str(row['visit_time'])
        return {"success": True, "data": rows}
    except Exception as e:
        print(f"Error in get_daily_site_visits: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def get_daily_calls_and_fresh_leads():
    """Returns today's call attempts and fresh leads for the EOD report."""
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        # Calls attempted today by employee
        cursor.execute("""
            SELECT
                COALESCE(e.emp_first_name, 'Unassigned') AS employee_name,
                COUNT(c.call_id) AS calls_today
            FROM call_log c
            LEFT JOIN employee e ON c.emp_id = e.emp_id
            WHERE DATE(c.created_at) = CURDATE()
            GROUP BY c.emp_id, employee_name
            ORDER BY calls_today DESC
        """)
        calls_by_emp = cursor.fetchall()
        total_calls = sum(r['calls_today'] for r in calls_by_emp)

        # Fresh leads created today
        cursor.execute("""
            SELECT
                l.lead_id,
                CONCAT(COALESCE(c.customer_first_name, ''), ' ', COALESCE(c.customer_last_name, '')) AS lead_name,
                COALESCE(e.emp_first_name, 'Unassigned') AS assigned_to,
                COALESCE(p.project_name, 'Unknown') AS project_name,
                ls.status_name,
                l.created_on
            FROM leads l
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            LEFT JOIN lead_status ls ON l.status_id = ls.status_id
            WHERE DATE(l.created_on) = CURDATE()
            ORDER BY l.created_on DESC
        """)
        fresh_leads = cursor.fetchall()
        for row in fresh_leads:
            if row.get('created_on'):
                row['created_on'] = str(row['created_on'])

        return {
            "success": True,
            "data": {
                "total_calls": total_calls,
                "calls_by_employee": calls_by_emp,
                "fresh_leads": fresh_leads,
                "fresh_leads_count": len(fresh_leads)
            }
        }
    except Exception as e:
        print(f"Error in get_daily_calls_and_fresh_leads: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_weekly_performance_report(project_id=None):
    """Returns user performance comparison for the current 7 days vs the previous 7 days."""
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        base_cond = ""
        params = []
        if project_id:
            base_cond += " AND l.project_id = %s "
            params.append(project_id)
            
        def get_aggregates_weekly(days_back_start, days_back_end):
            # cond matches current period (e.g. 7 days ago to today)
            cond = base_cond + f" AND l.created_on >= DATE_SUB(CURDATE(), INTERVAL {days_back_end} DAY) "
            if days_back_start > 0:
                cond += f" AND l.created_on < DATE_SUB(CURDATE(), INTERVAL {days_back_start} DAY) "

            # Overall
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_status ls ON l.status_id = ls.status_id WHERE 1=1 {cond}", tuple(params))
            total_leads = cursor.fetchone()['cnt']
            
            # Immutable site_visit / deal_closed from history
            hist_date_cond = f"WHERE h.changed_at >= DATE_SUB(CURDATE(), INTERVAL {days_back_end} DAY)"
            if days_back_start > 0:
                hist_date_cond += f" AND h.changed_at < DATE_SUB(CURDATE(), INTERVAL {days_back_start} DAY)"
            if project_id:
                hist_date_cond += " AND l.project_id = %s"
            cursor.execute(f"""
                SELECT COUNT(DISTINCT h.lead_id) as cnt
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                {hist_date_cond} AND ns.status_name = 'Site Visit Done'
            """, tuple([project_id] if project_id else []))
            site_visits = cursor.fetchone()['cnt']

            cursor.execute(f"""
                SELECT COUNT(DISTINCT h.lead_id) as cnt
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                {hist_date_cond} AND ns.status_name = 'Deal Closed'
            """, tuple([project_id] if project_id else []))
            deal_closed = cursor.fetchone()['cnt']

            # Calls
            call_cond = ""
            call_params = []
            if project_id:
                call_cond += " AND l.project_id = %s "
                call_params.append(project_id)

            call_at_cond = f" WHERE c.created_at >= DATE_SUB(CURDATE(), INTERVAL {days_back_end} DAY) "
            if days_back_start > 0:
                call_at_cond += f" AND c.created_at < DATE_SUB(CURDATE(), INTERVAL {days_back_start} DAY) "

            cursor.execute(f"""
                SELECT COUNT(*) as cnt
                FROM call_log c
                LEFT JOIN leads l ON c.lead_id = l.lead_id
                {call_at_cond} {call_cond}
            """, tuple(call_params))
            calls_attempted = cursor.fetchone()['cnt']

            # Individuals
            cursor.execute(f"""
                SELECT e.emp_id, e.emp_first_name,
                       COUNT(l.lead_id) as leads_received
                FROM employee e
                LEFT JOIN leads l ON e.emp_id = l.emp_id AND l.created_on >= DATE_SUB(CURDATE(), INTERVAL {days_back_end} DAY) {"AND l.created_on < DATE_SUB(CURDATE(), INTERVAL " + str(days_back_start) + " DAY)" if days_back_start > 0 else ""}
                WHERE e.emp_status = 'Active'
                GROUP BY e.emp_id
            """)
            emp_stats = cursor.fetchall()

            # Immutable per-employee visit/closed counts
            cursor.execute(f"""
                SELECT l.emp_id,
                       SUM(CASE WHEN ns.status_name = 'Site Visit Done' THEN 1 ELSE 0 END) AS site_visits,
                       SUM(CASE WHEN ns.status_name = 'Deal Closed'    THEN 1 ELSE 0 END) AS deals_closed
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                {hist_date_cond} AND ns.status_name IN ('Site Visit Done', 'Deal Closed')
                GROUP BY l.emp_id
            """, tuple([project_id] if project_id else []))
            weekly_hist_map = {row['emp_id']: row for row in cursor.fetchall()}
            
            cursor.execute(f"""
                SELECT e.emp_id, COUNT(c.call_id) as calls_attempted
                FROM employee e
                LEFT JOIN call_log c ON e.emp_id = c.emp_id AND c.created_at >= DATE_SUB(CURDATE(), INTERVAL {days_back_end} DAY) {"AND c.created_at < DATE_SUB(CURDATE(), INTERVAL " + str(days_back_start) + " DAY)" if days_back_start > 0 else ""}
                LEFT JOIN leads l ON c.lead_id = l.lead_id
                WHERE e.emp_status = 'Active' {call_cond}
                GROUP BY e.emp_id
            """, tuple(call_params))
            emp_call_stats = {row['emp_id']: row['calls_attempted'] for row in cursor.fetchall()}
            
            individuals = {}
            for stat in emp_stats:
                emp_id = stat['emp_id']
                if not emp_id: continue
                hist_w = weekly_hist_map.get(emp_id, {})
                individuals[emp_id] = {
                    "name": str(stat['emp_first_name']).upper(),
                    "leads_received": stat['leads_received'] or 0,
                    "site_visits": int(hist_w.get('site_visits') or 0),
                    "deals_closed": int(hist_w.get('deals_closed') or 0),
                    "calls_attempted": emp_call_stats.get(emp_id, 0)
                }

            return {
                "overall": {
                    "leads_received": total_leads,
                    "site_visits": site_visits,
                    "deal_closed": deal_closed,
                    "calls_attempted": calls_attempted
                },
                "individuals": individuals
            }

        curr_data = get_aggregates_weekly(0, 7)
        prev_data = get_aggregates_weekly(7, 14)
        
        return {
            "success": True, 
            "data": {
                "period": "Last 7 Days",
                "prev_period": "Previous 7 Days",
                "current": curr_data,
                "previous": prev_data
            }
        }
    except Exception as e:
        print(f"Error in get_weekly_performance_report: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'conn' in locals() and conn: conn.close()

def get_annual_performance_report(target_year=None, project_id=None):
    """Returns user performance comparison for the target year vs the previous year."""
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        base_cond = ""
        params = []
        if project_id:
            base_cond += " AND l.project_id = %s "
            params.append(project_id)
            
        import datetime
        year = int(target_year) if target_year else datetime.datetime.now().year
        prev_year = year - 1
        
        def get_aggregates_annual(y):
            cond = base_cond + f" AND YEAR(l.created_on) = {y} "
            
            # Overall
            cursor.execute(f"SELECT COUNT(*) as cnt FROM leads l LEFT JOIN lead_status ls ON l.status_id = ls.status_id WHERE 1=1 {cond}", tuple(params))
            total_leads = cursor.fetchone()['cnt']
            
            # Immutable overall site_visits / deals_closed from history
            ann_hist_cond = f"WHERE YEAR(h.changed_at) = {y}"
            if project_id:
                ann_hist_cond += " AND l.project_id = %s"
            cursor.execute(f"""
                SELECT COUNT(DISTINCT h.lead_id) as cnt
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                {ann_hist_cond} AND ns.status_name = 'Site Visit Done'
            """, tuple([project_id] if project_id else []))
            site_visits = cursor.fetchone()['cnt']

            cursor.execute(f"""
                SELECT COUNT(DISTINCT h.lead_id) as cnt
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                {ann_hist_cond} AND ns.status_name = 'Deal Closed'
            """, tuple([project_id] if project_id else []))
            deal_closed = cursor.fetchone()['cnt']

            # Calls
            call_cond = ""
            call_params = []
            if project_id:
                call_cond += " AND l.project_id = %s "
                call_params.append(project_id)
            cursor.execute(f"""
                SELECT COUNT(*) as cnt
                FROM call_log c
                LEFT JOIN leads l ON c.lead_id = l.lead_id
                WHERE YEAR(c.created_at) = {y} {call_cond}
            """, tuple(call_params))
            calls_attempted = cursor.fetchone()['cnt']

            # Individuals
            cursor.execute(f"""
                SELECT e.emp_id, e.emp_first_name,
                       COUNT(l.lead_id) as leads_received
                FROM employee e
                LEFT JOIN leads l ON e.emp_id = l.emp_id AND YEAR(l.created_on) = {y}
                WHERE e.emp_status = 'Active'
                GROUP BY e.emp_id
            """)
            emp_stats = cursor.fetchall()

            # Immutable per-employee visit/closed counts
            cursor.execute(f"""
                SELECT l.emp_id,
                       SUM(CASE WHEN ns.status_name = 'Site Visit Done' THEN 1 ELSE 0 END) AS site_visits,
                       SUM(CASE WHEN ns.status_name = 'Deal Closed'    THEN 1 ELSE 0 END) AS deals_closed
                FROM lead_status_history h
                JOIN leads l ON h.lead_id = l.lead_id
                JOIN lead_status ns ON h.new_status_id = ns.status_id
                {ann_hist_cond} AND ns.status_name IN ('Site Visit Done', 'Deal Closed')
                GROUP BY l.emp_id
            """, tuple([project_id] if project_id else []))
            annual_hist_map = {row['emp_id']: row for row in cursor.fetchall()}
            
            cursor.execute(f"""
                SELECT e.emp_id, COUNT(c.call_id) as calls_attempted
                FROM employee e
                LEFT JOIN call_log c ON e.emp_id = c.emp_id AND YEAR(c.created_at) = {y}
                LEFT JOIN leads l ON c.lead_id = l.lead_id
                WHERE e.emp_status = 'Active' {call_cond}
                GROUP BY e.emp_id
            """, tuple(call_params))
            emp_call_stats = {row['emp_id']: row['calls_attempted'] for row in cursor.fetchall()}
            
            individuals = {}
            for stat in emp_stats:
                emp_id = stat['emp_id']
                if not emp_id: continue
                hist_a = annual_hist_map.get(emp_id, {})
                individuals[emp_id] = {
                    "name": str(stat['emp_first_name']).upper(),
                    "leads_received": stat['leads_received'] or 0,
                    "site_visits": int(hist_a.get('site_visits') or 0),
                    "deals_closed": int(hist_a.get('deals_closed') or 0),
                    "calls_attempted": emp_call_stats.get(emp_id, 0)
                }

            return {
                "overall": {
                    "leads_received": total_leads,
                    "site_visits": site_visits,
                    "deal_closed": deal_closed,
                    "calls_attempted": calls_attempted
                },
                "individuals": individuals
            }

        curr_data = get_aggregates_annual(year)
        prev_data = get_aggregates_annual(prev_year)
        
        return {
            "success": True, 
            "data": {
                "period": f"Year {year}",
                "prev_period": f"Year {prev_year}",
                "current": curr_data,
                "previous": prev_data,
                "year": year,
                "prev_year": prev_year
            }
        }
    except Exception as e:
        print(f"Error in get_annual_performance_report: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'conn' in locals() and conn: conn.close()


def get_immutable_history_report(status_name, start_date=None, end_date=None, project_id=None, user_id=None):
    """
    Returns every lead that ever reached `status_name` ('Site Visit Done' or 'Deal Closed'),
    queried from lead_status_history (immutable). Includes current_status so admins can see
    how the lead progressed after the milestone.
    """
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        cond = "WHERE ns.status_name = %s"
        params = [status_name]

        if start_date:
            cond += " AND DATE(h.changed_at) >= %s"
            params.append(start_date)
        if end_date:
            end_date_time = f"{end_date} 23:59:59" if len(end_date) == 10 else end_date
            cond += " AND h.changed_at <= %s"
            params.append(end_date_time)
        if project_id:
            cond += " AND l.project_id = %s"
            params.append(project_id)
        if user_id:
            cond += " AND l.emp_id = %s"
            params.append(user_id)

        cursor.execute(f"""
            SELECT
                h.history_id,
                h.lead_id,
                TRIM(CONCAT(COALESCE(c.customer_first_name,''), ' ', COALESCE(c.customer_last_name,''))) AS lead_name,
                COALESCE(e.emp_first_name, 'Unassigned') AS employee_name,
                COALESCE(p.project_name, 'Unknown') AS project_name,
                h.changed_at,
                curr_s.status_name AS current_status,
                h.remarks
            FROM lead_status_history h
            JOIN leads l ON h.lead_id = l.lead_id
            JOIN lead_status ns ON h.new_status_id = ns.status_id
            LEFT JOIN lead_status curr_s ON l.status_id = curr_s.status_id
            LEFT JOIN customer c ON l.customer_id = c.customer_id
            LEFT JOIN employee e ON l.emp_id = e.emp_id
            LEFT JOIN project_registration p ON l.project_id = p.project_id
            {cond}
            ORDER BY h.changed_at DESC
        """, tuple(params))

        rows = cursor.fetchall()
        for row in rows:
            if row.get('changed_at'):
                row['changed_at'] = str(row['changed_at'])

        return {"success": True, "data": rows, "count": len(rows)}
    except Exception as e:
        print(f"Error in get_immutable_history_report: {traceback.format_exc()}")
        return {"success": False, "message": str(e)}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
