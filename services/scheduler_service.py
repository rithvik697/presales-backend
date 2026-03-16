from flask_apscheduler import APScheduler
from db import get_db
from services.reports_service import get_reports_summary, get_monthly_performance_report
from services.email_service import send_html_email
from datetime import datetime, timedelta
import traceback

scheduler = APScheduler()

def get_admin_emails():
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT email FROM employee WHERE role_id = 'ADMIN' AND email IS NOT NULL AND email != ''")
        admins = cursor.fetchall()
        return [admin['email'] for admin in admins]
    except Exception as e:
        print(f"Error fetching admin emails: {e}")
        return []
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def test_report_job():
    """Manual trigger for testing email delivery."""
    print("Running Test Report Job...")
    send_weekly_report()
    # Also trigger monthly to be sure
    send_monthly_report()


def send_weekly_report():
    print(f"Running Weekly Report Job at {datetime.now()}")
    try:
        # Last 7 days
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        summary_res = get_reports_summary(start_date, end_date)
        if not summary_res['success']:
            print("Failed to fetch summary for weekly report.")
            return

        data = summary_res['data']
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="background-color: #1976d2; color: white; padding: 20px; text-align: center;">
                <h2>Weekly Performance Report</h2>
                <p>{start_date} to {end_date}</p>
            </div>
            <div style="padding: 20px;">
                <h3>Overall Summary</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">Metric</th>
                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">Count</th>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 12px;">Total Leads</td>
                        <td style="border: 1px solid #ddd; padding: 12px;">{data['total_leads']}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 12px;">Active Leads</td>
                        <td style="border: 1px solid #ddd; padding: 12px;">{data['active_leads']}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 12px;">Deals Closed</td>
                        <td style="border: 1px solid #ddd; padding: 12px;">{data['closed_leads']}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 12px;">Spam/Lost</td>
                        <td style="border: 1px solid #ddd; padding: 12px;">{data['lost_leads']}</td>
                    </tr>
                </table>
                <p style="margin-top: 20px; font-size: 0.9em; color: #666;">
                    For detailed employee performance and logs, please log in to the CRM dashboard.
                </p>
            </div>
            <div style="background-color: #f2f2f2; padding: 10px; text-align: center; font-size: 0.8em; color: #888;">
                &copy; {datetime.now().year} CRM Automated Reporting System
            </div>
        </body>
        </html>
        """
        
        emails = get_admin_emails()
        for email in emails:
            send_html_email(email, f"Weekly Performance Report ({start_date} - {end_date})", html_content)
        print(f"Weekly Report sent to {len(emails)} admins.")
    except Exception as e:
        print(f"Error in send_weekly_report job: {traceback.format_exc()}")

def send_monthly_report():
    print(f"Running Monthly Report Job at {datetime.now()}")
    try:
        now = datetime.now()
        # If it's the 1st of the month, we pull for the previous month (the one that just ended)
        # Otherwise (like on the last day of the month), we pull for the current month
        if now.day == 1:
            target_date = now - timedelta(days=1)
        else:
            target_date = now
            
        month = target_date.month
        year = target_date.year
        
        report_res = get_monthly_performance_report(month, year)
        if not report_res['success']:
            print("Failed to fetch monthly report data.")
            return

        data = report_res['data']
        curr = data['current']['overall']
        prev = data['previous']['overall']
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="background-color: #1976d2; color: white; padding: 20px; text-align: center;">
                <h2>Monthly Performance Report</h2>
                <p>{data['month_name']} {data['year']}</p>
            </div>
            <div style="padding: 20px;">
                <h3>Overall Statistics</h3>
                <table style="width: 100%; border-collapse: collapse; font-size: 0.95em;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; padding: 10px; text-align: left;">Metric</th>
                        <th style="border: 1px solid #ddd; padding: 10px; text-align: left;">Current ({data['month_name']})</th>
                        <th style="border: 1px solid #ddd; padding: 10px; text-align: left;">Previous ({data['prev_month_name']})</th>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 10px;">Leads Received</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{curr['leads_received']}</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{prev['leads_received']}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 10px;">Site Visits</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{curr['site_visits']}</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{prev['site_visits']}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 10px;">Calls Attempted</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{curr['calls_attempted']}</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{prev['calls_attempted']}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 10px;">Deals Closed</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{curr['deal_closed']}</td>
                        <td style="border: 1px solid #ddd; padding: 10px;">{prev['deal_closed']}</td>
                    </tr>
                </table>

                <h3 style="margin-top: 25px;">Highlights</h3>
                <div style="background-color: #f9f9f9; border-left: 5px solid #4caf50; padding: 10px; margin-bottom: 10px;">
                    <strong>Good Performance:</strong><br/>
                    - Highest calls: {data['highlights']['highest_calls']['count']} by {data['highlights']['highest_calls']['name']}<br/>
                    - Highest site visits: {data['highlights']['highest_visits']['count']} by {data['highlights']['highest_visits']['name']}
                </div>
                <div style="background-color: #f9f9f9; border-left: 5px solid #f44336; padding: 10px;">
                    <strong>Needs Improvement:</strong><br/>
                    - Lowest calls: {data['highlights']['lowest_calls']['count']} by {data['highlights']['lowest_calls']['name']}<br/>
                    - Lowest site visits: {data['highlights']['lowest_visits']['count']} by {data['highlights']['lowest_visits']['name']}
                </div>
            </div>
            <div style="background-color: #f2f2f2; padding: 10px; text-align: center; font-size: 0.8em; color: #888;">
                Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
        </body>
        </html>
        """
        
        emails = get_admin_emails()
        for email in emails:
            send_html_email(email, f"Monthly Performance Report - {data['month_name']} {data['year']}", html_content)
        print(f"Monthly Report sent to {len(emails)} admins.")
    except Exception as e:
        print(f"Error in send_monthly_report job: {traceback.format_exc()}")

def send_quarterly_report():
    print(f"Running Quarterly Report Job at {datetime.now()}")
    try:
        now = datetime.now()
        # Find start of current quarter
        curr_quarter = (now.month - 1) // 3 + 1
        
        # If it's the first day of a month following a quarter end, we pull for the previous quarter
        if now.day == 1 and now.month in [1, 4, 7, 10]:
            target_quarter = curr_quarter - 1 if curr_quarter > 1 else 4
            target_year = now.year if curr_quarter > 1 else now.year - 1
        else:
            target_quarter = curr_quarter
            target_year = now.year
            
        q_start_month = (target_quarter - 1) * 3 + 1
        start_date = datetime(target_year, q_start_month, 1).strftime('%Y-%m-%d')
        # End date is start of next quarter minus 1 day
        if target_quarter < 4:
            end_date = (datetime(target_year, q_start_month + 3, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            end_date = datetime(target_year, 12, 31).strftime('%Y-%m-%d')

        summary_res = get_reports_summary(start_date, end_date)
        if not summary_res['success']:
            return

        data = summary_res['data']
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="background-color: #1976d2; color: white; padding: 20px; text-align: center;">
                <h2>Quarterly Performance Report</h2>
                <p>Q{target_quarter} {target_year} ({start_date} to {end_date})</p>
            </div>
            <div style="padding: 20px;">
                <h3>Quarterly Summary</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">Metric</th>
                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">Count</th>
                    </tr>
                    <tr><td style="border: 1px solid #ddd; padding: 12px;">Total Leads</td><td style="border: 1px solid #ddd; padding: 12px;">{data['total_leads']}</td></tr>
                    <tr><td style="border: 1px solid #ddd; padding: 12px;">Active Leads</td><td style="border: 1px solid #ddd; padding: 12px;">{data['active_leads']}</td></tr>
                    <tr><td style="border: 1px solid #ddd; padding: 12px;">Deals Closed</td><td style="border: 1px solid #ddd; padding: 12px;">{data['closed_leads']}</td></tr>
                </table>
            </div>
        </body>
        </html>
        """
        emails = get_admin_emails()
        for email in emails:
            send_html_email(email, f"Quarterly Performance Report - Q{target_quarter} {target_year}", html_content)
        print(f"Quarterly Report sent to {len(emails)} admins.")
    except Exception as e:
        print(f"Error in send_quarterly_report job: {traceback.format_exc()}")

def send_annual_report():
    print(f"Running Annual Report Job at {datetime.now()}")
    try:
        now = datetime.now()
        # If it's Jan 1st, we pull for previous year
        if now.month == 1 and now.day == 1:
            target_year = now.year - 1
        else:
            target_year = now.year
            
        start_date = f"{target_year}-01-01"
        end_date = f"{target_year}-12-31"

        summary_res = get_reports_summary(start_date, end_date)
        if not summary_res['success']:
            return

        data = summary_res['data']
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="background-color: #1976d2; color: white; padding: 20px; text-align: center;">
                <h2>Annual Performance Report</h2>
                <p>Year {target_year}</p>
            </div>
            <div style="padding: 20px;">
                <h3>Annual Summary</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">Metric</th>
                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">Count</th>
                    </tr>
                    <tr><td style="border: 1px solid #ddd; padding: 12px;">Total Leads</td><td style="border: 1px solid #ddd; padding: 12px;">{data['total_leads']}</td></tr>
                    <tr><td style="border: 1px solid #ddd; padding: 12px;">Active Leads</td><td style="border: 1px solid #ddd; padding: 12px;">{data['active_leads']}</td></tr>
                    <tr><td style="border: 1px solid #ddd; padding: 12px;">Deals Closed</td><td style="border: 1px solid #ddd; padding: 12px;">{data['closed_leads']}</td></tr>
                </table>
            </div>
        </body>
        </html>
        """
        emails = get_admin_emails()
        for email in emails:
            send_html_email(email, f"Annual Performance Report - {target_year}", html_content)
        print(f"Annual Report sent to {len(emails)} admins.")
    except Exception as e:
        print(f"Error in send_annual_report job: {traceback.format_exc()}")

def init_scheduler(app):
    scheduler.init_app(app)
    
    # Weekly: Every Sunday at 19:00 (7:00 PM)
    scheduler.add_job(id='weekly_report', func=send_weekly_report, trigger='cron', day_of_week='sun', hour=19, minute=0)
    
    # Monthly: Last day of the month at 23:59
    try:
        scheduler.add_job(id='monthly_report', func=send_monthly_report, trigger='cron', day='last', hour=23, minute=59)
    except:
        scheduler.add_job(id='monthly_report', func=send_monthly_report, trigger='cron', day=1, hour=0, minute=5)
    
    # Quarterly: Last day of Mar, Jun, Sep, Dec at 23:59
    try:
        scheduler.add_job(id='quarterly_report', func=send_quarterly_report, trigger='cron', month='3,6,9,12', day='last', hour=23, minute=59)
    except:
        # Fallback to 1st of Jan, Apr, Jul, Oct
        scheduler.add_job(id='quarterly_report', func=send_quarterly_report, trigger='cron', month='1,4,7,10', day=1, hour=0, minute=10)

    # Annual: Dec 31st at 23:59
    try:
        scheduler.add_job(id='annual_report', func=send_annual_report, trigger='cron', month=12, day='last', hour=23, minute=59)
    except:
        # Fallback to Jan 1st
        scheduler.add_job(id='annual_report', func=send_annual_report, trigger='cron', month=1, day=1, hour=0, minute=15)
        
    scheduler.start()

