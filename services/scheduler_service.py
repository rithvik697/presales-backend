from flask_apscheduler import APScheduler
from db import get_db
from services.reports_service import (
    get_reports_summary, 
    get_monthly_performance_report,
    get_weekly_performance_report,
    get_annual_performance_report,
    get_daily_site_visits,
    get_daily_calls_and_fresh_leads
)
from services.email_service import send_html_email
from services.report_email_service import get_recipients_for_report
from services.notification_service import create_notification
from datetime import datetime, timedelta
import traceback

scheduler = APScheduler()
SITE_VISIT_REMINDER_TITLE = "Expected Site Visit"
REMINDER_TYPES = {
    "D_MINUS_2_EOD": {
        "days_before": 2,
        "time_label": "7:30 PM reminder",
        "job_label": "2-day"
    },
    "D_MINUS_1_EOD": {
        "days_before": 1,
        "time_label": "7:30 PM reminder",
        "job_label": "1-day"
    },
    "VISIT_DAY_MORNING": {
        "days_before": 0,
        "time_label": "9:30 AM reminder",
        "job_label": "same-day"
    }
}

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


def get_report_emails(report_type):
    """Get emails for a report type from config table, fallback to admin emails."""
    emails = get_recipients_for_report(report_type)
    if not emails:
        emails = get_admin_emails()
    return emails
def get_admin_and_manager_emails():
    """Returns emails for both Admins and Sales Managers."""
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT DISTINCT email 
            FROM employee 
            WHERE role_id IN ('ADMIN', 'SALES_MANAGER') 
              AND email IS NOT NULL AND email != ''
        """)
        rows = cursor.fetchall()
        return [r['email'] for r in rows]
    except Exception as e:
        print(f"Error fetching admin/manager emails: {e}")
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


def ensure_site_visit_reminder_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_visit_reminders (
            reminder_id INT AUTO_INCREMENT PRIMARY KEY,
            schedule_id INT NOT NULL,
            emp_id VARCHAR(20) NOT NULL,
            reminder_type VARCHAR(30) NOT NULL,
            sent_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_site_visit_reminder (schedule_id, emp_id, reminder_type)
        )
    """)


def send_site_visit_reminders(reminder_type):
    reminder_config = REMINDER_TYPES[reminder_type]
    target_date = (datetime.now() + timedelta(days=reminder_config["days_before"])).date()

    print(
        f"Running {reminder_config['job_label']} site visit reminder job at {datetime.now()} "
        f"for visits on {target_date}"
    )

    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        ensure_site_visit_reminder_table(cursor)

        cursor.execute("""
            SELECT
                s.schedule_id,
                s.lead_id,
                s.scheduled_at,
                TRIM(CONCAT(c.customer_first_name, ' ', IFNULL(c.customer_last_name, ''))) AS lead_name
            FROM lead_scheduled_activities s
            JOIN lead_status ls ON ls.status_id = s.status_id
            JOIN leads l ON l.lead_id = s.lead_id AND l.is_active = 1
            JOIN customer c ON c.customer_id = l.customer_id
            WHERE ls.status_name = 'Expected Site Visit'
              AND s.status = 'SCHEDULED'
              AND DATE(s.scheduled_at) = %s
              AND NOT EXISTS (
                    SELECT 1
                    FROM lead_status_history h
                    JOIN lead_status done_ls ON done_ls.status_id = h.new_status_id
                    WHERE h.lead_id = s.lead_id
                      AND done_ls.status_name = 'Site Visit Done'
                      AND h.changed_at >= s.created_on
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM lead_scheduled_activities s2
                    JOIN lead_status ls2 ON ls2.status_id = s2.status_id
                    WHERE s2.lead_id = s.lead_id
                      AND s2.status = 'SCHEDULED'
                      AND ls2.status_name = 'Expected Site Visit'
                      AND s2.schedule_id > s.schedule_id
              )
            ORDER BY s.scheduled_at ASC
        """, (target_date,))

        schedules = cursor.fetchall()
        if not schedules:
            print(f"No site visit schedules found for reminder type {reminder_type}.")
            return

        cursor.execute("""
            SELECT emp_id
            FROM employee
            WHERE emp_status = 'Active'
        """)
        users = cursor.fetchall()

        if not users:
            print("No active users found for site visit reminders.")
            return

        reminders_created = 0

        for schedule in schedules:
            visit_time = schedule["scheduled_at"].strftime("%d-%m-%Y %I:%M %p")
            message = (
                f"{schedule['lead_name']} ({schedule['lead_id']}) is expected to visit the site on "
                f"{visit_time}. This is your {reminder_config['time_label']} notification."
            )

            for user in users:
                cursor.execute("""
                    SELECT 1
                    FROM site_visit_reminders
                    WHERE schedule_id = %s
                      AND emp_id = %s
                      AND reminder_type = %s
                """, (schedule["schedule_id"], user["emp_id"], reminder_type))

                if cursor.fetchone():
                    continue

                create_notification(
                    user["emp_id"],
                    SITE_VISIT_REMINDER_TITLE,
                    message,
                    "Leads",
                    schedule["lead_id"]
                )

                cursor.execute("""
                    INSERT INTO site_visit_reminders (schedule_id, emp_id, reminder_type)
                    VALUES (%s, %s, %s)
                """, (schedule["schedule_id"], user["emp_id"], reminder_type))

                reminders_created += 1

        conn.commit()
        print(
            f"Created {reminders_created} site visit reminder notifications for "
            f"reminder type {reminder_type}."
        )
    except Exception:
        if conn:
            conn.rollback()
        print(f"Error in send_site_visit_reminders job: {traceback.format_exc()}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _row(bold_label, value, prev_label='', prev_value=None):
    """Generates a consistent row: bold label + value, optional previous month note."""
    prev_note = f'<br><span style="color:#888;font-size:0.88em;">({prev_label}: {prev_value})</span>' if prev_value is not None else ''
    return (
        f'<p style="margin:6px 0;">'
        f'<strong>{bold_label}</strong>&nbsp;&nbsp;{value}'
        f'{prev_note}</p>'
    )


def _individual_block(name, curr, prev=None, period_label=''):
    """Generates a per-employee block in the WhatsApp style."""
    def _line(label, curr_val, prev_val=None):
        if prev_val is not None and period_label:
            return (f'<p style="margin:3px 0 3px 12px;">{label}:&nbsp;&nbsp;{curr_val}'
                    f'<br><span style="color:#888;font-size:0.88em;">({period_label}: {prev_val})</span></p>')
        return f'<p style="margin:3px 0 3px 12px;">{label}:&nbsp;&nbsp;{curr_val}</p>'

    p = prev or {}
    block = f'<p style="margin:14px 0 4px 0;"><strong>{name}</strong></p>'
    block += _line('Leads received', curr.get('leads_received', 0), p.get('leads_received'))
    block += _line('Site Visit Done', curr.get('site_visits', 0), p.get('site_visits'))
    block += _line('Calls attempted', curr.get('calls_attempted', 0), p.get('calls_attempted'))
    block += _line('Pipeline', curr.get('pipeline', 0), p.get('pipeline'))
    block += _line('DEAL CLOSED', curr.get('deals_closed', 0), p.get('deals_closed'))
    return block


def _html_wrap(title, subtitle, body):
    """Wraps body in a consistent header and footer."""
    year = datetime.now().year
    return f"""
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:650px;margin:auto;">
<div style="background:#1976d2;color:white;padding:20px;text-align:center;border-radius:6px 6px 0 0;">
  <h2 style="margin:0;">{title}</h2>
  <p style="margin:4px 0 0;font-size:0.95em;">{subtitle}</p>
</div>
<div style="padding:20px;border:1px solid #e0e0e0;border-top:none;">
{body}
</div>
<div style="background:#f2f2f2;padding:8px;text-align:center;font-size:0.8em;color:#888;">
  &copy; {year} CRM Automated Reporting System
</div>
</body></html>
"""


def send_weekly_report():
    print(f"Running Weekly Report Job at {datetime.now()}")
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        res = get_weekly_performance_report()
        if not res['success']:
            print("Failed to fetch weekly performance data.")
            return

        data = res['data']
        curr_overall = data['current']['overall']
        curr_indiv = data['current']['individuals']

        date_range = f"{start_date} to {end_date}"

        body = f'<p style="font-size:1em;"><strong>Please find the Weekly Report ({date_range})</strong></p>'
        body += '<hr style="border:none;border-top:1px solid #eee;">'
        body += '<p><strong>OVERALL</strong></p>'
        body += _row('*Leads Received:*', curr_overall.get('leads_received', 0))
        body += _row('*Site Visit Done:*', curr_overall.get('site_visits', 0))
        body += _row('*Pipeline:*', curr_overall.get('pipeline', curr_overall.get('deal_closed', 0)))
        body += _row('*Calls attempted:*', curr_overall.get('calls_attempted', 0))
        body += _row('*DEAL CLOSED:*', curr_overall.get('deal_closed', 0))

        if curr_indiv:
            body += '<hr style="border:none;border-top:1px solid #eee; margin-top:16px;">'
            body += '<p><strong>Each Individual Performance:</strong></p>'
            for emp_id, ic in curr_indiv.items():
                body += _individual_block(ic['name'], ic)

        html = _html_wrap('Weekly Performance Report', date_range, body)
        emails = get_admin_and_manager_emails()
        for email in emails:
            send_html_email(email, f"Weekly Performance Report ({date_range})", html)
        print(f"Weekly Report sent to {len(emails)} recipients.")
    except Exception:
        print(f"Error in send_weekly_report job: {traceback.format_exc()}")


def send_monthly_report():
    print(f"Running Monthly Report Job at {datetime.now()}")
    try:
        now = datetime.now()
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
        curr_indiv = data['current']['individuals']
        prev_indiv = data['previous']['individuals']
        month_name = data['month_name']
        prev_month_name = data['prev_month_name']
        yr = data['year']
        prev_label = f"{prev_month_name} month"

        body = f'<p style="font-size:1em;"><strong>Please find the {month_name} {yr} monthly report (TG)</strong></p>'
        body += '<hr style="border:none;border-top:1px solid #eee;">'
        body += '<p><strong>OVERALL</strong></p>'
        body += _row('*Leads Received:*', curr.get('leads_received', 0), prev_label, prev.get('leads_received', 0))
        body += _row('*Test Leads:*', curr.get('test_leads', 0), prev_label, prev.get('test_leads', 0))
        body += _row('*Site Visit Done:*', curr.get('site_visits', 0), prev_label, prev.get('site_visits', 0))
        body += _row('*Not Enquired/Spam:*', curr.get('spam', 0), prev_label, prev.get('spam', 0))
        body += _row('*Not Interested:*', curr.get('not_interested', 0), prev_label, prev.get('not_interested', 0))
        body += _row('*Walk-ins (incl digital):*', curr.get('walkins', 0), prev_label, prev.get('walkins', 0))
        body += _row('*mcube (IVR):*', curr.get('mcube', 0), prev_label, prev.get('mcube', 0))
        body += _row('*Calls attempted:*', curr.get('calls_attempted', 0), prev_label, prev.get('calls_attempted', 0))
        body += _row('*Pipeline:*', curr.get('pipeline', 0), prev_label, prev.get('pipeline', 0))
        body += _row('*DEAL CLOSED:*', curr.get('deal_closed', 0), prev_label, prev.get('deal_closed', 0))

        if curr_indiv:
            body += '<hr style="border:none;border-top:1px solid #eee; margin-top:16px;">'
            body += '<p><strong>Each Individual Performance:</strong></p>'
            for emp_id, ic in curr_indiv.items():
                pc = prev_indiv.get(emp_id, {})
                body += _individual_block(ic['name'], ic, pc, prev_label)

        html = _html_wrap('Monthly Performance Report', f"{month_name} {yr}", body)
        emails = get_admin_and_manager_emails()
        for email in emails:
            send_html_email(email, f"Monthly Performance Report - {month_name} {yr}", html)
        print(f"Monthly Report sent to {len(emails)} recipients.")
    except Exception:
        print(f"Error in send_monthly_report job: {traceback.format_exc()}")


def send_quarterly_report():
    print(f"Running Quarterly Report Job at {datetime.now()}")
    try:
        now = datetime.now()
        curr_quarter = (now.month - 1) // 3 + 1

        if now.day == 1 and now.month in [1, 4, 7, 10]:
            target_quarter = curr_quarter - 1 if curr_quarter > 1 else 4
            target_year = now.year if curr_quarter > 1 else now.year - 1
        else:
            target_quarter = curr_quarter
            target_year = now.year

        q_start_month = (target_quarter - 1) * 3 + 1
        start_date = datetime(target_year, q_start_month, 1).strftime('%Y-%m-%d')
        if target_quarter < 4:
            end_date = (datetime(target_year, q_start_month + 3, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            end_date = datetime(target_year, 12, 31).strftime('%Y-%m-%d')

        summary_res = get_reports_summary(start_date, end_date)
        if not summary_res['success']:
            return

        data = summary_res['data']
        period_label = f"Q{target_quarter} {target_year} ({start_date} to {end_date})"

        body = f'<p style="font-size:1em;"><strong>Please find the Quarterly Report – {period_label}</strong></p>'
        body += '<hr style="border:none;border-top:1px solid #eee;">'
        body += '<p><strong>OVERALL</strong></p>'
        body += _row('*Leads Received:*', data.get('total_leads', 0))
        body += _row('*Active Leads:*', data.get('active_leads', 0))
        body += _row('*DEAL CLOSED:*', data.get('closed_leads', 0))

        html = _html_wrap('Quarterly Performance Report', period_label, body)
        emails = get_admin_and_manager_emails()
        for email in emails:
            send_html_email(email, f"Quarterly Performance Report - Q{target_quarter} {target_year}", html)
        print(f"Quarterly Report sent to {len(emails)} recipients.")
    except Exception:
        print(f"Error in send_quarterly_report job: {traceback.format_exc()}")


def send_annual_report():
    print(f"Running Annual Report Job at {datetime.now()}")
    try:
        now = datetime.now()
        if now.month == 1 and now.day == 1:
            target_year = now.year - 1
        else:
            target_year = now.year

        res = get_annual_performance_report(target_year)
        if not res['success']:
            print("Failed to fetch annual data.")
            return

        data = res['data']
        curr_overall = data['current']['overall']
        curr_indiv = data['current']['individuals']
        prev_overall = data['previous']['overall']
        prev_year = data['prev_year']

        body = f'<p style="font-size:1em;"><strong>Please find the Annual Report – {target_year}</strong></p>'
        body += '<hr style="border:none;border-top:1px solid #eee;">'
        body += '<p><strong>OVERALL</strong></p>'
        body += _row('*Leads Received:*', curr_overall.get('leads_received', 0))
        body += _row('*Site Visit Done:*', curr_overall.get('site_visits', 0))
        body += _row('*Calls attempted:*', curr_overall.get('calls_attempted', 0))
        body += _row('*DEAL CLOSED:*', curr_overall.get('deal_closed', 0))

        if curr_indiv:
            body += '<hr style="border:none;border-top:1px solid #eee; margin-top:16px;">'
            body += '<p><strong>Each Individual Performance:</strong></p>'
            for emp_id, ic in curr_indiv.items():
                body += _individual_block(ic['name'], ic)

        html = _html_wrap('Annual Performance Report', f"Year {target_year}", body)
        emails = get_admin_and_manager_emails()
        for email in emails:
            send_html_email(email, f"Annual Performance Report – {target_year}", html)
        print(f"Annual Report sent to {len(emails)} recipients.")
    except Exception:
        print(f"Error in send_annual_report job: {traceback.format_exc()}")


def send_daily_site_visit_report():
    """Sent at 7:30 PM daily — shows today's Site Visit Done events."""
    print(f"Running Daily Site Visit Report at {datetime.now()}")
    try:
        today = datetime.now().strftime('%d %B %Y')
        res = get_daily_site_visits()
        if not res['success']:
            print("Failed to fetch daily site visit data.")
            return

        visits = res['data']
        rows_html = ''.join([
            f"""<p style="margin:4px 0;">
                &bull; <strong>{v['lead_name']}</strong> ({v['employee_name']}) &mdash;
                {v['project_name']} @ {v['visit_time']}
            </p>"""
            for v in visits
        ]) if visits else '<p style="color:#888;">No site visits recorded today.</p>'

        body = f'<p><strong>Daily Site Visit Report – {today}</strong></p>'
        body += f'<p><strong>Total Site Visits Today: {len(visits)}</strong></p>'
        body += '<hr style="border:none;border-top:1px solid #eee;">'
        body += rows_html

        html = _html_wrap('Daily Site Visit Report', today, body)
        emails = get_admin_and_manager_emails()
        for email in emails:
            send_html_email(email, f"Daily Site Visit Report – {today}", html)
        print(f"Daily site visit report sent to {len(emails)} recipients.")
    except Exception:
        print(f"Error in send_daily_site_visit_report: {traceback.format_exc()}")


def send_daily_eod_report():
    """Sent at 11:59 PM daily — shows calls attempted and fresh leads today."""
    print(f"Running Daily EOD Report at {datetime.now()}")
    try:
        today = datetime.now().strftime('%d %B %Y')
        res = get_daily_calls_and_fresh_leads()
        if not res['success']:
            print("Failed to fetch EOD data.")
            return

        data = res['data']
        fresh_leads = data['fresh_leads']
        calls_by_emp = data['calls_by_employee']

        body = f'<p><strong>Daily End-of-Day Report – {today}</strong></p>'
        body += '<hr style="border:none;border-top:1px solid #eee;">'

        body += f'<p><strong>&#128222; Calls Attempted – Total: {data["total_calls"]}</strong></p>'
        for r in calls_by_emp:
            body += f'<p style="margin:3px 0 3px 12px;">{r["employee_name"]}:&nbsp;&nbsp;<strong>{r["calls_today"]}</strong></p>'
        if not calls_by_emp:
            body += '<p style="color:#888;margin-left:12px;">No calls recorded today.</p>'

        body += f'<p style="margin-top:14px;"><strong>&#127807; Fresh Leads Today – Total: {data["fresh_leads_count"]}</strong></p>'
        for l in fresh_leads:
            body += (f'<p style="margin:3px 0 3px 12px;"><strong>{l["lead_name"]}</strong> '
                     f'&mdash; {l["assigned_to"]} | {l["project_name"]} | {l["status_name"] or "New"}</p>')
        if not fresh_leads:
            body += '<p style="color:#888;margin-left:12px;">No new leads today.</p>'

        html = _html_wrap('Daily End-of-Day Report', today, body)
        emails = get_admin_and_manager_emails()
        for email in emails:
            send_html_email(email, f"Daily EOD Report – {today}", html)
        print(f"Daily EOD report sent to {len(emails)} recipients.")
    except Exception:
        print(f"Error in send_daily_eod_report: {traceback.format_exc()}")



def send_site_visit_reminders_two_days_before():
    send_site_visit_reminders("D_MINUS_2_EOD")


def send_site_visit_reminders_one_day_before():
    send_site_visit_reminders("D_MINUS_1_EOD")


def send_site_visit_reminders_visit_day():
    send_site_visit_reminders("VISIT_DAY_MORNING")

def init_scheduler(app):
    scheduler.init_app(app)
    
    # Daily: Site Visit Report at 7:30 PM
    scheduler.add_job(id='daily_site_visit_report', func=send_daily_site_visit_report, trigger='cron', hour=19, minute=30)

    # Daily: EOD Calls + Fresh Leads at 11:59 PM
    scheduler.add_job(id='daily_eod_report', func=send_daily_eod_report, trigger='cron', hour=23, minute=59)

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

    # Annual: March 31st at 23:59 (Financial Year End)
    try:
        scheduler.add_job(id='annual_report', func=send_annual_report, trigger='cron', month=3, day='last', hour=23, minute=59)
    except:
        # Fallback to Apr 1st
        scheduler.add_job(id='annual_report', func=send_annual_report, trigger='cron', month=4, day=1, hour=0, minute=15)

    scheduler.add_job(
        id='site_visit_reminder_two_days_before',
        func=send_site_visit_reminders_two_days_before,
        trigger='cron',
        hour=19,
        minute=30
    )
    scheduler.add_job(
        id='site_visit_reminder_one_day_before',
        func=send_site_visit_reminders_one_day_before,
        trigger='cron',
        hour=19,
        minute=30
    )
    scheduler.add_job(
        id='site_visit_reminder_visit_day',
        func=send_site_visit_reminders_visit_day,
        trigger='cron',
        hour=9,
        minute=30
    )
        
    scheduler.start()

