import json
import logging
import requests
import sys

from datetime import datetime, timedelta
from retrying import retry
import tableauserverclient as TSC

# ─── Load Config & Logging ─────────────────────────────────────────────────────

cf = json.load(open('config.json'))

logging.basicConfig(
    filename = cf.get('LogFile', 'tableau_status_check.log'),
    level    = logging.INFO,
    format   = '%(asctime)s - %(levelname)s - %(message)s',
    filemode = 'a'
)

# ─── Constants ─────────────────────────────────────────────────────────────────

# Lookback window (hours); override by adding "TimeframeHours" to config.json
TIMEFRAME_HOURS  = cf.get('TimeframeHours', 4)

# Tableau job types
JOB_EXTRACT_TYPE = 'RefreshExtract'
JOB_FLOW_TYPE    = 'RunFlow'

# ─── Authentication Helpers ────────────────────────────────────────────────────

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def sign_in(server_url, token_name, token_secret, site_id):
    """
    Signs in via Personal Access Token, retrying up to 3 times.
    Uses `http_options` from config for SSL verification.
    """
    server = TSC.Server(server_url, use_server_version=True)
    server.add_http_options({'verify': cf.get('http_options', True)})
    auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
    server.auth.sign_in(auth)
    logging.info(f"Signed in to {server_url} (site: '{site_id}')")
    return server

def sign_out(server):
    """Signs out cleanly (ignoring any errors)."""
    try:
        server.auth.sign_out()
        logging.info("Signed out")
    except Exception as e:
        logging.warning(f"Sign-out issue: {e}")

# ─── Fetching Failed Jobs ──────────────────────────────────────────────────────

def get_failed_jobs(server, job_type):
    """
    Fetches all jobs of `job_type` created within the last TIMEFRAME_HOURS
    and returns those with non-zero finish_code.
    """
    cutoff = datetime.utcnow() - timedelta(hours=TIMEFRAME_HOURS)

    opts = TSC.RequestOptions()
    opts.filter.add(TSC.Filter(
        field    = TSC.RequestOptions.Field.Type,
        operator = TSC.RequestOptions.Operator.Equals,
        value    = job_type
    ))
    opts.sort.add(TSC.Sort(
        field     = TSC.RequestOptions.Field.CreatedAt,
        direction = TSC.RequestOptions.Direction.Desc
    ))

    all_jobs, _ = server.jobs.get(req_options=opts)
    failures = []

    for job in all_jobs:
        created = job.created_at.replace(tzinfo=None)
        if job.finish_code != 0 and created >= cutoff:
            # Retrieve error detail via job details call
            details = server.jobs.get_job_details(job.id)
            failures.append({
                'id'         : job.id,
                'type'       : job_type,
                'finish_code': job.finish_code,
                'created_at' : created.strftime('%Y-%m-%d %H:%M:%S'),
                'error'      : getattr(details, 'error_detail', 'Unknown error')
            })

    return failures

# ─── Microsoft Teams Notification ─────────────────────────────────────────────

def send_teams_notification(failures, env_label):
    """
    Sends a summary card and, if failures exist, a details card
    to the Teams webhook.
    """
    webhook_url = cf['TeamsWebhookURL']
    title       = f"{env_label} – Tableau Job Failures"

    # Build summary text
    if not failures:
        summary_text = f"No failures in the last {TIMEFRAME_HOURS} hours."
    else:
        counts = {}
        for f in failures:
            counts[f['type']] = counts.get(f['type'], 0) + 1
        parts = [f"{cnt} {typ.lower()}(s) failed" for typ, cnt in counts.items()]
        summary_text = " | ".join(parts)

    # Send summary card
    summary_payload = {
        "@type"     : "MessageCard",
        "@context"  : "http://schema.org/extensions",
        "themeColor": "0076D7",
        "title"     : title,
        "text"      : summary_text
    }
    resp = requests.post(webhook_url, json=summary_payload)
    if resp.status_code == 200:
        logging.info(f"{env_label} summary sent.")
    else:
        logging.error(f"{env_label} summary failed: {resp.status_code}, {resp.text}")

    # Send detailed card if needed
    if failures:
        lines = []
        for f in failures:
            lines.append(
                f"• [{f['type']}] Job {f['id']} at {f['created_at']} UTC — "
                f"Code {f['finish_code']} — {f['error']}"
            )
        detail_payload = {
            "@type"     : "MessageCard",
            "@context"  : "http://schema.org/extensions",
            "themeColor": "D70076",
            "title"     : title + " (Details)",
            "text"      : "\n".join(lines)
        }
        resp = requests.post(webhook_url, json=detail_payload)
        if resp.status_code == 200:
            logging.info(f"{env_label} details sent.")
        else:
            logging.error(f"{env_label} details failed: {resp.status_code}, {resp.text}")

# ─── Main Environment Check ────────────────────────────────────────────────────

def check_environment(is_cloud=False):
    """
    Signs in to either Tableau Cloud or On-Prem,
    fetches failures, sends alerts, and signs out.
    """
    key_prefix = 'Cloud' if is_cloud else 'OnPrem'
    server_url = cf[f"{key_prefix}ServerURL"]
    token_name = cf[f"{key_prefix}TokenName"]
    token_sec  = cf[f"{key_prefix}TokenSecret"]
    site_id    = cf.get(f"{key_prefix}SiteID", "")

    env_label = "Tableau Cloud" if is_cloud else "Tableau On-Prem"
    server    = None

    try:
        server = sign_in(server_url, token_name, token_sec, site_id)
        logging.info(f"Connected to {env_label}")

        # Collect failures
        fails_extract = get_failed_jobs(server, JOB_EXTRACT_TYPE)
        fails_flow    = get_failed_jobs(server, JOB_FLOW_TYPE)

        # Notify
        send_teams_notification(fails_extract + fails_flow, env_label)

    except Exception as e:
        logging.error(f"Issue in {env_label}: {e}")

    finally:
        if server:
            sign_out(server)

# ─── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    check_environment(is_cloud=True)
    check_environment(is_cloud=False)
    sys.exit(0)
