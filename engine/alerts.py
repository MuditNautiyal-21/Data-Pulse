"""
DataPulse Alerts — sends Slack messages when checks fail.
"""
import requests
import yaml
from datetime import datetime


def load_alert_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f).get("alerts", {})


def send_slack(webhook_url, message):
    """Send a message to Slack."""
    try:
        r = requests.post(webhook_url, json={"text": message})
        if r.status_code == 200:
            print("  Slack alert sent!")
        else:
            print(f"  Slack failed: {r.status_code}")
    except Exception as e:
        print(f"  Slack error: {e}")


def alert_on_failures(results, source):
    """Send Slack alert if any checks failed."""
    config = load_alert_config()
    slack = config.get("slack", {})
    if not slack.get("enabled"):
        return
    url = slack.get("webhook_url", "")
    if not url:
        return

    failures = [r for r in results if not r["passed"]]
    if not failures:
        return

    source_name = source.split("/")[-1]
    lines = [f"*DataPulse Alert: {len(failures)} check(s) failed on {source_name}*\n"]
    for f in failures:
        icon = "🔴" if f["severity"] == "critical" else "🟡"
        lines.append(f"{icon} {f['name']}: {f['details']}")

    send_slack(url, "\n".join(lines))