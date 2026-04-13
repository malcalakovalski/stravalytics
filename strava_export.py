"""
Strava Activity Exporter

Exports running activities (summary + second-by-second streams) and
soccer activities to CSV files. Handles OAuth2 with token persistence.
"""

import csv
import json
import os
import sys
import time
import webbrowser
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import requests
from dotenv import load_dotenv


# ── Configuration ───────────────────────────────────────────────────

load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")

TOKEN_FILE = Path("strava_tokens.json")
REDIRECT_PORT = 8000
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/callback"

AUTH_URL = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"
API_BASE = "https://www.strava.com/api/v3"

MONTHS_BACK = 18
STREAM_COUNT = 15  # Recent runs to pull streams for


# ── OAuth2 Authentication ──────────────────────────────────────────


class _CallbackHandler(BaseHTTPRequestHandler):
    """Captures the OAuth redirect code from Strava."""

    code = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            _CallbackHandler.code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>Authorized! You can close this tab.</h1>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Authorization failed.")

    def log_message(self, *args):
        pass  # Suppress HTTP server logging


def _authorize():
    """Open browser for Strava OAuth, return authorization code."""
    _CallbackHandler.code = None
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "activity:read_all",
        "approval_prompt": "auto",
    }
    url = f"{AUTH_URL}?{urlencode(params)}"

    print("\nOpening browser for authorization...")
    print(f"If it doesn't open, visit:\n{url}\n")
    webbrowser.open(url)

    try:
        server = HTTPServer(("localhost", REDIRECT_PORT), _CallbackHandler)
    except OSError:
        sys.exit(
            f"Port {REDIRECT_PORT} is in use. Stop the process using it "
            "or change REDIRECT_PORT in the script."
        )
    server.handle_request()

    if not _CallbackHandler.code:
        sys.exit("Failed to get authorization code.")
    return _CallbackHandler.code


def _token_request(**data):
    """POST to Strava token endpoint."""
    data.update({"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET})
    resp = requests.post(TOKEN_URL, data=data)
    resp.raise_for_status()
    return resp.json()


def get_token():
    """Return a valid access token. Refreshes or re-authorizes as needed."""
    if TOKEN_FILE.exists():
        tokens = json.loads(TOKEN_FILE.read_text())
        if tokens["expires_at"] > time.time():
            return tokens["access_token"]

        print("Token expired, refreshing...")
        tokens = _token_request(
            grant_type="refresh_token",
            refresh_token=tokens["refresh_token"],
        )
        TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
        return tokens["access_token"]

    # First-time authorization
    code = _authorize()
    tokens = _token_request(grant_type="authorization_code", code=code)
    TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
    return tokens["access_token"]


# ── API Helpers ─────────────────────────────────────────────────────

_request_count = 0


def api_get(token, path, params=None):
    """GET from Strava API with rate-limit handling and 429 retry."""
    global _request_count
    url = f"{API_BASE}/{path}"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers, params=params)
    _request_count += 1

    # Proactive rate-limit check via response headers
    usage = resp.headers.get("X-RateLimit-Usage", "")
    if usage:
        short_usage = int(usage.split(",")[0])
        if short_usage >= 90:
            print(f"\n  Rate limit nearing cap ({short_usage}/100). Pausing 2 min...")
            time.sleep(120)

    # Retry once on 429
    if resp.status_code == 429:
        wait = int(resp.headers.get("Retry-After", 300))
        print(f"\n  Rate limited. Retrying in {wait}s...")
        time.sleep(wait)
        resp = requests.get(url, headers=headers, params=params)
        _request_count += 1

    resp.raise_for_status()
    time.sleep(0.4)  # Courtesy delay between requests
    return resp.json()


def fetch_all_activities(token, after_ts):
    """Fetch all activities after a Unix timestamp, handling pagination."""
    activities = []
    page = 1
    while True:
        batch = api_get(token, "athlete/activities", {
            "after": int(after_ts),
            "page": page,
            "per_page": 200,
        })
        if not batch:
            break
        activities.extend(batch)
        if len(batch) < 200:
            break
        page += 1
    return activities


# ── Conversion Helpers ──────────────────────────────────────────────


def m_to_mi(meters):
    """Meters to miles."""
    return round(meters / 1609.344, 2)


def fmt_duration(seconds):
    """Seconds to H:MM:SS or M:SS string."""
    h, r = divmod(int(seconds), 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def fmt_pace(mps):
    """Meters/second to min:sec per mile. Returns '' if no data."""
    if not mps:
        return ""
    spm = 1609.344 / mps
    return f"{int(spm // 60)}:{int(spm % 60):02d}"


def parse_date(iso_str):
    """Parse Strava ISO date string to datetime."""
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


# ── Row Formatters ──────────────────────────────────────────────────


def run_row(activity, zones=None):
    """Format a running activity into a summary CSV row."""
    row = {
        "date": parse_date(activity["start_date_local"]).strftime("%Y-%m-%d %H:%M"),
        "name": activity.get("name", ""),
        "distance_miles": m_to_mi(activity.get("distance", 0)),
        "duration": fmt_duration(activity.get("moving_time", 0)),
        "duration_seconds": activity.get("moving_time", 0),
        "avg_pace_per_mile": fmt_pace(activity.get("average_speed")),
        "avg_heartrate": activity.get("average_heartrate", ""),
        "max_heartrate": activity.get("max_heartrate", ""),
        "elevation_gain_ft": round(activity.get("total_elevation_gain", 0) * 3.28084, 1),
        "activity_id": activity["id"],
    }

    # Add HR zone distribution (time in each zone) if available
    if zones:
        for zone_set in zones:
            if zone_set.get("type") == "heartrate":
                for i, bucket in enumerate(zone_set.get("distribution_buckets", []), 1):
                    row[f"hr_zone_{i}_sec"] = bucket.get("time", 0)

    return row


def soccer_row(activity, detail=None):
    """Format a soccer activity into a summary CSV row."""
    calories = ""
    if detail and "calories" in detail:
        calories = detail["calories"]
    elif "calories" in activity:
        calories = activity["calories"]

    return {
        "date": parse_date(activity["start_date_local"]).strftime("%Y-%m-%d %H:%M"),
        "name": activity.get("name", ""),
        "duration": fmt_duration(activity.get("moving_time", 0)),
        "duration_seconds": activity.get("moving_time", 0),
        "distance_miles": m_to_mi(activity.get("distance", 0)),
        "avg_heartrate": activity.get("average_heartrate", ""),
        "max_heartrate": activity.get("max_heartrate", ""),
        "calories": calories,
    }


def stream_rows(stream_data):
    """Convert Strava stream response into list of per-second row dicts."""
    if not stream_data:
        return []

    streams = {s["type"]: s["data"] for s in stream_data}
    if "time" not in streams:
        return []

    n = len(streams["time"])
    dist = streams.get("distance", [])
    vs = streams.get("velocity_smooth", [])
    hr = streams.get("heartrate", [])
    alt = streams.get("altitude", [])

    rows = []
    for i in range(n):
        rows.append({
            "elapsed_seconds": streams["time"][i],
            "distance_miles": m_to_mi(dist[i]) if i < len(dist) and dist[i] is not None else "",
            "pace_per_mile": fmt_pace(vs[i]) if i < len(vs) and vs[i] else "",
            "heartrate": hr[i] if i < len(hr) and hr[i] is not None else "",
            "altitude_ft": round(alt[i] * 3.28084, 1) if i < len(alt) and alt[i] is not None else "",
        })
    return rows


# ── CSV Writing ─────────────────────────────────────────────────────


def write_csv(path, rows, fieldnames=None):
    """Write list of dicts to CSV. Returns row count."""
    if not rows:
        return 0
    fieldnames = fieldnames or list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


# ── Main ────────────────────────────────────────────────────────────


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        sys.exit(
            "Set STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET in a .env file.\n"
            "See .env.example for the template."
        )

    # Authenticate
    print("Authenticating with Strava...")
    token = get_token()
    print("Authenticated.\n")

    # Calculate date cutoff
    cutoff = datetime.now() - timedelta(days=int(MONTHS_BACK * 30.44))
    print(f"Fetching activities since {cutoff.strftime('%Y-%m-%d')}...")

    # Fetch and filter
    all_activities = fetch_all_activities(token, cutoff.timestamp())
    runs = [a for a in all_activities if a.get("type") == "Run"]
    soccer = [a for a in all_activities if a.get("type") in ("Soccer", "Football")]
    print(f"Found {len(all_activities)} activities total: {len(runs)} runs, {len(soccer)} soccer\n")

    # ── Process runs (summary + HR zones) ───────────────────────────
    print("Processing runs...")
    run_data = []
    for i, activity in enumerate(runs, 1):
        # Only fetch zones for activities that have HR data
        zones = None
        if activity.get("has_heartrate"):
            try:
                zones = api_get(token, f"activities/{activity['id']}/zones")
            except requests.HTTPError:
                pass  # Zone data unavailable, skip

        run_data.append(run_row(activity, zones))
        if i % 25 == 0:
            print(f"  {i}/{len(runs)} processed")

    # Filter out junk runs (accidental starts, < 0.5 mi)
    run_data = [r for r in run_data if r["distance_miles"] >= 0.5]
    run_data.sort(key=lambda r: r["date"], reverse=True)

    # Build fieldnames: base columns + however many HR zone columns exist
    base_fields = [
        "date", "name", "distance_miles", "duration", "duration_seconds",
        "avg_pace_per_mile", "avg_heartrate", "max_heartrate", "elevation_gain_ft",
    ]
    zone_count = max(
        (sum(1 for k in r if k.startswith("hr_zone_")) for r in run_data),
        default=0,
    )
    zone_fields = [f"hr_zone_{z}_sec" for z in range(1, zone_count + 1)]
    run_fields = base_fields + zone_fields + ["activity_id"]

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    n = write_csv(data_dir / "strava-runs-summary.csv", run_data, run_fields)
    print(f"Wrote data/strava-runs-summary.csv ({n} rows)")

    # ── Process soccer (summary + calories from detail endpoint) ────
    if soccer:
        print("\nProcessing soccer activities...")
        soccer_data = []
        for activity in soccer:
            # Fetch detail to get calories (not in list response)
            detail = None
            try:
                detail = api_get(token, f"activities/{activity['id']}")
            except requests.HTTPError:
                pass
            soccer_data.append(soccer_row(activity, detail))

        soccer_data.sort(key=lambda r: r["date"], reverse=True)
        n = write_csv(data_dir / "strava-soccer-summary.csv", soccer_data)
        print(f"Wrote data/strava-soccer-summary.csv ({n} rows)")
    else:
        soccer_data = []

    # ── Fetch streams for most recent runs ──────────────────────────
    recent = sorted(runs, key=lambda a: a["start_date_local"], reverse=True)[:STREAM_COUNT]
    if recent:
        streams_dir = data_dir / "activity-streams"
        streams_dir.mkdir(exist_ok=True)
        print(f"\nFetching streams for {len(recent)} most recent runs...")

        for i, activity in enumerate(recent, 1):
            date_str = parse_date(activity["start_date_local"]).strftime("%Y-%m-%d")
            try:
                data = api_get(token, f"activities/{activity['id']}/streams", {
                    "keys": "time,distance,velocity_smooth,heartrate,altitude",
                    "key_type": "time",
                })
            except requests.HTTPError:
                print(f"  [{i}/{len(recent)}] No stream data for {activity['id']}")
                continue

            rows = stream_rows(data)
            if rows:
                fname = f"{date_str}-{activity['id']}.csv"
                write_csv(streams_dir / fname, rows)
                print(f"  [{i}/{len(recent)}] {fname} ({len(rows)} points)")
            else:
                print(f"  [{i}/{len(recent)}] Empty stream for {activity['id']}")

    # ── Summary stats ───────────────────────────────────────────────
    print("\n" + "=" * 50)
    print("EXPORT COMPLETE")
    print("=" * 50)

    if run_data:
        miles = sum(r["distance_miles"] for r in run_data)
        dates = [r["date"] for r in run_data]
        print(f"\nRunning:  {len(run_data)} runs, {miles:,.1f} miles")
        print(f"  Range:  {min(dates)} to {max(dates)}")

    if soccer_data:
        miles = sum(r["distance_miles"] for r in soccer_data)
        dates = [r["date"] for r in soccer_data]
        print(f"\nSoccer:   {len(soccer_data)} activities, {miles:,.1f} miles")
        print(f"  Range:  {min(dates)} to {max(dates)}")

    print(f"\nAPI requests: {_request_count}")
    print("=" * 50)


if __name__ == "__main__":
    main()
