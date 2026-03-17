"""
Google Analytics 4 (GA4) integration for real traffic and conversion data.

When a user connects their GA4 property, this module fetches:
- Real organic/paid session counts
- Conversion events (form submissions, phone clicks, booking completions)
- Top landing pages by session volume
- Bounce rate and engagement metrics

Configuration (environment variables):
  GA4_CREDENTIALS_PATH=<path to service account JSON>
  GA4_PROPERTY_ID=<GA4 property ID, e.g. "properties/123456789">

Alternatively, users can provide credentials via the settings page (future).
Falls back gracefully when not configured.
"""

import os
import json
import logging
from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

GA4_CREDENTIALS_PATH = os.getenv("GA4_CREDENTIALS_PATH")
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")

_ga4_available: Optional[bool] = None


def _check_ga4_deps() -> bool:
    """Lazy-check whether google-analytics-data library is installed."""
    global _ga4_available
    if _ga4_available is not None:
        return _ga4_available
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient  # noqa: F401
        _ga4_available = True
    except ImportError:
        _ga4_available = False
        logger.info("google-analytics-data not installed — GA4 integration disabled")
    return _ga4_available


def is_ga4_available() -> bool:
    """Return True if GA4 is both configured and the SDK is installed."""
    if not GA4_CREDENTIALS_PATH or not GA4_PROPERTY_ID:
        return False
    if not os.path.exists(GA4_CREDENTIALS_PATH):
        return False
    return _check_ga4_deps()


def _get_client():
    """Create a GA4 BetaAnalyticsDataClient from service account credentials."""
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GA4_CREDENTIALS_PATH
    return BetaAnalyticsDataClient()


def fetch_ga4_traffic(
    property_id: Optional[str] = None,
    days: int = 30,
    domain_filter: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch traffic and conversion data from GA4 for the last N days.

    Returns a dict with:
        - total_sessions
        - organic_sessions
        - paid_sessions
        - direct_sessions
        - referral_sessions
        - total_users
        - conversions (dict of event_name -> count)
        - top_landing_pages (list of {page, sessions})
        - bounce_rate
        - avg_session_duration_seconds
    """
    if not is_ga4_available():
        return None

    prop_id = property_id or GA4_PROPERTY_ID

    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            RunReportRequest,
            DateRange,
            Dimension,
            Metric,
            FilterExpression,
            Filter,
        )

        client = _get_client()
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days)

        # Traffic by channel grouping
        channel_request = RunReportRequest(
            property=prop_id,
            date_ranges=[DateRange(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )],
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
            ],
        )
        channel_response = client.run_report(channel_request)

        total_sessions = 0
        organic_sessions = 0
        paid_sessions = 0
        direct_sessions = 0
        referral_sessions = 0
        total_users = 0
        bounce_rates = []
        durations = []

        for row in channel_response.rows:
            channel = row.dimension_values[0].value.lower()
            sessions = int(row.metric_values[0].value)
            users = int(row.metric_values[1].value)
            bounce = float(row.metric_values[2].value)
            duration = float(row.metric_values[3].value)

            total_sessions += sessions
            total_users += users
            bounce_rates.append((bounce, sessions))
            durations.append((duration, sessions))

            if "organic" in channel:
                organic_sessions += sessions
            elif "paid" in channel:
                paid_sessions += sessions
            elif "direct" in channel:
                direct_sessions += sessions
            elif "referral" in channel:
                referral_sessions += sessions

        weighted_bounce = sum(b * s for b, s in bounce_rates) / max(total_sessions, 1)
        weighted_duration = sum(d * s for d, s in durations) / max(total_sessions, 1)

        # Conversion events
        conversion_request = RunReportRequest(
            property=prop_id,
            date_ranges=[DateRange(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )],
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount")],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    in_list_filter=Filter.InListFilter(values=[
                        "generate_lead",
                        "form_submit",
                        "phone_click",
                        "book_appointment",
                        "schedule",
                        "contact_form_submission",
                        "purchase",
                    ]),
                ),
            ),
        )
        try:
            conv_response = client.run_report(conversion_request)
            conversions = {}
            for row in conv_response.rows:
                event = row.dimension_values[0].value
                count = int(row.metric_values[0].value)
                conversions[event] = count
        except Exception:
            conversions = {}

        # Top landing pages
        pages_request = RunReportRequest(
            property=prop_id,
            date_ranges=[DateRange(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )],
            dimensions=[Dimension(name="landingPage")],
            metrics=[Metric(name="sessions")],
            limit=20,
        )
        try:
            pages_response = client.run_report(pages_request)
            top_pages = [
                {
                    "page": row.dimension_values[0].value,
                    "sessions": int(row.metric_values[0].value),
                }
                for row in pages_response.rows
            ]
        except Exception:
            top_pages = []

        return {
            "provider": "ga4",
            "property_id": prop_id,
            "days": days,
            "total_sessions": total_sessions,
            "organic_sessions": organic_sessions,
            "paid_sessions": paid_sessions,
            "direct_sessions": direct_sessions,
            "referral_sessions": referral_sessions,
            "total_users": total_users,
            "conversions": conversions,
            "top_landing_pages": top_pages,
            "bounce_rate": round(weighted_bounce, 3),
            "avg_session_duration_seconds": round(weighted_duration, 1),
        }

    except Exception as exc:
        logger.warning("GA4 data fetch failed: %s", exc)
        return None


def augment_lead_with_ga4(lead: Dict, property_id: Optional[str] = None) -> Dict:
    """
    Augment a lead dict with real GA4 traffic and conversion data.

    Stores results under lead["ga4_data"] and upgrades traffic signals.
    """
    if not is_ga4_available():
        return lead

    ga4_data = fetch_ga4_traffic(property_id=property_id)
    if not ga4_data:
        return lead

    lead["ga4_data"] = ga4_data

    organic = ga4_data.get("organic_sessions", 0)
    if organic > 0:
        lead["signal_real_organic_traffic"] = organic
        lead["signal_traffic_source"] = "ga4"

    paid = ga4_data.get("paid_sessions", 0)
    if paid > 0:
        lead["signal_real_paid_traffic"] = paid

    total_conversions = sum(ga4_data.get("conversions", {}).values())
    if total_conversions > 0:
        lead["signal_ga4_conversions_30d"] = total_conversions
        lead["signal_ga4_conversion_events"] = ga4_data["conversions"]

    logger.info(
        "GA4 data: %d total sessions, %d organic, %d paid, %d conversions",
        ga4_data.get("total_sessions", 0),
        organic, paid, total_conversions,
    )
    return lead
