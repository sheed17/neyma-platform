"""
Google Ads API integration for real spend and performance data.

When a user connects their Google Ads account, this module fetches:
- Actual monthly ad spend
- Click and impression counts
- Cost per click (CPC)
- Conversion counts and cost per conversion
- Campaign-level breakdown

Configuration (environment variables):
  GOOGLE_ADS_DEVELOPER_TOKEN=<developer token>
  GOOGLE_ADS_CLIENT_ID=<OAuth client ID>
  GOOGLE_ADS_CLIENT_SECRET=<OAuth client secret>
  GOOGLE_ADS_REFRESH_TOKEN=<OAuth refresh token>
  GOOGLE_ADS_CUSTOMER_ID=<customer ID, e.g. "123-456-7890">
  GOOGLE_ADS_LOGIN_CUSTOMER_ID=<MCC customer ID if applicable>

Falls back gracefully when not configured.
"""

import os
import logging
from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")

_gads_available: Optional[bool] = None


def _check_gads_deps() -> bool:
    """Lazy-check whether google-ads library is installed."""
    global _gads_available
    if _gads_available is not None:
        return _gads_available
    try:
        from google.ads.googleads.client import GoogleAdsClient  # noqa: F401
        _gads_available = True
    except ImportError:
        _gads_available = False
        logger.info("google-ads not installed — Google Ads API integration disabled")
    return _gads_available


def is_google_ads_api_available() -> bool:
    """Return True if Google Ads API is configured and the SDK is installed."""
    required = [GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID,
                GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN,
                GOOGLE_ADS_CUSTOMER_ID]
    if not all(required):
        return False
    return _check_gads_deps()


def _get_client():
    """Create a GoogleAdsClient from environment configuration."""
    from google.ads.googleads.client import GoogleAdsClient

    config = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        config["login_customer_id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID.replace("-", "")

    return GoogleAdsClient.load_from_dict(config)


def fetch_google_ads_spend(
    customer_id: Optional[str] = None,
    days: int = 30,
) -> Optional[Dict[str, Any]]:
    """
    Fetch Google Ads spend and performance data for the last N days.

    Returns a dict with:
        - total_spend_micros (in micros, divide by 1_000_000 for dollars)
        - total_spend_usd
        - total_clicks
        - total_impressions
        - total_conversions
        - avg_cpc_usd
        - cost_per_conversion_usd
        - campaigns (list of campaign-level data)
    """
    if not is_google_ads_api_available():
        return None

    cid = (customer_id or GOOGLE_ADS_CUSTOMER_ID or "").replace("-", "")

    try:
        client = _get_client()
        ga_service = client.get_service("GoogleAdsService")

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days)

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                metrics.cost_micros,
                metrics.clicks,
                metrics.impressions,
                metrics.conversions,
                metrics.cost_per_conversion,
                metrics.average_cpc
            FROM campaign
            WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
                AND campaign.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
        """

        response = ga_service.search(customer_id=cid, query=query)

        total_spend_micros = 0
        total_clicks = 0
        total_impressions = 0
        total_conversions = 0.0
        campaigns: List[Dict] = []

        for row in response:
            campaign = row.campaign
            metrics = row.metrics

            spend_micros = metrics.cost_micros
            clicks = metrics.clicks
            impressions = metrics.impressions
            conversions = metrics.conversions

            total_spend_micros += spend_micros
            total_clicks += clicks
            total_impressions += impressions
            total_conversions += conversions

            campaigns.append({
                "id": str(campaign.id),
                "name": campaign.name,
                "status": campaign.status.name,
                "spend_usd": round(spend_micros / 1_000_000, 2),
                "clicks": clicks,
                "impressions": impressions,
                "conversions": round(conversions, 1),
                "avg_cpc_usd": round(metrics.average_cpc / 1_000_000, 2) if metrics.average_cpc else None,
            })

        total_spend_usd = round(total_spend_micros / 1_000_000, 2)
        avg_cpc = round(total_spend_micros / max(total_clicks, 1) / 1_000_000, 2)
        cost_per_conv = round(total_spend_micros / max(total_conversions, 1) / 1_000_000, 2) if total_conversions > 0 else None

        return {
            "provider": "google_ads_api",
            "customer_id": cid,
            "days": days,
            "total_spend_micros": total_spend_micros,
            "total_spend_usd": total_spend_usd,
            "total_clicks": total_clicks,
            "total_impressions": total_impressions,
            "total_conversions": round(total_conversions, 1),
            "avg_cpc_usd": avg_cpc,
            "cost_per_conversion_usd": cost_per_conv,
            "campaigns": campaigns,
        }

    except Exception as exc:
        logger.warning("Google Ads API fetch failed: %s", exc)
        return None


def augment_lead_with_google_ads_api(lead: Dict, customer_id: Optional[str] = None) -> Dict:
    """
    Augment a lead dict with real Google Ads spend data.

    Stores results under lead["google_ads_api_data"] and upgrades
    spend estimate signals when real data is available.
    """
    if not is_google_ads_api_available():
        return lead

    ads_data = fetch_google_ads_spend(customer_id=customer_id)
    if not ads_data:
        return lead

    lead["google_ads_api_data"] = ads_data

    spend = ads_data.get("total_spend_usd", 0)
    if spend > 0:
        lead["signal_real_paid_spend"] = spend
        lead["signal_runs_paid_ads"] = True

    clicks = ads_data.get("total_clicks", 0)
    if clicks > 0:
        lead["signal_real_paid_traffic"] = clicks

    conversions = ads_data.get("total_conversions", 0)
    if conversions > 0:
        lead["signal_real_paid_conversions"] = conversions

    logger.info(
        "Google Ads API: $%.2f spend, %d clicks, %.1f conversions (%d days)",
        spend, clicks, conversions, ads_data.get("days", 30),
    )
    return lead
