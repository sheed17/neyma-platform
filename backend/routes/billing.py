"""
Stripe billing endpoints for Neyma Pro.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import stripe
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from backend.access import require_signed_in, request_user
from pipeline.db import (
    get_user_by_stripe_customer_id,
    get_user_by_stripe_subscription_id,
    sync_user_billing,
)

router = APIRouter(prefix="/billing", tags=["billing"])


def _stripe_configured() -> tuple[str, str, str]:
    secret_key = (os.getenv("STRIPE_SECRET_KEY") or "").strip()
    price_id = (os.getenv("STRIPE_PRO_PRICE_ID") or "").strip()
    app_base_url = (os.getenv("APP_BASE_URL") or "").strip().rstrip("/")
    if not secret_key or not price_id or not app_base_url:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "BILLING_UNAVAILABLE",
                "message": "Billing isn't ready right now. Please try again shortly.",
                "recommended_cta": None,
            },
        )
    stripe.api_key = secret_key
    return secret_key, price_id, app_base_url


def _period_end_iso(subscription: stripe.Subscription | dict[str, Any] | None) -> str | None:
    if not subscription:
        return None
    value = subscription.get("current_period_end") if isinstance(subscription, dict) else getattr(subscription, "current_period_end", None)
    if not value:
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()


def _plan_tier_for_status(status: str | None) -> str:
    normalized = str(status or "").strip().lower()
    return "pro" if normalized in {"active", "trialing"} else "free"


def _sync_subscription(user_id: int, subscription: stripe.Subscription | dict[str, Any], customer_id: str | None = None):
    if isinstance(subscription, dict):
        items = ((subscription.get("items") or {}).get("data") or [])
        first_item = items[0] if items else {}
        price_id = ((first_item or {}).get("price") or {}).get("id")
        subscription_id = subscription.get("id")
        status = subscription.get("status")
        customer_value = customer_id or subscription.get("customer")
    else:
        items = getattr(getattr(subscription, "items", None), "data", []) or []
        first_item = items[0] if items else None
        price = getattr(first_item, "price", None) if first_item else None
        price_id = getattr(price, "id", None)
        subscription_id = getattr(subscription, "id", None)
        status = getattr(subscription, "status", None)
        customer_value = customer_id or getattr(subscription, "customer", None)
    return sync_user_billing(
        int(user_id),
        stripe_customer_id=str(customer_value) if customer_value else None,
        stripe_subscription_id=str(subscription_id) if subscription_id else None,
        stripe_price_id=str(price_id) if price_id else None,
        stripe_subscription_status=str(status) if status else None,
        stripe_current_period_end=_period_end_iso(subscription),
        plan_tier=_plan_tier_for_status(status),
    )


class CheckoutSessionResponse(BaseModel):
    url: str


class PortalSessionResponse(BaseModel):
    url: str


@router.post("/checkout", response_model=CheckoutSessionResponse)
def create_checkout_session(request: Request):
    require_signed_in(request, message="Create a free account before starting billing.")
    _, price_id, app_base_url = _stripe_configured()
    user = request_user(request)
    email = str(user.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="A verified email is required for billing.")

    customer_id = str(user.get("stripe_customer_id") or "").strip()
    if not customer_id:
        customer = stripe.Customer.create(
            email=email,
            name=(user.get("name") or email.split("@")[0]),
            metadata={"user_id": str(user["id"])},
        )
        customer_id = customer.id
        sync_user_billing(int(user["id"]), stripe_customer_id=customer_id)

    session = stripe.checkout.Session.create(
        mode="subscription",
        customer=customer_id,
        line_items=[{"price": price_id, "quantity": 1}],
        subscription_data={
            "trial_period_days": 7,
        },
        client_reference_id=str(user["id"]),
        metadata={"user_id": str(user["id"])},
        allow_promotion_codes=True,
        success_url=f"{app_base_url}/settings?billing=success",
        cancel_url=f"{app_base_url}/settings?billing=cancel",
    )
    return CheckoutSessionResponse(url=session.url)


@router.post("/customer-portal", response_model=PortalSessionResponse)
def create_customer_portal_session(request: Request):
    require_signed_in(request)
    _secret_key, _price_id, app_base_url = _stripe_configured()
    user = request_user(request)
    customer_id = str(user.get("stripe_customer_id") or "").strip()
    if not customer_id:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "NO_BILLING_ACCOUNT",
                "message": "No billing account was found for this workspace yet.",
                "recommended_cta": "Start 7-day trial",
            },
        )
    session = stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=f"{app_base_url}/settings",
    )
    return PortalSessionResponse(url=session.url)


@router.post("/webhook")
async def handle_webhook(request: Request):
    _secret_key, _price_id, _app_base_url = _stripe_configured()
    webhook_secret = (os.getenv("STRIPE_WEBHOOK_SECRET") or "").strip()
    if not webhook_secret:
        raise HTTPException(status_code=503, detail="Stripe webhook is not configured.")

    payload = await request.body()
    signature = request.headers.get("stripe-signature")
    if not signature:
        raise HTTPException(status_code=400, detail="Missing Stripe signature.")

    try:
        event = stripe.Webhook.construct_event(payload=payload, sig_header=signature, secret=webhook_secret)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid Stripe payload.") from exc
    except stripe.error.SignatureVerificationError as exc:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature.") from exc

    event_type = str(event.get("type") or "")
    obj = event.get("data", {}).get("object", {})

    if event_type == "checkout.session.completed" and obj.get("mode") == "subscription":
        customer_id = obj.get("customer")
        subscription_id = obj.get("subscription")
        user_id = obj.get("client_reference_id") or (obj.get("metadata") or {}).get("user_id")
        if user_id and subscription_id:
            subscription = stripe.Subscription.retrieve(subscription_id)
            _sync_subscription(int(user_id), subscription, customer_id=str(customer_id) if customer_id else None)
        elif customer_id and subscription_id:
            user = get_user_by_stripe_customer_id(str(customer_id))
            if user:
                subscription = stripe.Subscription.retrieve(subscription_id)
                _sync_subscription(int(user["id"]), subscription, customer_id=str(customer_id))

    elif event_type in {"customer.subscription.updated", "customer.subscription.deleted"}:
        customer_id = obj.get("customer")
        subscription_id = obj.get("id")
        user = get_user_by_stripe_customer_id(str(customer_id)) if customer_id else None
        if not user and subscription_id:
            user = get_user_by_stripe_subscription_id(str(subscription_id))
        if user:
            _sync_subscription(int(user["id"]), obj, customer_id=str(customer_id) if customer_id else None)

    elif event_type == "invoice.paid":
        customer_id = obj.get("customer")
        subscription_id = obj.get("subscription")
        user = get_user_by_stripe_customer_id(str(customer_id)) if customer_id else None
        if not user and subscription_id:
            user = get_user_by_stripe_subscription_id(str(subscription_id))
        if user and subscription_id:
            subscription = stripe.Subscription.retrieve(subscription_id)
            _sync_subscription(int(user["id"]), subscription, customer_id=str(customer_id) if customer_id else None)

    return {"received": True}
