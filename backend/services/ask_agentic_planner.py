"""Agentic planner for iterative Ask scans (plan-only LLM; deterministic execution)."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

PLANNER_PROMPT = """You are NeymaScanPlanner.Goal: Iteratively produce a safe, deterministic scan plan that will yield the user's target number of prospects while preserving accuracy constraints.IMPORTANT: You do NOT execute scans. You do NOT browse the web. You ONLY propose plan adjustments. Output MUST be valid JSON only.You receive: (1) normalized_intent (already validated against a criteria registry), (2) telemetry from the last scan iteration (candidate_count, matched_count, breakdown_by_filter, time_elapsed_seconds, accuracy_mode, verification_yield if verified), (3) system_limits (max_radius_miles, max_candidate_cap, max_iterations, max_verify_per_iteration).Your job: Return a JSON plan update that changes ONLY these knobs: radius_miles (1..max_radius_miles), candidate_cap (20..max_candidate_cap), filter_strategy (hard_filters, soft_filters_rank_only, relaxation_order), verification_strategy (if accuracy_mode = verified). Never add criteria not in normalized_intent.criteria. If matched_count is too low, first expand radius and candidate_cap before dropping user-requested criteria. Only move a criterion from hard to soft if it's not the user's core ask. Stop conditions (min_results, max_iterations, max_minutes) must be respected.Output schema:{  \"plan_update\": {    \"radius_miles\": number,    \"candidate_cap\": number,    \"filter_strategy\": {      \"hard_filters\": [string],      \"soft_filters_rank_only\": [string],      \"relaxation_order\": [        { \"action\": \"increase_radius_miles\", \"to\": number } |        { \"action\": \"increase_candidate_cap\", \"to\": number } |        { \"action\": \"move_hard_to_soft\", \"criterion\": string } |        { \"action\": \"drop_soft_preference\", \"criterion\": string }      ]    },    \"verification_strategy\": { \"verify_top_n\": number, \"fallback_if_too_few_verified\": { \"verify_next_n\": number } },    \"stop_when\": { \"min_results\": number, \"max_iterations\": number, \"max_minutes\": number }  },  \"stop_reason\": string|null}Return JSON only."""


def _extract_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start:end + 1]
    obj = json.loads(raw)
    return obj if isinstance(obj, dict) else {}


def planner_llm_update(
    normalized_intent: Dict[str, Any],
    telemetry: Dict[str, Any],
    system_limits: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        from openai import OpenAI
    except Exception:
        return {}

    try:
        client = OpenAI()
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        user_msg = (
            "normalized_intent: " + json.dumps(normalized_intent, ensure_ascii=False) + "\n"
            "telemetry: " + json.dumps(telemetry, ensure_ascii=False) + "\n"
            "system_limits: " + json.dumps(system_limits, ensure_ascii=False)
        )
        r = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": PLANNER_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        txt = (r.choices[0].message.content or "") if getattr(r, "choices", None) else ""
        out = _extract_json(txt)
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}
