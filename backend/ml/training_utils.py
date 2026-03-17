"""Helpers for market-aware lead-quality dataset building and training."""

from __future__ import annotations

import random
from typing import Any, Dict, Mapping, Sequence, Tuple


def normalize_market(city: Any, state: Any) -> Tuple[str, str, str]:
    city_norm = " ".join(str(city or "").strip().split()).lower()
    state_norm = str(state or "").strip().upper()
    if not city_norm and not state_norm:
        return "", "", ""
    market_key = f"{city_norm}|{state_norm}" if state_norm else city_norm
    return city_norm, state_norm, market_key


def parse_market_spec(spec: str) -> Dict[str, str]:
    raw = " ".join(str(spec or "").strip().split())
    if not raw:
        raise ValueError("Market cannot be empty.")
    if "," not in raw:
        raise ValueError(f"Invalid market '{spec}'. Use 'City, ST'.")
    city_raw, state_raw = [part.strip() for part in raw.rsplit(",", 1)]
    city_norm, state_norm, market_key = normalize_market(city_raw, state_raw)
    if not city_norm or not state_norm:
        raise ValueError(f"Invalid market '{spec}'. Use 'City, ST'.")
    return {
        "raw": f"{city_raw}, {state_norm}",
        "city": city_raw,
        "state": state_norm,
        "city_norm": city_norm,
        "state_norm": state_norm,
        "market_key": market_key,
    }


def load_market_specs(
    market_values: Sequence[str] | None = None,
    market_file: str | None = None,
) -> list[Dict[str, str]]:
    specs = list(market_values or [])
    if market_file:
        with open(market_file, "r", encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                specs.append(stripped)
    seen: set[str] = set()
    parsed: list[Dict[str, str]] = []
    for spec in specs:
        item = parse_market_spec(spec)
        key = item["market_key"]
        if key in seen:
            continue
        seen.add(key)
        parsed.append(item)
    return parsed


def market_key_from_row(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("market_key") or "").strip().lower()
    if explicit:
        return explicit
    _, _, market_key = normalize_market(row.get("city"), row.get("state"))
    return market_key


def group_value_from_row(row: Mapping[str, Any], group_by: str, idx: int) -> str:
    if group_by == "market":
        return market_key_from_row(row) or str(row.get("place_id") or f"row-{idx}")
    return str(row.get("place_id") or market_key_from_row(row) or f"row-{idx}")


def split_keys(keys: Sequence[str], rng: random.Random) -> Tuple[list[str], list[str], list[str]]:
    values = list(keys)
    rng.shuffle(values)
    n = len(values)
    if n <= 0:
        return [], [], []
    if n == 1:
        return values, [], []
    if n == 2:
        return [values[0]], [values[1]], []
    train_n = max(1, int(round(n * 0.70)))
    val_n = max(1, int(round(n * 0.15)))
    test_n = n - train_n - val_n
    if test_n <= 0:
        test_n = 1
        if train_n > val_n and train_n > 1:
            train_n -= 1
        elif val_n > 1:
            val_n -= 1
    train_keys = values[:train_n]
    val_keys = values[train_n:train_n + val_n]
    test_keys = values[train_n + val_n:]
    return train_keys, val_keys, test_keys


def split_keys_two_way(keys: Sequence[str], rng: random.Random, primary_share: float = 0.85) -> Tuple[list[str], list[str]]:
    values = list(keys)
    rng.shuffle(values)
    n = len(values)
    if n <= 0:
        return [], []
    if n == 1:
        return values, []
    primary_n = max(1, int(round(n * primary_share)))
    if primary_n >= n:
        primary_n = n - 1
    return values[:primary_n], values[primary_n:]


def group_split_rows(
    rows: Sequence[Mapping[str, Any]],
    seed: int,
    target_column: str,
    *,
    group_by: str = "place_id",
    holdout_groups: Sequence[str] | None = None,
) -> Tuple[list[int], list[int], list[int], Dict[str, Any]]:
    groups: Dict[str, list[int]] = {}
    for idx, row in enumerate(rows):
        groups.setdefault(group_value_from_row(row, group_by, idx), []).append(idx)

    normalized_holdouts = {str(item or "").strip().lower() for item in (holdout_groups or []) if str(item or "").strip()}
    unknown_holdouts = sorted(normalized_holdouts - set(groups))
    if unknown_holdouts:
        raise ValueError(f"Holdout groups not found in dataset: {unknown_holdouts}")

    rng = random.Random(seed)
    positive_keys: list[str] = []
    negative_keys: list[str] = []
    for key, idxs in groups.items():
        group_positive = any(int(float(rows[idx].get(target_column) or 0)) == 1 for idx in idxs)
        if group_positive:
            positive_keys.append(key)
        else:
            negative_keys.append(key)

    holdout_set = set(normalized_holdouts)
    if holdout_set:
        remaining_positive = [key for key in positive_keys if key not in holdout_set]
        remaining_negative = [key for key in negative_keys if key not in holdout_set]
        pos_train, pos_val = split_keys_two_way(remaining_positive, rng)
        neg_train, neg_val = split_keys_two_way(remaining_negative, rng)
        train_keys = set(pos_train + neg_train)
        val_keys = set(pos_val + neg_val)
        test_keys = holdout_set
    else:
        pos_train, pos_val, pos_test = split_keys(positive_keys, rng)
        neg_train, neg_val, neg_test = split_keys(negative_keys, rng)
        train_keys = set(pos_train + neg_train)
        val_keys = set(pos_val + neg_val)
        test_keys = set(pos_test + neg_test)

    train_idx = [idx for key in train_keys for idx in groups[key]]
    val_idx = [idx for key in val_keys for idx in groups[key]]
    test_idx = [idx for key in test_keys for idx in groups[key]]
    split_meta = {
        "group_by": group_by,
        "train_group_count": len(train_keys),
        "val_group_count": len(val_keys),
        "test_group_count": len(test_keys),
        "holdout_groups": sorted(test_keys) if holdout_set else [],
        "train_groups": sorted(train_keys),
        "val_groups": sorted(val_keys),
        "test_groups": sorted(test_keys),
    }
    return train_idx, val_idx, test_idx, split_meta
