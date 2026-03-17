#!/usr/bin/env python3
"""
Evaluate labeled service-page benchmark CSV.

Computes:
- exact status accuracy
- class precision/recall/F1 for status labels
- binary dedicated-page precision/recall/F1 (using label_dedicated_page_exists)
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from typing import Dict, List, Tuple


STATUSES = ["strong", "moderate", "weak", "weak_stub_page", "missing"]


def _norm_status(v: str) -> str:
    t = (v or "").strip().lower()
    return t if t in STATUSES else ""


def _norm_bool(v: str) -> str:
    t = (v or "").strip().lower()
    if t in {"1", "true", "yes", "y"}:
        return "true"
    if t in {"0", "false", "no", "n"}:
        return "false"
    return ""


def _prf(tp: int, fp: int, fn: int) -> Tuple[float, float, float]:
    p = (tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    r = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    f1 = (2 * p * r / (p + r)) if (p + r) > 0 else 0.0
    return p, r, f1


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate service-page benchmark labels.")
    parser.add_argument("--csv", required=True, help="CSV file from generate_service_page_benchmark.py")
    args = parser.parse_args()

    with open(args.csv, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    usable: List[Dict[str, str]] = []
    skipped = 0
    for r in rows:
        pred = _norm_status(r.get("prediction_status", ""))
        lab = _norm_status(r.get("label_status", ""))
        if not pred or not lab:
            skipped += 1
            continue
        rr = dict(r)
        rr["prediction_status"] = pred
        rr["label_status"] = lab
        rr["label_dedicated_page_exists"] = _norm_bool(r.get("label_dedicated_page_exists", ""))
        usable.append(rr)

    if not usable:
        raise SystemExit("No labeled rows found. Fill label_status first.")

    total = len(usable)
    exact = sum(1 for r in usable if r["prediction_status"] == r["label_status"])
    print(f"Rows total: {len(rows)}")
    print(f"Rows usable (labeled): {total}")
    print(f"Rows skipped (unlabeled/invalid): {skipped}")
    print(f"Exact status accuracy: {exact}/{total} = {exact/total:.3f}")

    cm = defaultdict(Counter)
    for r in usable:
        cm[r["label_status"]][r["prediction_status"]] += 1

    print("\nConfusion matrix (label -> prediction):")
    header = ["label\\pred"] + STATUSES
    print(",".join(header))
    for lab in STATUSES:
        vals = [str(cm[lab][pred]) for pred in STATUSES]
        print(",".join([lab] + vals))

    print("\nPer-class precision/recall/F1:")
    print("class,precision,recall,f1,support")
    for cls in STATUSES:
        tp = cm[cls][cls]
        fp = sum(cm[lab][cls] for lab in STATUSES if lab != cls)
        fn = sum(cm[cls][pred] for pred in STATUSES if pred != cls)
        p, r, f1 = _prf(tp, fp, fn)
        support = sum(cm[cls].values())
        print(f"{cls},{p:.3f},{r:.3f},{f1:.3f},{support}")

    # Binary dedicated-page evaluation
    # predicted dedicated := prediction_status != missing
    # labeled dedicated from label_dedicated_page_exists
    binary_rows = [r for r in usable if r["label_dedicated_page_exists"] in {"true", "false"}]
    if binary_rows:
        tp = fp = fn = tn = 0
        for r in binary_rows:
            pred_pos = r["prediction_status"] != "missing"
            lab_pos = r["label_dedicated_page_exists"] == "true"
            if pred_pos and lab_pos:
                tp += 1
            elif pred_pos and (not lab_pos):
                fp += 1
            elif (not pred_pos) and lab_pos:
                fn += 1
            else:
                tn += 1
        p, rec, f1 = _prf(tp, fp, fn)
        acc = (tp + tn) / len(binary_rows)
        print("\nBinary dedicated-page metrics:")
        print(f"rows={len(binary_rows)} tp={tp} fp={fp} fn={fn} tn={tn}")
        print(f"precision={p:.3f} recall={rec:.3f} f1={f1:.3f} accuracy={acc:.3f}")
    else:
        print("\nBinary dedicated-page metrics: skipped (label_dedicated_page_exists not filled).")


if __name__ == "__main__":
    main()

