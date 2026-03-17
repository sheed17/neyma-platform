#!/usr/bin/env python3
"""
Evaluate labeled capture benchmark CSV.

Expected labeled columns (bool labels use true/false; blanks skipped):
- label_online_booking
- label_contact_form
- label_phone_prominent
- label_phone_clickable
- label_has_cta
- label_form_structure (single_step|multi_step|none|unknown)

Expected prediction columns:
- pred_online_booking
- pred_contact_form
- pred_phone_prominent
- pred_phone_clickable
- pred_has_cta
- pred_form_structure

Optional:
- pred_error (row excluded from metric support when present)
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


def _parse_bool(value: str) -> Optional[bool]:
    s = str(value or "").strip().lower()
    if s in {"1", "true", "yes", "y"}:
        return True
    if s in {"0", "false", "no", "n"}:
        return False
    return None


def _parse_form(value: str) -> Optional[str]:
    s = str(value or "").strip().lower().replace("-", "_")
    if not s:
        return None
    if s in {"single", "single_step"}:
        return "single_step"
    if s in {"multi", "multi_step"}:
        return "multi_step"
    if s in {"none", "not_found", "not detected"}:
        return "none"
    if s in {"unknown", "not_evaluated", "not evaluated"}:
        return "unknown"
    return s


def _prf(tp: int, fp: int, fn: int) -> Tuple[float, float, float]:
    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * p * r / (p + r)) if (p + r) > 0 else 0.0
    return p, r, f1


@dataclass
class BoolMetric:
    name: str
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0
    support: int = 0

    def add(self, label: Optional[bool], pred: Optional[bool]) -> None:
        if label is None or pred is None:
            return
        self.support += 1
        if label and pred:
            self.tp += 1
        elif (not label) and pred:
            self.fp += 1
        elif label and (not pred):
            self.fn += 1
        else:
            self.tn += 1

    def report(self) -> Dict[str, float | int | str]:
        p, r, f1 = _prf(self.tp, self.fp, self.fn)
        acc = (self.tp + self.tn) / self.support if self.support > 0 else 0.0
        return {
            "signal": self.name,
            "precision": round(p, 4),
            "recall": round(r, 4),
            "f1": round(f1, 4),
            "accuracy": round(acc, 4),
            "support": self.support,
            "tp": self.tp,
            "fp": self.fp,
            "fn": self.fn,
            "tn": self.tn,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate labeled capture-signal benchmark CSV.")
    parser.add_argument("--csv", required=True, help="CSV path with labels + predictions.")
    parser.add_argument("--json-out", default="", help="Optional JSON summary output path.")
    parser.add_argument("--show-errors", action="store_true", help="Print rows with pred_error.")
    args = parser.parse_args()

    with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("No rows found.")
        return 1

    bool_pairs = [
        ("online_booking", "label_online_booking", "pred_online_booking"),
        ("contact_form", "label_contact_form", "pred_contact_form"),
        ("phone_prominent", "label_phone_prominent", "pred_phone_prominent"),
        ("phone_clickable", "label_phone_clickable", "pred_phone_clickable"),
        ("has_cta", "label_has_cta", "pred_has_cta"),
    ]
    metrics: Dict[str, BoolMetric] = {name: BoolMetric(name=name) for name, _l, _p in bool_pairs}

    form_support = 0
    form_correct = 0

    all_capture_support = 0
    all_capture_correct = 0

    error_rows: List[Dict[str, str]] = []
    for r in rows:
        if str(r.get("pred_error") or "").strip():
            error_rows.append(r)
            continue

        row_eval_pairs: List[Tuple[bool, bool]] = []
        for name, lcol, pcol in bool_pairs:
            lb = _parse_bool(r.get(lcol, ""))
            pr = _parse_bool(r.get(pcol, ""))
            if lb is not None and pr is not None:
                row_eval_pairs.append((lb, pr))
            metrics[name].add(lb, pr)

        if row_eval_pairs:
            all_capture_support += 1
            if all(lb == pr for lb, pr in row_eval_pairs):
                all_capture_correct += 1

        lf = _parse_form(r.get("label_form_structure", ""))
        pf = _parse_form(r.get("pred_form_structure", ""))
        if lf is not None and pf is not None:
            form_support += 1
            if lf == pf:
                form_correct += 1

    metric_reports = [m.report() for m in metrics.values()]
    macro_f1 = (
        sum(float(m["f1"]) for m in metric_reports if int(m["support"]) > 0)
        / max(1, sum(1 for m in metric_reports if int(m["support"]) > 0))
    )
    micro_tp = sum(m.tp for m in metrics.values())
    micro_fp = sum(m.fp for m in metrics.values())
    micro_fn = sum(m.fn for m in metrics.values())
    micro_p, micro_r, micro_f1 = _prf(micro_tp, micro_fp, micro_fn)

    print("Capture signal metrics")
    print("signal,precision,recall,f1,accuracy,support,tp,fp,fn,tn")
    for rep in metric_reports:
        print(
            f"{rep['signal']},{rep['precision']:.4f},{rep['recall']:.4f},{rep['f1']:.4f},"
            f"{rep['accuracy']:.4f},{rep['support']},{rep['tp']},{rep['fp']},{rep['fn']},{rep['tn']}"
        )

    form_acc = (form_correct / form_support) if form_support > 0 else 0.0
    row_acc = (all_capture_correct / all_capture_support) if all_capture_support > 0 else 0.0

    print("\nSummary")
    print(f"rows_total={len(rows)} rows_with_pred_error={len(error_rows)}")
    print(f"macro_f1={macro_f1:.4f}")
    print(f"micro_precision={micro_p:.4f} micro_recall={micro_r:.4f} micro_f1={micro_f1:.4f}")
    print(f"form_structure_accuracy={form_acc:.4f} (support={form_support})")
    print(f"all_capture_row_accuracy={row_acc:.4f} (support={all_capture_support})")

    if args.show_errors and error_rows:
        print("\nRows with prediction errors")
        for r in error_rows[:100]:
            business = str(r.get("business_name") or "").strip()
            city = str(r.get("city") or "").strip()
            print(f"- {business} ({city}): {r.get('pred_error')}")

    summary = {
        "rows_total": len(rows),
        "rows_with_pred_error": len(error_rows),
        "metrics": metric_reports,
        "macro_f1": round(macro_f1, 4),
        "micro_precision": round(micro_p, 4),
        "micro_recall": round(micro_r, 4),
        "micro_f1": round(micro_f1, 4),
        "form_structure_accuracy": round(form_acc, 4),
        "form_structure_support": form_support,
        "all_capture_row_accuracy": round(row_acc, 4),
        "all_capture_row_support": all_capture_support,
    }
    if args.json_out.strip():
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=True)
        print(f"\nWrote JSON summary: {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

