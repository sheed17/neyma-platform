#!/usr/bin/env python3
"""
Test website signal extraction with known sites.

AGENCY-SAFE Tri-State Signal Semantics:
- true  = Confidently observed (human can see it)
- null  = Unknown / cannot be determined (DEFAULT)
- false = Explicit absence ONLY (very rare)

JSON Output: Python None → JSON null

Usage:
    python test_website_signals.py
"""

import json
from pipeline.signals import analyze_website, CONTACT_FORM_TEXT_PATTERNS
import re


# Test cases: sites where we expect specific signals
# HVAC Signal Model:
#   - Phone = primary booking mechanism (not tested here - comes from Place Details)
#   - Contact form = inbound readiness (AGENCY-SAFE: false is rare)
#   - Email = inbound reachability (NEVER false)
#   - Automated scheduling = ops maturity (false is OK here)
#   - Trust badges = established business (BBB, HomeAdvisor, etc.)
#
# AGENCY-SAFE Tri-State Semantics:
#   true  = Confidently observed (human can see it)
#   null  = Unknown (DEFAULT for uncertainty)
#   false = Explicit absence ONLY (rare, must be defensible)
TEST_SITES = [
    {
        "url": "https://californiairheatingandac.com",
        "expected": {
            "website_accessible": True,       # Confidently accessible
            "has_ssl": True,                  # HTTPS works
            "mobile_friendly": True,          # Viewport tag present
            "has_contact_form": True,         # "Request a Call Back", "Free Quote"
            "has_automated_scheduling": False,  # Analyzed, none found = opportunity
            "has_trust_badges": True,         # BBB, HomeAdvisor, Thumbtack, Yelp
            # has_email could be True or None depending on if email visible
        },
        "description": "HVAC site - Active, Inbound-ready, Manual ops (confirmed), High opportunity"
    },
    {
        "url": "https://google.com",
        "expected": {
            "website_accessible": True,
            "has_ssl": True,
            "mobile_friendly": True,
        },
        "description": "Google - should always work"
    },
]


def run_site_check(site_config: dict) -> dict:
    """Test a single site and compare to expected values."""
    url = site_config["url"]
    expected = site_config["expected"]
    description = site_config.get("description", url)
    
    print(f"\n{'=' * 60}")
    print(f"Testing: {description}")
    print(f"URL: {url}")
    print('=' * 60)
    
    # Analyze the website
    signals = analyze_website(url)
    
    # Compare results
    results = {
        "url": url,
        "signals": signals,
        "passed": [],
        "failed": [],
    }
    
    for key, expected_value in expected.items():
        actual_value = signals.get(key)
        if actual_value == expected_value:
            results["passed"].append(key)
            print(f"  ✓ {key}: {json.dumps(actual_value)}")  # JSON format
        else:
            results["failed"].append({
                "signal": key,
                "expected": expected_value,
                "actual": actual_value
            })
            print(f"  ✗ {key}: {json.dumps(actual_value)} (expected: {json.dumps(expected_value)})")
    
    # Show all signals in JSON format (null not None)
    print(f"\nAll signals (JSON format):")
    for key, value in signals.items():
        print(f"  {key}: {json.dumps(value)}")
    
    return results


def main():
    """Run all website signal tests."""
    print("Website Signal Extraction Tests")
    print("Testing false negative fixes...")
    
    all_results = []
    total_passed = 0
    total_failed = 0
    
    for site in TEST_SITES:
        result = run_site_check(site)
        all_results.append(result)
        total_passed += len(result["passed"])
        total_failed += len(result["failed"])
    
    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print('=' * 60)
    print(f"Sites tested: {len(TEST_SITES)}")
    print(f"Checks passed: {total_passed}")
    print(f"Checks failed: {total_failed}")
    
    if total_failed == 0:
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed:")
        for result in all_results:
            if result["failed"]:
                print(f"\n  {result['url']}:")
                for failure in result["failed"]:
                    print(f"    - {failure['signal']}: got {failure['actual']}, expected {failure['expected']}")


if __name__ == "__main__":
    main()
