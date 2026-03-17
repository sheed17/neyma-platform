"""Shared config for the unified Ask adaptive pipeline."""

ADAPTIVE_LIMITS_DEFAULTS = {
    "max_iterations": 3,
    "max_minutes": 5.0,
    "radius_start": 2.0,
    "radius_step": 1.0,
    "max_radius": 6.0,
    "cap_start": 150,
    "cap_step": 100,
    "max_cap": 500,
    "deep_top_k": 12,
    "shortlist_n": 50,
}
