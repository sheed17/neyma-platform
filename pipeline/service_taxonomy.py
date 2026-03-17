"""
Subtype-aware expected service taxonomies.
"""

from __future__ import annotations

from typing import List

GENERAL_DENTISTRY_SERVICES = [
    "Implants",
    "Invisalign",
    "Orthodontics",
    "Veneers",
    "Cosmetic Dentistry",
    "All-on-4",
    "Full Mouth Reconstruction",
    "Emergency Dentistry",
    "Crowns",
    "Root Canal",
    "Pediatric Dentistry",
    "Teeth Whitening",
]

ORTHODONTIST_SERVICES = [
    "Braces",
    "Invisalign",
    "Clear Aligners",
    "Retainers",
    "Early Orthodontic Treatment",
    "Surgical Orthodontics",
]

ORAL_SURGEON_SERVICES = [
    "Dental Implants",
    "Wisdom Teeth Removal",
    "Bone Grafting",
    "Full Arch Implants",
    "Sedation Dentistry",
]

PEDIATRIC_SERVICES = [
    "Children Dentistry",
    "Fluoride Treatment",
    "Sealants",
    "Space Maintainers",
    "Pediatric Cleanings",
]

PERIODONTIST_SERVICES = [
    "Gum Disease Treatment",
    "Scaling and Root Planing",
    "Laser Gum Therapy",
    "Dental Implants",
]


def get_expected_services(practice_type: str) -> List[str]:
    practice_type = str(practice_type or "").strip().lower()
    if practice_type == "orthodontist":
        return list(ORTHODONTIST_SERVICES)
    if practice_type == "oral_surgeon":
        return list(ORAL_SURGEON_SERVICES)
    if practice_type == "pediatric_dentist":
        return list(PEDIATRIC_SERVICES)
    if practice_type == "periodontist":
        return list(PERIODONTIST_SERVICES)
    return list(GENERAL_DENTISTRY_SERVICES)
