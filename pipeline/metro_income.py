"""
High-income metro area detection.

Uses a curated lookup of metros/cities where dental practice revenue
bands tend to be 10-15% higher due to higher household income,
willingness-to-pay for elective procedures, and higher case values.

Source: US Census Bureau ACS median household income data,
filtered to metros where median HHI exceeds $90k.
"""

from typing import Optional

# Cities/metros where median household income > $90k and dental
# case values tend to be materially higher than national averages.
# Format: lowercase city name → True
HIGH_INCOME_CITIES = {
    # Northeast
    "new york", "manhattan", "brooklyn", "queens", "bronx",
    "stamford", "greenwich", "westport", "darien",
    "hoboken", "jersey city", "princeton", "morristown",
    "boston", "cambridge", "brookline", "newton", "wellesley",
    "washington", "bethesda", "arlington", "mclean", "alexandria",
    "chevy chase", "potomac", "great falls",
    "philadelphia", "wayne", "bryn mawr", "radnor",
    "hartford", "west hartford", "glastonbury",
    # Southeast
    "miami", "miami beach", "coral gables", "boca raton",
    "palm beach", "west palm beach", "naples", "sarasota",
    "charlotte", "raleigh", "chapel hill", "durham",
    "atlanta", "buckhead", "alpharetta", "roswell",
    "nashville", "franklin",
    # Midwest
    "chicago", "evanston", "naperville", "lake forest", "winnetka",
    "hinsdale", "highland park",
    "minneapolis", "edina", "wayzata", "plymouth",
    "detroit", "birmingham", "bloomfield hills", "ann arbor",
    "columbus", "dublin", "upper arlington",
    # Southwest / Mountain
    "dallas", "highland park", "university park", "plano", "frisco",
    "houston", "the woodlands", "sugar land", "river oaks",
    "austin", "westlake", "lakeway",
    "scottsdale", "paradise valley", "gilbert", "chandler",
    "denver", "cherry hills village", "greenwood village", "boulder",
    "salt lake city", "park city",
    "las vegas", "henderson", "summerlin",
    # West Coast
    "san francisco", "palo alto", "menlo park", "atherton",
    "mountain view", "sunnyvale", "cupertino", "saratoga",
    "san jose", "los gatos", "campbell",
    "los angeles", "beverly hills", "santa monica", "brentwood",
    "manhattan beach", "hermosa beach", "redondo beach",
    "pasadena", "la canada flintridge", "san marino",
    "irvine", "newport beach", "laguna beach", "dana point",
    "huntington beach", "carlsbad", "la jolla", "del mar",
    "san diego", "coronado", "encinitas",
    "seattle", "bellevue", "mercer island", "kirkland",
    "redmond", "sammamish", "medina",
    "portland", "lake oswego", "west linn",
    # Hawaii
    "honolulu",
}

# State-level high-income indicators (states where the majority
# of metro areas have elevated HHI)
HIGH_INCOME_STATES = {
    "connecticut", "ct",
    "massachusetts", "ma",
    "new jersey", "nj",
    "maryland", "md",
    "hawaii", "hi",
}


def is_high_income_metro(
    city: Optional[str] = None,
    state: Optional[str] = None,
) -> bool:
    """
    Determine whether a city/state combination is in a high-income metro area.

    Uses a curated lookup of cities and states where dental practice
    revenue tends to be materially higher than national averages.
    """
    city_lower = (city or "").strip().lower()
    state_lower = (state or "").strip().lower()

    if city_lower in HIGH_INCOME_CITIES:
        return True

    if state_lower in HIGH_INCOME_STATES:
        return True

    return False
