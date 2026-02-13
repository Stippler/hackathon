from typing import Any, Dict


FRAUNHOFER_LSCM_PROFILE = {
    "focus_areas": [
        {
            "name": "Strategische Supply-Chain- und Netzwerkplanung",
            "keywords": ["netzwerkplanung", "standort", "reshoring", "nearshoring", "milp", "resilienz"],
        },
        {
            "name": "Datengetriebene Planung und Bestandsoptimierung",
            "keywords": ["bestandsoptimierung", "bedarfsprognose", "disposition", "simulation", "forecasting"],
        },
        {
            "name": "Lagerplanung, Automatisierung und Intralogistik",
            "keywords": ["lagerplanung", "intralogistik", "materialfluss", "layout", "automatisierung"],
        },
        {
            "name": "Mobile Robotik und FTS",
            "keywords": ["fts", "agv", "mobile robotik", "cobot", "automatisierte verladung"],
        },
        {
            "name": "Ersatzteil- und Instandhaltungsmanagement",
            "keywords": ["ersatzteil", "predictive maintenance", "ausfallprognose", "instandhaltung"],
        },
        {
            "name": "Stammdaten-Optimierung und KI-Enablement",
            "keywords": ["stammdaten", "datenqualität", "ki-agenten", "web scraping", "kpi"],
        },
    ],
    "target_industries": [
        "Industrie und Maschinenbau",
        "Handel und Großhandel",
        "Logistikdienstleister",
        "Energie und Infrastruktur",
        "Baustoffindustrie",
        "Produktion",
    ],
    "acquisition_intent": (
        "Suche nach Unternehmen, die Fraunhofer-LSCM-nahe Kompetenzen ergänzen: "
        "SCM-Analytics, Optimierung, Intralogistik, Automatisierung, Predictive Maintenance, "
        "Daten-/KI-Enablement."
    ),
}


def fraunhofer_lscm_focus() -> Dict[str, Any]:
    return FRAUNHOFER_LSCM_PROFILE
