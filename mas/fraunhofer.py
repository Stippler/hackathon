from typing import Any, Dict, List


_FRAUNHOFER_OVERVIEW: Dict[str, Any] = {
    "unit": "Logistik und Supply Chain Management - Fraunhofer Austria",
    "positioning": [
        "Support across the full value chain from strategy to implementation.",
        "Application-oriented research with measurable business impact.",
        "Neutral technology and vendor evaluation.",
        "Quantitative decision support for investments and transformations.",
    ],
    "core_methods": [
        "Data science",
        "Mathematical optimization (including MILP)",
        "Simulation",
        "AI and forecasting methods",
        "Domain-specific logistics expertise",
    ],
}


_SERVICE_AREAS: Dict[str, Dict[str, Any]] = {
    "network_planning": {
        "title": "Strategic supply chain and network planning",
        "project_types": [
            "European and global network planning",
            "Site evaluation and allocation (including center-of-gravity analyses)",
            "Reshoring and nearshoring analyses",
            "Make-or-buy decisions",
            "Sustainability and resilience assessments",
        ],
        "methods": [
            "Big-data analysis of volume, cost, and transport data",
            "Scenario and sensitivity analysis",
            "Mathematical optimization models",
            "Interactive dashboards and scalable data models",
        ],
        "value": [
            "Reduced logistics costs (up to about 15 percent per year in selected cases)",
            "Transparent basis for long-term network decisions",
            "Integrated trade-off of cost, service level, and delivery reliability",
        ],
        "industries": [
            "Consumer goods",
            "Industrial and mechanical engineering",
            "Wholesale",
            "Construction materials",
            "Energy supply",
        ],
    },
    "planning_inventory": {
        "title": "Data-driven supply chain planning and inventory optimization",
        "project_types": [
            "Inventory planning and optimization",
            "AI-supported demand forecasting",
            "Replenishment parameter optimization",
            "Scenario-based supply strategies",
        ],
        "methods": [
            "Big-data analysis of ordering, inventory, and demand data",
            "Time-series and AI forecasting models",
            "Simulation-based optimization",
            "Company-specific AI algorithm development",
        ],
        "value": [
            "Reduced working capital tie-up (roughly 10 to 30 percent)",
            "Lower logistics costs (about 6 to 7 percent)",
            "Faster planning processes (roughly 25 to 30 percent time savings)",
            "Improved service levels and product availability",
        ],
        "industries": [
            "Wholesale",
            "B2B and B2C trade",
            "Industry",
            "Spare parts and after-sales logistics",
        ],
    },
    "warehouse_automation": {
        "title": "Warehouse planning, automation, and intralogistics",
        "project_types": [
            "High-level and detailed warehouse/logistics center design",
            "Automation technology selection and evaluation",
            "Tendering support and vendor evaluation",
            "Implementation and go-live support",
        ],
        "methods": [
            "Process, material-flow, and data analysis",
            "Scenario and business-case calculations",
            "Layout and functional area planning (including CAD)",
            "Vendor-neutral technology selection",
        ],
        "value": [
            "De-risked investment decisions",
            "Optimized throughput, capacity, and automation level",
            "Reduced ongoing logistics costs",
            "Future-proof site and layout concepts",
        ],
        "industries": [
            "Trade",
            "Industry",
            "Logistics service providers",
            "Manufacturing",
        ],
    },
    "warehouse_efficiency": {
        "title": "Warehouse efficiency and operational optimization",
        "project_types": [
            "Logistics quick checks",
            "Material-flow and layout optimization",
            "Slotting and article placement optimization",
            "Picking process optimization",
        ],
        "methods": [
            "Location-based material-flow analysis",
            "Real-time transport data analysis",
            "Item correlation analyses",
            "Fraunhofer in-house tools (for example Warehouse Squirrel)",
        ],
        "value": [
            "Transparency on distances, empty trips, and bottlenecks",
            "Reduced picking travel and cycle times",
            "More efficient use of labor and infrastructure",
            "Better decision basis for automation initiatives",
        ],
        "industries": [
            "FMCG",
            "Industry",
            "Wholesale",
            "Logistics service providers",
        ],
    },
    "mobile_robotics": {
        "title": "Automation with mobile robotics",
        "project_types": [
            "Automated guided vehicle (AGV/FTS) introductions",
            "Automated truck loading",
            "Cobot integration",
        ],
        "methods": [
            "Structured phase models from concept to implementation",
            "MTM analyses",
            "Technology comparison and ROI assessment",
            "Vendor-neutral tender documentation",
        ],
        "value": [
            "Reduced manual operations",
            "Objective technology selection",
            "Structured implementation up to SOP/go-live",
        ],
        "industries": [
            "Industry",
            "Logistics",
            "Manufacturing",
        ],
    },
    "spare_parts_maintenance": {
        "title": "Spare parts and maintenance management",
        "project_types": [
            "Predictive spare parts management",
            "Predictive maintenance",
            "Optimized stocking strategies",
        ],
        "methods": [
            "Combination of sensor data and historical demand",
            "AI-supported failure prediction",
            "Interactive analytics tools",
        ],
        "value": [
            "Reduced inventory and procurement costs",
            "Higher asset availability",
            "Improved maintenance planning",
        ],
        "industries": [
            "Industry",
            "Plant engineering",
            "Energy",
            "Infrastructure operators",
        ],
    },
    "master_data_ai": {
        "title": "Master data optimization for AI use cases",
        "project_types": [
            "Data maturity assessments",
            "Master data cleansing and enrichment",
            "AI agents and web scraping support",
            "Holistic data strategy design",
        ],
        "methods": [
            "Automated extraction from unstructured sources",
            "Dashboarding and KPI systems",
            "Scalable data model design",
        ],
        "value": [
            "Reliable baseline for AI and automation programs",
            "Scalable data quality along the value chain",
            "Improved investment and planning decisions",
        ],
        "industries": [
            "Energy",
            "Industry",
            "Trade",
            "Logistics",
        ],
    },
}


_CROSS_INDUSTRY_COLLABORATION: List[str] = [
    "Trade and wholesale",
    "Industry and mechanical engineering",
    "Construction materials",
    "Energy and infrastructure",
    "Logistics services",
    "Public sector (for example crisis and supply analyses)",
]


_KEY_CUSTOMER_VALUE: List[str] = [
    "Data-based and objective decision-making",
    "Technology and vendor neutrality",
    "Quantifiable economic impact",
    "End-to-end process/data/organization perspective",
    "Support from analysis through implementation",
]


def _normalize_key(value: str) -> str:
    return (value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _safe_area_lookup(area: str) -> Dict[str, Any]:
    key = _normalize_key(area)
    if not key:
        raise ValueError("Missing area. Use one of: " + ", ".join(sorted(_SERVICE_AREAS.keys())))
    if key not in _SERVICE_AREAS:
        raise ValueError(f"Unknown area '{area}'. Use one of: " + ", ".join(sorted(_SERVICE_AREAS.keys())))
    return _SERVICE_AREAS[key]


def fraunhofer_service_areas() -> Dict[str, Any]:
    """List available Fraunhofer service areas with their human-readable titles."""
    areas = [{"key": key, "title": value["title"]} for key, value in sorted(_SERVICE_AREAS.items())]
    return {"ok": True, "count": len(areas), "areas": areas}


def fraunhofer_overview() -> Dict[str, Any]:
    """Return high-level Fraunhofer positioning plus core methods and consulting profile."""
    return {"ok": True, "overview": _FRAUNHOFER_OVERVIEW}


def fraunhofer_area_details(area: str) -> Dict[str, Any]:
    """Return detailed content for one service area, including methods, project types, value, and industries."""
    try:
        details = _safe_area_lookup(area)
        return {"ok": True, "area": _normalize_key(area), "details": details}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fraunhofer_value_drivers(area: str = "") -> Dict[str, Any]:
    """Return key value drivers (quantified and qualitative), either overall or scoped to one area."""
    try:
        if area.strip():
            details = _safe_area_lookup(area)
            return {"ok": True, "scope": _normalize_key(area), "value_drivers": details.get("value", [])}
        return {"ok": True, "scope": "all", "value_drivers": _KEY_CUSTOMER_VALUE}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fraunhofer_methods(area: str = "") -> Dict[str, Any]:
    """Return methods used across the portfolio or within a selected service area."""
    try:
        if area.strip():
            details = _safe_area_lookup(area)
            return {"ok": True, "scope": _normalize_key(area), "methods": details.get("methods", [])}
        return {"ok": True, "scope": "all", "methods": _FRAUNHOFER_OVERVIEW.get("core_methods", [])}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fraunhofer_industries(area: str = "") -> Dict[str, Any]:
    """Return industries served overall or the industries addressed by a specific area."""
    try:
        if area.strip():
            details = _safe_area_lookup(area)
            return {"ok": True, "scope": _normalize_key(area), "industries": details.get("industries", [])}
        return {"ok": True, "scope": "cross_industry", "industries": _CROSS_INDUSTRY_COLLABORATION}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fraunhofer_project_types(area: str) -> Dict[str, Any]:
    """Return representative project types typically delivered in one service area."""
    try:
        details = _safe_area_lookup(area)
        return {"ok": True, "area": _normalize_key(area), "project_types": details.get("project_types", [])}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
