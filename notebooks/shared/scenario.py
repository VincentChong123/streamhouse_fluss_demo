"""Canonical Northstar scenario metadata for the Marimo playbooks.

Matches ``scenarios/01-northstar-suspicious.json`` and ``TableSchemas.java``.
Per CONTRIBUTING.md Ground Rule 4, the interactive UI showcases only this
canonical scenario; negative cases belong in ``scenarios/`` fixtures and tests.
"""

SUPPLIER_ID = "SUP-NORTHSTAR"
PAYMENT_ID = "PAY-NS-0001"

SCENARIO_TITLE = "The $75,000 Payment That Looked Legitimate"
SCENARIO_SUMMARY = (
    "ABC Company has paid Northstar Logistics for three years: routine USD 75,000 "
    "payments to beneficiary account ending 1842. The account changes to ending 9077, "
    "identity activity links that exact change to an unseen device, and a USD 75,000 "
    "payment request follows. The signals indicate potential fraud, not confirmed fraud; "
    "nothing is ever paid, authorized or released."
)

RULE_SUMMARY = (
    "recent_change (0 <= payment - bank_change <= 10 min) "
    "AND unfamiliar_device (matched change/actor/session, device_is_known == false) "
    "AND large_payment (USD, amount_minor > 5_000_000) -> MANUAL_REVIEW"
)

FLUSS_TABLES = {
    "supplier_risk": {
        "primary_key": ("run_id", "supplier_id"),
        "description": "Latest correlated supplier context, signal_band, version, and source evidence.",
    },
    "payment_reviews": {
        "primary_key": ("run_id", "payment_id"),
        "description": "Deterministic payment review outcome, condition checks, and evidence references.",
    },
    "event_receipts": {
        "primary_key": ("run_id", "event_id"),
        "description": "Per-event processing disposition and provenance.",
    },
}

# Reference overview of the 4 canonical events emitted by demo-app in Stage 1.
# Never returned as fake acknowledgments: real event IDs, partitions, and offsets
# always come from Kafka via demo-app.
NORTHSTAR_EVENTS = [
    {
        "step": "baseline",
        "business_time": "10:00:00Z",
        "event_type": "SUPPLIER_REGISTERED",
        "supplier_id": SUPPLIER_ID,
        "expected_context": "v1 · NORMAL",
        "caption": "Northstar Logistics: three-year relationship, stable beneficiary account ending 1842, routine USD 75,000 payments.",
    },
    {
        "step": "bank-change",
        "business_time": "10:01:00Z",
        "event_type": "BENEFICIARY_CHANGED",
        "supplier_id": SUPPLIER_ID,
        "expected_context": "v2 · ELEVATED",
        "caption": "Beneficiary account changes from ending 1842 to ending 9077 (CHG-1001, actor usr-ap-clerk-17, session sess-4f2a).",
    },
    {
        "step": "identity",
        "business_time": "10:02:00Z",
        "event_type": "IDENTITY_ACTIVITY",
        "supplier_id": SUPPLIER_ID,
        "expected_context": "v3 · HIGH",
        "caption": "Identity activity links change CHG-1001 to previously unseen device dev-7c1e-unseen.",
    },
    {
        "step": "payment",
        "business_time": "10:03:00Z",
        "event_type": "PAYMENT_REQUESTED",
        "supplier_id": SUPPLIER_ID,
        "expected_context": "MANUAL_REVIEW (v3)",
        "caption": "USD 75,000 payment requested (PAY-NS-0001) to account ending 9077.",
    },
]
