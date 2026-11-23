"""Scenario definitions and event payloads for Streamhouse testing."""

SUPPLIER_ID = "SUP-NORTHSTAR"
PAYMENT_ID = "PAY-NS-0001"

NORTHSTAR_EVENTS = [
    {
        "event_id": "evt-0e0a0da6-0001-baseline",
        "event_type": "SUPPLIER_REGISTERED",
        "supplier_id": SUPPLIER_ID,
        "business_time": "10:00:00Z",
        "caption": "Northstar Logistics: three-year relationship, stable beneficiary account ending 1842, routine USD 75,000 payments.",
        "payload": {
            "source_system": "simulated-vendor-master",
            "supplier_name": "Northstar Logistics",
            "relationship_years": 3,
            "beneficiary_masked": "•••• 1842",
            "recurring_amount_minor": 7500000,
            "currency": "USD",
        },
    },
    {
        "event_id": "evt-0e0a0da6-0002-change",
        "event_type": "BENEFICIARY_CHANGED",
        "supplier_id": SUPPLIER_ID,
        "business_time": "10:01:00Z",
        "caption": "Beneficiary account changes to ending 9077 (CHG-1001, actor usr-ap-clerk-17).",
        "payload": {
            "source_system": "simulated-vendor-master-cdc",
            "change_id": "CHG-1001",
            "actor_id": "usr-ap-clerk-17",
            "session_id": "sess-4f2a",
            "before": {"beneficiary_masked": "•••• 1842"},
            "after": {"beneficiary_masked": "•••• 9077"},
        },
    },
    {
        "event_id": "evt-0e0a0da6-0003-identity",
        "event_type": "IDENTITY_ACTIVITY",
        "supplier_id": SUPPLIER_ID,
        "business_time": "10:02:00Z",
        "caption": "Identity activity links change CHG-1001 to a previously unseen device.",
        "payload": {
            "source_system": "simulated-identity-provider",
            "change_id": "CHG-1001",
            "actor_id": "usr-ap-clerk-17",
            "session_id": "sess-4f2a",
            "device_id": "dev-7c1e-unseen",
            "device_is_known": False,
            "activity": "BANK_DETAILS_MODIFIED",
        },
    },
    {
        "event_id": "evt-0e0a0da6-0004-payment",
        "event_type": "PAYMENT_REQUESTED",
        "supplier_id": SUPPLIER_ID,
        "business_time": "10:03:00Z",
        "caption": "USD 75,000 payment requested to account ending 9077.",
        "payload": {
            "source_system": "simulated-payments-hub",
            "payment_id": PAYMENT_ID,
            "amount_minor": 7500000,
            "currency": "USD",
            "beneficiary_masked": "•••• 9077",
        },
    },
]
