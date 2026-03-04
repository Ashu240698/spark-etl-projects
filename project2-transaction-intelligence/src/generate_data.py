import csv
import random
import string
from datetime import datetime, timedelta

random.seed(42)

# ── MASTER DATA ────────────────────────────────────────────────────
payment_channels = ["UPI", "NEFT", "RTGS", "IMPS", "CARD"]

merchant_categories = [
    "GROCERY", "FUEL", "ELECTRONICS", "RESTAURANT",
    "TRAVEL", "HEALTHCARE", "EDUCATION", "UTILITIES",
    "ENTERTAINMENT", "CLOTHING"
]

cities = [
    "Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad",
    "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat"
]

intl_countries = ["USA", "UK", "UAE", "Singapore", "Germany"]
intl_cities    = ["New York", "London", "Dubai", "Singapore", "Berlin"]

statuses = {
    "SUCCESS" : ["SUCCESS", "Success", "success"],
    "FAILED"  : ["FAILED",  "Failed",  "failed"],
    "PENDING" : ["PENDING", "Pending", "pending"],
    "REVERSED": ["REVERSED","Reversed","reversed"]
}

failure_reasons = [
    "INSUFFICIENT_FUNDS",
    "INVALID_ACCOUNT",
    "DAILY_LIMIT_EXCEEDED",
    "NETWORK_TIMEOUT",
    "BLOCKED_MERCHANT",
    None
]

device_types   = ["MOBILE", "WEB", "ATM", "POS"]
branch_codes   = [f"BR{str(i).zfill(4)}" for i in range(1, 51)]
rm_ids         = [f"RM{str(i).zfill(3)}" for i in range(1, 21)]
currencies_intl= ["USD", "GBP", "AED", "SGD", "EUR"]

# ── HELPERS ────────────────────────────────────────────────────────
def random_id(prefix, length=8):
    return prefix + ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_account():
    return ''.join(random.choices(string.digits, k=12))

def format_date(dt, channel):
    """Each channel sends dates in a different format — real world problem"""
    formats = {
        "UPI" : "%Y-%m-%d %H:%M:%S",
        "NEFT": "%d-%m-%Y %H:%M",
        "RTGS": "%m/%d/%Y %H:%M:%S",
        "IMPS": "%Y/%m/%d %H:%M",
        "CARD": "%d/%m/%Y %H:%M:%S",
    }
    return dt.strftime(formats[channel])

def random_amount():
    """Realistic transaction amounts by tier"""
    tier = random.random()
    if tier < 0.5:   return round(random.uniform(100, 5000), 2)
    elif tier < 0.8: return round(random.uniform(5000, 50000), 2)
    elif tier < 0.95:return round(random.uniform(50000, 200000), 2)
    else:            return round(random.uniform(200000, 500000), 2)

def format_amount(amount, channel):
    """CARD channel uses European format with comma decimal"""
    if channel == "CARD" and random.random() < 0.15:
        return str(amount).replace(".", ",")
    return str(amount)

# ── GENERATE ROWS ──────────────────────────────────────────────────
rows = []
base_date = datetime(2026, 1, 1)

for i in range(1200):
    channel     = random.choice(payment_channels)
    is_intl     = random.random() < 0.15
    status_key  = random.choices(
                    ["SUCCESS", "FAILED", "PENDING", "REVERSED"],
                    weights=[70, 15, 10, 5]
                  )[0]
    status_val  = random.choice(statuses[status_key])
    amount      = random_amount()
    delta       = timedelta(
                    days=random.randint(0, 58),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                  )
    txn_dt      = base_date + delta

    # Future dated transactions — system clock error
    if random.random() < 0.02:
        txn_dt = datetime.now() + timedelta(days=random.randint(1, 30))

    proc_time   = random.randint(100, 5000)
    # Negative processing time — corrupt data
    if random.random() < 0.03:
        proc_time = random.randint(-500, -1)

    merchant_cat = random.choice(merchant_categories)
    # Invalid merchant category — free text instead of code
    if random.random() < 0.03:
        merchant_cat = random.choice([
            "general store", "PETROL BUNK", "online", "N/A"
        ])

    row = {
        "transaction_id"     : random_id("TXN"),
        "user_id"            : random_id("USR", 6),
        "account_number"     : random_account(),
        "merchant_id"        : random_id("MRC", 6),
        "merchant_category"  : merchant_cat,
        "transaction_amount" : format_amount(amount, channel),
        "currency"           : random.choice(currencies_intl) if is_intl else "INR",
        "transaction_date"   : format_date(txn_dt, channel),
        "payment_channel"    : channel,
        "transaction_status" : status_val,
        "failure_reason"     : random.choice(failure_reasons) if status_key == "FAILED" else "",
        "device_type"        : random.choice(device_types),
        "location_city"      : random.choice(intl_cities) if is_intl else random.choice(cities),
        "location_country"   : random.choice(intl_countries) if is_intl else "India",
        "is_international"   : is_intl,
        "bank_branch_code"   : random.choice(branch_codes),
        "relationship_manager": random.choice(rm_ids),
        "approval_status"    : random.choice(["APPROVED", "DECLINED", "PENDING_REVIEW"]),
        "processing_time_ms" : proc_time,
    }

    # ── INTRODUCE DATA QUALITY ISSUES ─────────────────────────────
    # 4% missing amounts
    if random.random() < 0.04:
        row["transaction_amount"] = ""

    # 2% missing merchant_id
    if random.random() < 0.02:
        row["merchant_id"] = ""

    rows.append(row)

# 5% duplicates — network retry
duplicates = random.sample(rows, int(len(rows) * 0.05))
rows.extend(duplicates)
random.shuffle(rows)

# ── WRITE CSV ──────────────────────────────────────────────────────
output_path = "/home/ashu/spark-etl-projects/project2-transaction-intelligence/data/transactions_raw.csv"

headers = list(rows[0].keys())

with open(output_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)

# ── SUMMARY ────────────────────────────────────────────────────────
total          = len(rows)
missing_amt    = sum(1 for r in rows if r["transaction_amount"] == "")
missing_merch  = sum(1 for r in rows if r["merchant_id"] == "")
neg_proc       = sum(1 for r in rows if str(r["processing_time_ms"]).startswith("-"))
intl_rows      = sum(1 for r in rows if r["is_international"] == True)
card_comma_amt = sum(1 for r in rows if "," in str(r["transaction_amount"]))
dupes          = len(duplicates)
bad_categories = sum(1 for r in rows if r["merchant_category"] in
                     ["general store", "PETROL BUNK", "online", "N/A"])

print("=" * 55)
print("TRANSACTION DATA GENERATOR — SUMMARY")
print("=" * 55)
print(f"Total rows generated       : {total}")
print(f"Duplicate rows (5%)        : {dupes}")
print(f"Missing amounts (4%)       : {missing_amt}")
print(f"Missing merchant_id (2%)   : {missing_merch}")
print(f"Negative processing time   : {neg_proc}")
print(f"International transactions  : {intl_rows}")
print(f"CARD comma-format amounts  : {card_comma_amt}")
print(f"Invalid merchant categories: {bad_categories}")
print("=" * 55)
print(f"Saved to: {output_path}")
