import csv
import random
from datetime import datetime, timedelta

random.seed(42)

# --- Master data ---
regions = {
    "us-east-1":      {"team": "Team_Americas", "tz": "UTC-5",  "email": "americas-oncall@company.com"},
    "us-west-2":      {"team": "Team_Americas", "tz": "UTC-8",  "email": "americas-oncall@company.com"},
    "eu-west-1":      {"team": "Team_Europe",   "tz": "UTC+1",  "email": "europe-oncall@company.com"},
    "eu-central-1":   {"team": "Team_Europe",   "tz": "UTC+2",  "email": "europe-oncall@company.com"},
    "ap-south-1":     {"team": "Team_APAC",     "tz": "UTC+5",  "email": "apac-oncall@company.com"},
    "ap-southeast-1": {"team": "Team_APAC",     "tz": "UTC+8",  "email": "apac-oncall@company.com"},
}

db_types = ["mysql", "postgres", "oracle", "mongodb", "redis",
            "cassandra", "elasticsearch", "dynamodb"]

environments = ["prod", "staging", "dev", "dr"]

def random_timestamp():
    base = datetime(2026, 2, 1)
    delta = timedelta(
        days=random.randint(0, 24),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    # Introduce format inconsistency — real world problem
    fmt = random.choice([
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%m-%d-%Y %H:%M:%S"
    ])
    return (base + delta).strftime(fmt)

def generate_status(cpu):
    if cpu >= 90:   return random.choice(["CRITICAL", "critical", "Critical"])
    elif cpu >= 70: return random.choice(["WARNING",  "warning",  "Warning", "WARN"])
    else:           return random.choice(["HEALTHY",  "healthy",  "Healthy", "OK"])

rows = []
db_id = 1

for region in regions:
    num_dbs = random.randint(70, 100)
    for i in range(num_dbs):
        db_type = random.choice(db_types)
        env     = random.choice(environments)
        db_name = f"{db_type}_{env}_{str(i+1).zfill(3)}"
        cpu     = round(random.uniform(5.0, 99.0), 2)
        memory  = round(random.uniform(10.0, 98.0), 2)
        disk    = round(random.uniform(5.0, 95.0), 2)
        sessions = random.randint(0, 500)
        sla_target = random.choice([99.9, 99.5, 99.0])

        # Introduce data quality issues deliberately
        # 1. Missing cpu values
        if random.random() < 0.04:
            cpu = ""
        # 2. Missing db_name
        if random.random() < 0.02:
            db_name = ""
        # 3. Negative sessions (corrupt data)
        if random.random() < 0.03:
            sessions = random.randint(-50, -1)
        # 4. Region casing inconsistency
        region_val = random.choice([
            region, region.upper(), region.replace("-", "_")
        ])

        rows.append([
            db_id, db_name, region_val, db_type, env,
            cpu, memory, disk, sessions,
            generate_status(cpu) if cpu != "" else "UNKNOWN",
            sla_target,
            random_timestamp()
        ])
        db_id += 1

# Add deliberate duplicates — 5% of rows
duplicates = random.sample(rows, int(len(rows) * 0.05))
rows.extend(duplicates)
random.shuffle(rows)

# Write to CSV
headers = [
    "db_id", "db_name", "region", "db_type", "environment",
    "cpu_utilization", "memory_utilization", "disk_utilization",
    "active_sessions", "status", "sla_target", "recorded_at"
]

with open("/home/ashu/pyspark_learning/project1/data/fleet_metrics_raw.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

print(f"Generated {len(rows)} rows")
print("Saved to fleet_metrics_raw.csv")

# Quick preview of the mess
print("\nSample of data quality issues:")
issues = [r for r in rows if r[1] == "" or r[5] == "" or r[8] < 0]
for r in issues[:5]:
    print(r)