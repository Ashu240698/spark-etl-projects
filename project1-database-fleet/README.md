# Database Fleet Intelligence Pipeline

A PySpark ETL pipeline that ingests raw database monitoring metrics, cleans and normalises the data, scores each database instance by operational risk, classifies incidents, and writes partitioned analytical outputs.

Built as Project 1 of a structured PySpark learning curriculum focused on production-grade pipeline patterns.

---

## Problem Statement

Infrastructure monitoring systems export raw fleet metrics as CSV files — but the data arrives inconsistent. Regions use different naming conventions, timestamps come in multiple formats, status fields have mixed casing, and some records are duplicated due to retry mechanisms. This pipeline takes that raw data and produces clean, risk-scored, incident-classified outputs ready for ops team consumption.

---

## Dataset

- **Source:** Simulated fleet metrics CSV — 529 rows (including 25 duplicates)
- **Columns:** db_id, db_name, region, db_type, environment, cpu_utilization, memory_utilization, disk_utilization, active_sessions, status, sla_target, recorded_at
- **Lookup:** Region configuration metadata (6 regions with team names, timezones, alert emails)

**Data quality issues introduced:**
| Issue | Details |
|-------|---------|
| Duplicate rows | 25 network retry duplicates |
| Negative active_sessions | Corrupt sensor readings |
| Mixed status values | HEALTHY/OK/healthy, WARNING/WARN, CRITICAL/critical |
| Region naming inconsistency | us_east_1 vs us-east-1 (underscore vs hyphen) |
| Mixed timestamp formats | 6 different formats across data sources |
| Missing db_name | Some database names absent from source |

---

## Pipeline Architecture

```
Raw CSV (529 rows)
      │
      ▼
┌─────────────────────┐
│   Layer 1: Ingest   │  Explicit schema — all StringType
│                     │  No inferSchema — audit first
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   Layer 2: Clean    │  Deduplication, normalisation,
│                     │  timestamp parsing, type casting,
│                     │  missing value handling
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Layer 3: Enrich    │  Broadcast join with region config
│  + Risk Scoring     │  Weighted risk score calculation
│                     │  SLA multiplier applied
│                     │  Incident classification
│                     │  Regional benchmarking
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   Layer 4: Output   │  Partitioned Parquet (warehouse)
│                     │  Incident CSV (ops team)
│                     │  Regional summary Parquet
└─────────────────────┘
```

---

## Data Quality — Cleaning Rules

| Rule | Issue | Strategy |
|------|-------|----------|
| 1 | Duplicate rows | `drop_duplicates()` — exact match removal |
| 2 | Region naming | `lower()` + `regexp_replace` underscore → hyphen |
| 3 | Mixed status casing | `upper()` then map to 4 standard values |
| 4 | Negative active_sessions | Replace with null — physically impossible |
| 5 | Missing db_name | Replace with `UNKNOWN_DB_<db_id>` |
| 6 | 6 timestamp formats | `coalesce` + `try_to_timestamp` per format |
| 7 | Timestamp flag | PARSE_FAILED / AMBIGUOUS_FORMAT / OK |
| 8 | Schema enforcement | Cast all columns to correct types |

---

## Risk Scoring Logic

Each database instance receives a weighted risk score based on four resource metrics:

```
cpu_weight     = cpu_utilization    × 0.4   (40%)
memory_weight  = memory_utilization × 0.3   (30%)
disk_weight    = disk_utilization   × 0.2   (20%)
session_weight = (active_sessions / 500) × 100 × 0.1  (10%)

raw_score = cpu_weight + memory_weight + disk_weight + session_weight
```

SLA multiplier escalates risk for high-availability databases:

```
sla_target >= 99.9  →  × 1.5  (highest priority)
sla_target >= 99.5  →  × 1.2
sla_target >= 99.0  →  × 1.0
otherwise           →  × 1.0

final_risk_score = raw_score × sla_multiplier
```

Risk categories:

```
final_risk_score >= 80  →  CRITICAL_RISK
final_risk_score >= 60  →  HIGH_RISK
final_risk_score >= 40  →  MEDIUM_RISK
otherwise               →  LOW_RISK
```

---

## Incident Classification

Applied on top of risk scoring for ops team alerting:

| Classification | Condition |
|----------------|-----------|
| `IMMEDIATE_PAGE` | cpu > 90% AND active_sessions > 300 |
| `URGENT_TICKET` | cpu > 80% AND active_sessions > 200 |
| `MEMORY_ALERT` | memory_utilization > 85% |
| `DISK_ALERT` | disk_utilization > 80% |
| `MONITOR_CLOSELY` | None of the above |

---

## Outputs

| Output | Format | Location | Notes |
|--------|--------|----------|-------|
| Clean metrics | Parquet | `output_data/clean_metrics/` | Partitioned by `region` + `environment` |
| Incident report | CSV | `incidents/incident_data_<date>.csv` | CRITICAL and HIGH risk only, coalesce(1) |
| Regional summary | Parquet | `output_data/regional_summary/` | Aggregated benchmarks per region |

**Partition pruning verified** — reading `region=us-east-1` reads only the matching folder. Physical plan confirms `PartitionFilters` applied at scan level.

---

## Tech Stack

- **PySpark** 4.1.1
- **Python** 3.12
- **Ubuntu** 24.04 (WSL2)
- **Output formats:** Parquet (partitioned), CSV
- **Join strategy:** Broadcast join for region config (6 rows)

---

## Project Structure

```
project1-database-fleet/
├── src/
│   └── generate_data.py        # Data generator with quality issues
├── notebooks/
│   └── data_cleaning.ipynb     # Full pipeline notebook
├── data/                       # Raw data (gitignored)
└── output/                     # Pipeline outputs (gitignored)
```
