# Financial Transaction Intelligence Pipeline

A production-grade PySpark ETL pipeline that ingests raw payment transaction data from multiple channels, detects fraudulent patterns, profiles merchant risk, and outputs structured analytical datasets.

Built as part of a structured PySpark learning curriculum — designed to reflect real-world data engineering problems, not textbook examples.

---

## Problem Statement

Payment systems receive transaction data from multiple channels (UPI, NEFT, RTGS, IMPS, Card) — each with its own date format, amount format, and data quality quirks. Raw files arrive messy: duplicate records from network retries, missing amounts, negative processing times, inconsistent status casing, and timestamps in 6 different formats.

This pipeline takes that raw mess and turns it into clean, enriched, fraud-flagged analytical outputs ready for downstream consumption.

---

## Dataset

- **Source:** Simulated raw transaction CSV — 1,260 rows (including 5% duplicates)
- **Channels:** UPI, NEFT, RTGS, IMPS, Card
- **Lookup:** Merchant category metadata (11 categories with risk levels and transaction limits)

**Deliberate data quality issues introduced:**
| Issue | Count | Details |
|-------|-------|---------|
| Duplicate rows | 60 | Network retry duplicates |
| Missing transaction amount | 46 | 3.65% of rows |
| Missing merchant ID | 20 | 1.59% of rows |
| Negative processing time | 30 | Corrupt sensor data |
| Invalid merchant categories | 39 | Free text instead of standard codes |
| Mixed status casing | 12 variants | SUCCESS/Success/success etc. |
| Mixed timestamp formats | 6 formats | One per channel |
| Future dated transactions | 98 | System clock errors |

---

## Pipeline Architecture

```
Raw CSV (1,260 rows)
      │
      ▼
┌─────────────────────┐
│   Layer 1: Ingest   │  Read all columns as StringType
│                     │  Schema defined explicitly
│                     │  Audit performed before cleaning
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   Layer 2: Clean    │  10 cleaning rules applied
│                     │  Deduplication, normalization,
│                     │  timestamp parsing, type casting
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Layer 3: Enrich    │  Broadcast join with merchant lookup
│  + Fraud Detection  │  5 fraud flag rules
│                     │  Merchant risk profiling
│                     │  Channel performance analysis
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   Layer 4: Output   │  Partitioned Parquet (warehouse)
│                     │  Flagged transactions CSV (ops team)
│                     │  Merchant + channel summaries
└─────────────────────┘
```

---

## Data Quality — Cleaning Rules

| Rule | Issue | Strategy |
|------|-------|----------|
| 1 | Duplicate rows | `drop_duplicates()` — exact match removal |
| 2 | Mixed status casing | `upper()` — normalize to 4 standard values |
| 3 | Invalid merchant categories | Map to standard codes or UNKNOWN |
| 4 | Amount comma format (CARD channel) | `regexp_replace` comma → dot, cast to Double |
| 5 | Missing merchant ID | Replace with `UNKNOWN_MERCHANT_<transaction_id>` |
| 6 | Negative processing time | Replace with null — physically impossible values |
| 7 | 6 timestamp formats | `coalesce` + `try_to_timestamp` per channel format |
| 8 | Future dated transactions | Flag as `FUTURE_DATE` — kept for compliance audit trail |
| 9 | `is_international` as String | Cast to BooleanType |
| 10 | `account_number` stays String | Never cast identifiers to numeric — leading zeros matter |

**Key decision:** Rows with missing amounts are kept (not dropped). In a financial pipeline, a missing amount is itself an auditable event — dropping it would create gaps in the transaction trail.

---

## Fraud Detection Rules

Applied after enrichment with merchant metadata:

| Flag | Condition | Risk |
|------|-----------|------|
| `MISSING_AMOUNT` | `transaction_amount` is null | Unverifiable |
| `AMOUNT_LIMIT_EXCEEDED` | Amount exceeds merchant category limit | High |
| `HIGH_VALUE_INTERNATIONAL` | International + amount > 100,000 | High |
| `UNKNOWN_MERCHANT_RISK` | Merchant category is UNKNOWN | Medium |
| `FAILED_HIGH_VALUE` | Failed transaction + amount > 50,000 | Medium |
| `CLEAN` | None of the above | Low |

**Risk level** is then assigned by combining `fraud_flag` with `category_risk_level` from the merchant lookup:

```
fraud_flag != CLEAN  +  category_risk = HIGH    →  CRITICAL
fraud_flag != CLEAN  +  category_risk = MEDIUM  →  HIGH
fraud_flag != CLEAN  +  any other               →  MEDIUM
CLEAN                                           →  LOW
```

**Results from this dataset:**
```
CRITICAL :  26 transactions
HIGH     :  73 transactions  
MEDIUM   : 161 transactions
LOW      : 940 transactions (78% clean)
```

---

## Outputs

| Output | Format | Location | Notes |
|--------|--------|----------|-------|
| Clean transactions | Parquet | `output/parquet/clean_transactions/` | Partitioned by `payment_channel` + `transaction_status` (20 folders) |
| Flagged transactions | CSV | `output/csv/flagged_transactions/` | 260 rows — fraud_flag != CLEAN |
| Merchant risk profile | Parquet | `output/parquet/merchant_risk_profile/` | 11 rows aggregated by category |
| Channel performance | Parquet | `output/parquet/payment_channel_performance/` | 5 rows aggregated by channel |

**Partition pruning verified** — reading `payment_channel=UPI` + `transaction_status=SUCCESS` reads 1 of 20 folders. Physical plan confirms `PartitionFilters` applied at scan level.

---

## Tech Stack

- **PySpark** 4.1.1
- **Python** 3.12
- **Ubuntu** 24.04 (WSL2)
- **Output formats:** Parquet (columnar, partitioned), CSV
- **Join strategy:** Broadcast join for merchant lookup (11 rows)

---

## Project Structure

```
project2-transaction-intelligence/
├── data/
│   ├── transactions_raw.csv        # Raw input (gitignored)
│   └── merchant_lookup.csv         # Merchant metadata lookup
├── notebooks/
│   └── transaction_pipeline.ipynb  # Full pipeline notebook
├── src/
│   └── generate_data.py            # Data generator with quality issues
├── output/                         # All outputs (gitignored)
│   ├── parquet/
│   │   ├── clean_transactions/
│   │   ├── merchant_risk_profile/
│   │   └── payment_channel_performance/
│   └── csv/
│       └── flagged_transactions/
└── README.md
```

---

## Key Learnings

**Operator precedence in PySpark filters is a real bug source.**
`col("x") == "False" & col("y") == "India"` doesn't do what you think. The `&` binds tighter than `==`. Always wrap each condition in explicit parentheses.

**Cache placement matters more than caching itself.**
Caching a DataFrame and then replacing it with `.withColumn()` creates a new object — the cache is wasted. Cache after all transformations are complete, not before.

**Floating point equality is unreliable.**
`col("sla_target") == 99.9` silently fails on Double columns. Use `>=` with ordered conditions instead.

**Reading everything as StringType for audit is not optional.**
If you let Spark infer types during audit, it silently coerces bad values and hides the real shape of your data quality problems. Always audit raw, then clean.

**A schema mismatch between your StructType and the actual CSV shifts every column silently.**
One extra field in the schema mapped city names into the device_type column. `df.head(1)` immediately after read catches this — make it a habit.
