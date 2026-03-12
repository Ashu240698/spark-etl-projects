# Spark ETL Projects

Production-grade PySpark ETL pipelines — built from scratch with real-world data quality issues, business logic, and optimized outputs.

Each project simulates a different domain with deliberately messy source data, multi-layer pipeline architecture, and portfolio-ready documentation.

---

## Projects

### Project 1 — Database Fleet Intelligence Pipeline
- **Domain:** Infrastructure monitoring and SLA management
- **Sources:** Raw fleet metrics CSV (529 rows, 25 duplicates)
- **Pipeline:** Ingest → Clean → Enrich → Risk Scoring → Regional Benchmarking → Output
- **Key concepts:** Broadcast joins, partitioned Parquet, SLA multipliers, incident classification, partition pruning

### Project 2 — Financial Transaction Intelligence Pipeline
- **Domain:** Payment processing and fraud detection
- **Sources:** Multi-channel transaction files (UPI, NEFT, RTGS, IMPS, Card) — 1,260 rows
- **Pipeline:** Ingest → Clean → Enrich → Fraud Detection → Risk Scoring → Output
- **Key concepts:** Multi-format timestamp parsing, fraud flag rules, merchant risk profiling, channel performance analysis, broadcast joins, partitioned Parquet

---

## Tech Stack

- Apache Spark 4.1.1 (PySpark)
- Python 3.12
- Parquet / CSV
- WSL2 / Ubuntu 24.04

---

## Repository Structure

```
spark-etl-projects/
├── project1-database-fleet/
│   ├── src/           # Pipeline scripts
│   ├── notebooks/     # Development notebooks
│   ├── data/          # Raw data (gitignored)
│   └── output/        # Pipeline outputs (gitignored)
└── project2-transaction-intelligence/
    ├── src/           # Pipeline scripts
    ├── notebooks/     # Development notebooks
    ├── docs/          # Architecture and design docs
    ├── data/          # Raw data (gitignored)
    └── output/        # Pipeline outputs (gitignored)
```

---

## Design Principles

- Raw data always ingested as `StringType` — audit before cleaning
- Schema enforced explicitly — no `inferSchema` on large files
- Cleaning rules defined from audit findings — no blind transformations
- Outputs partitioned for query efficiency — partition pruning verified
- Every pipeline documented with inline comments and a project README
