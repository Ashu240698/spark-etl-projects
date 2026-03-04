# Spark ETL Projects

Production-grade PySpark ETL pipelines — built from scratch with 
real-world data quality issues, business logic, and optimized outputs.

## Projects

### Project 2 — Financial Transaction Intelligence Pipeline
- **Domain:** Payment processing and fraud detection
- **Sources:** Multi-channel transaction files (UPI, NEFT, RTGS, IMPS, Card)
- **Pipeline:** Ingest → Validate → Enrich → Fraud Detection → Risk Score → Output
- **Key concepts:** Multi-source ingestion, fraud pattern detection, 
  merchant risk profiling, time-based analysis, compliance reporting,
  partitioned Parquet, broadcast joins

### Tech Stack
- Apache Spark 4.1.1 (PySpark)
- Python 3.12
- Parquet / CSV
- WSL2 / Ubuntu 24.04

## Structure
\`\`\`
spark-etl-projects/
└── project2-transaction-intelligence/
    ├── src/          # Production pipeline scripts
    ├── notebooks/    # Development notebooks
    ├── docs/         # Architecture and design docs
    ├── data/         # Raw data (gitignored)
    └── output/       # Pipeline outputs (gitignored)
\`\`\`
