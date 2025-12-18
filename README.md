Data Engineering Intern Assessment
Overview

![World Bank](https://upload.wikimedia.org/wikipedia/commons/5/56/World_Bank_logo.svg)


This project implements a small, reproducible ETL (Extract, Transform, Load) pipeline using a public dataset from the World Bank.
The goal is to demonstrate core data engineering skills: ingestion, cleaning, analytics-ready outputs, documentation, and containerized execution.

The pipeline is intentionally simple, transparent, and reproducible.

Dataset

Source: World Bank Open Data (World Development Indicators)

Dataset / Indicator: Population, total

Indicator Code: SP.POP.TOTL

Official Link: https://data.worldbank.org/indicator/SP.POP.TOTL

Download Date: 2025-12-18

Raw File: raw/worldbank\_population\_total.csv

Notes:

The raw CSV contains metadata rows before the actual header.

The dataset includes yearly population values for countries and regions.

Missing values are expected for some country–year combinations.

Project Structure
.
├── src/
│   └── run\_pipeline.py
├── raw/
│   ├── worldbank\_population\_total.csv
│   └── dataset\_raw.parquet
├── processed/
│   └── dataset\_processed.parquet
├── analytics/
│   ├── column\_profile.parquet
│   └── group\_summary.parquet
├── reports/
│   ├── data\_quality\_report.md
│   └── data\_source.md
├── requirements.txt
├── Dockerfile
└── README.md

Pipeline Overview

The pipeline performs the following ETL workflow:

raw CSV
→ raw Parquet (no modifications)
→ processed Parquet (basic cleaning)
→ analytics outputs (two derived tables)
→ reports (data quality + provenance)

Pipeline Steps

1. Ingestion (raw/)

Reads the original World Bank CSV.

Automatically detects the correct header row due to metadata lines at the top of the file.

Converts the dataset to Parquet format without modifying any values.

Writes output to:

raw/dataset\_raw.parquet

This preserves the raw dataset in an efficient, analytics-friendly format.

2. Processing (processed/)

Minimal, reproducible cleaning is applied:

Trims leading/trailing whitespace from text columns.

Removes exact duplicate rows.

Drops columns that are 100% missing (CSV export artifacts).

Preserves missing values in valid data columns.

Output:

processed/dataset\_processed.parquet

This step prepares the data for analysis while maintaining data integrity.

3. Analytics (analytics/)

Two analytical tables are generated from the processed dataset:

Column Profile

Column name

Data type

Percentage of missing values

Number of unique values
Output: analytics/column\_profile.parquet

Group Summary

Aggregates row counts by country
Output: analytics/group\_summary.parquet

These outputs demonstrate basic data profiling and aggregation.

4. Reporting (reports/)

Data Quality Report

Row counts before and after deduplication

Missingness percentage per column
Output: reports/data\_quality\_report.md

Data Source Documentation

Dataset provenance and metadata
Output: reports/data\_source.md

Outputs Produced

raw/dataset\_raw.parquet

processed/dataset\_processed.parquet

analytics/column\_profile.parquet

analytics/group\_summary.parquet

reports/data\_quality\_report.md

reports/data\_source.md

How to Run (Local)
Prerequisites

Python 3.11+

pip

Commands
py -m pip install -r requirements.txt
py src/run\_pipeline.py

Docker (Optional Bonus)

A Dockerfile is included for containerized execution of the pipeline.

Build
docker build -t de-intern-pipeline .

Run
docker run --rm -v "${PWD}:/app" de-intern-pipeline



This runs the pipeline inside a container using the same project structure and data.

Assumptions \& Design Decisions

The raw dataset must remain unmodified; all cleaning occurs only in the processed stage.

Missing values are preserved to reflect the source data accurately.

Cleaning is intentionally minimal and reproducible.

Columns with 100% missing values are removed only from the processed dataset.

The pipeline is designed for clarity and correctness rather than over-optimization.

Summary

This project demonstrates a complete, reproducible data pipeline:

Clear separation of raw, processed, analytics, and reporting layers

Efficient storage using Parquet

Explicit documentation of data quality and provenance

Local and Docker-based execution#   d a t a - e n g i n e e r i n g - i n t e r n - a s s e s s m e n t

