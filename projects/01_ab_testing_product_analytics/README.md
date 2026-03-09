# 01 — A/B Testing Product Analytics with Google Analytics Sample Data

## Overview

This project is part of a broader professional data science portfolio designed to present analytics and data science work in a 
production-style format, rather than as notebook-only exercises.

The project is inspired by themes commonly found in product analytics, experimentation, and the MIT MicroMasters in Statistics 
and Data Science. It uses the public Google Analytics sample dataset available in BigQuery and focuses on building a realistic 
analytics workflow with modular Python code, structured extraction logic, and analysis notebooks used mainly for exploration 
and communication.

The main idea is to simulate how a real analytics or data science project would be developed:

- inspect the source data directly from BigQuery
- define a reusable extraction layer
- organize logic into maintainable Python modules
- prepare analysis-ready datasets
- frame business and experimentation-style questions

## Project Goals

This project aims to demonstrate the ability to:

- work with cloud data source through BigQuery
- explore raw source data to guide schema and extractor design
- build reusable and parameterized extraction code
- separate engineering logic from notebook-based exploration
- structure a data science project in a recruiter-friendly way
- connect exploratory analysis with product analytics and A/B testing style thinking

## Dataset

**Source:**  
`bigquery-public-data.google_analytics_sample.ga_sessions_*`

This is a public Google Analytics sample dataset hosted in Google BigQuery.

The dataset contains session-level web analytics data, including:

- session timing
- traffic acquisition information
- device characteristics
- geography
- engagement measures
- commerce-related outcomes such as transactions and revenue

## Project Structure

```text
projects/01_ab_testing_product_analytics/
├─ README.md
├─ data/
│  ├─ raw/
│  ├─ interim/
│  └─ processed/
├─ notebooks/
│  └─ 01_ga_sessions_raw_eda.ipynb
├─ scripts/
│  └─ extract_ga_sessions.py
└─ src/
   ├─ __init__.py
   ├─ column_sets.py
   ├─ query_builders.py
   ├─ extractors.py
   └─ io_utils.py
