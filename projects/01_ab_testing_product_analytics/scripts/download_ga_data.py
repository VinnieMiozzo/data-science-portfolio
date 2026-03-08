import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery


DEFAULT_COLUMNS = [
    "CAST(PARSE_DATE('%Y%m%d', date) AS STRING) AS session_date",
    "TIMESTAMP_SECONDS(visitStartTime) AS session_start_ts",
    "fullVisitorId AS user_id",
    "visitId AS session_id",
    "visitNumber AS visit_number",
    "channelGrouping AS channel_grouping",
    "device.deviceCategory AS device_category",
    "device.isMobile AS is_mobile",
    "device.browser AS browser",
    "device.operatingSystem AS operating_system",
    "device.language AS device_language",
    "geoNetwork.continent AS continent",
    "geoNetwork.subContinent AS subcontinent",
    "geoNetwork.country AS country",
    "geoNetwork.region AS region",
    "geoNetwork.city AS city",
    "geoNetwork.networkDomain AS network_domain",
    "trafficSource.source AS traffic_source",
    "trafficSource.medium AS traffic_medium",
    "trafficSource.campaign AS campaign",
    "trafficSource.keyword AS keyword",
    "trafficSource.adContent AS ad_content",
    "totals.visits AS visits",
    "totals.hits AS hits",
    "totals.pageviews AS pageviews",
    "totals.timeOnSite AS time_on_site",
    "totals.bounces AS bounces",
    "totals.newVisits AS new_visits",
    "totals.transactions AS transactions",
    "totals.totalTransactionRevenue / 1000000 AS revenue",
]


def build_query(start_date: str, end_date: str, columns: list[str]) -> str:
    column_sql = ",\n  ".join(columns)
    return f"""
SELECT
  {column_sql}
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
""".strip()


def validate_date(date_str: str) -> str:
    datetime.strptime(date_str, "%Y%m%d")
    return date_str


def save_metadata(
    metadata_path: Path,
    *,
    project_id: str,
    start_date: str,
    end_date: str,
    output_path: str,
    row_count: int,
    query: str,
) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "project_id": project_id,
        "start_date": start_date,
        "end_date": end_date,
        "output_path": output_path,
        "row_count": row_count,
        "extracted_at_utc": datetime.utcnow().isoformat(),
        "query": query,
    }
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract Google Analytics sample sessions from BigQuery."
    )
    parser.add_argument("--project-id", required=True, help="Google Cloud project ID.")
    parser.add_argument("--start-date", required=True, help="Start date in YYYYMMDD.")
    parser.add_argument("--end-date", required=True, help="End date in YYYYMMDD.")
    parser.add_argument(
        "--output-path",
        required=True,
        help="Output file path, e.g. ../data/raw/ga_sessions_201708.parquet",
    )
    parser.add_argument(
        "--format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format.",
    )
    parser.add_argument(
        "--metadata-path",
        default="../data/metadata/last_extract.json",
        help="Where to save extraction metadata.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for testing.",
    )

    args = parser.parse_args()

    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    query = build_query(start_date=start_date, end_date=end_date, columns=DEFAULT_COLUMNS)
    if args.limit is not None:
        query = f"{query}\nLIMIT {args.limit}"

    logging.info("Creating BigQuery client.")
    client = bigquery.Client(project=args.project_id)

    logging.info("Running query for range %s to %s.", start_date, end_date)
    df = client.query(query).to_dataframe()

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info("Saving %s rows to %s.", len(df), output_path)
    if args.format == "parquet":
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False)

    save_metadata(
        Path(args.metadata_path),
        project_id=args.project_id,
        start_date=start_date,
        end_date=end_date,
        output_path=str(output_path),
        row_count=len(df),
        query=query,
    )

    logging.info("Done.")


if __name__ == "__main__":
    main()
