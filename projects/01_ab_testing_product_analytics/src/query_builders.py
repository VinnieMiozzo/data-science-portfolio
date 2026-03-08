from __future__ import annotations

from .column_sets import get_select_expressions


DEFAULT_SOURCE_TABLE = "bigquery-public-data.google_analytics_sample.ga_sessions_*"


def build_ga_sessions_query(
    start_date: str,
    end_date: str,
    columns: list[str],
    where_clause: str | None = None,
    limit: int | None = None,
    source_table: str = DEFAULT_SOURCE_TABLE,
) -> str:
    """
    Build a BigQuery SQL query for extracting Google Analytics sessions.

    Parameters
    ----------
    start_date
        Inclusive start table suffix in YYYYMMDD format.
    end_date
        Inclusive end table suffix in YYYYMMDD format.
    columns
        List of validated column identifiers.
    where_clause
        Optional additional SQL predicate appended with AND.
    limit
        Optional positive row limit.
    source_table
        Fully qualified wildcard table reference.
    """
    if limit is not None and limit <= 0:
        raise ValueError("`limit` must be a positive integer when provided.")

    select_expressions = get_select_expressions(columns)
    column_sql = ",\n  ".join(select_expressions)

    query = f"""
SELECT
  {column_sql}
FROM `{source_table}`
WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
""".strip()

    if where_clause:
        query += f"\n  AND ({where_clause})"

    if limit is not None:
        query += f"\nLIMIT {limit}"

    return query
