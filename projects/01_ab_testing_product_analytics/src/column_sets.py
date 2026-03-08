from __future__ import annotations

from pathlib import Path


AVAILABLE_COLUMNS: dict[str, str] = {
    "session_date": "CAST(PARSE_DATE('%Y%m%d', date) AS STRING) AS session_date",
    "session_start_ts": "TIMESTAMP_SECONDS(visitStartTime) AS session_start_ts",
    "user_id": "fullVisitorId AS user_id",
    "session_id": "visitId AS session_id",
    "visit_number": "visitNumber AS visit_number",
    "channel_grouping": "channelGrouping AS channel_grouping",
    "device_category": "device.deviceCategory AS device_category",
    "is_mobile": "device.isMobile AS is_mobile",
    "browser": "device.browser AS browser",
    "operating_system": "device.operatingSystem AS operating_system",
    "device_language": "device.language AS device_language",
    "continent": "geoNetwork.continent AS continent",
    "subcontinent": "geoNetwork.subContinent AS subcontinent",
    "country": "geoNetwork.country AS country",
    "region": "geoNetwork.region AS region",
    "city": "geoNetwork.city AS city",
    "network_domain": "geoNetwork.networkDomain AS network_domain",
    "traffic_source": "trafficSource.source AS traffic_source",
    "traffic_medium": "trafficSource.medium AS traffic_medium",
    "campaign": "trafficSource.campaign AS campaign",
    "keyword": "trafficSource.keyword AS keyword",
    "ad_content": "trafficSource.adContent AS ad_content",
    "visits": "totals.visits AS visits",
    "hits": "totals.hits AS hits",
    "pageviews": "totals.pageviews AS pageviews",
    "time_on_site": "totals.timeOnSite AS time_on_site",
    "bounces": "totals.bounces AS bounces",
    "new_visits": "totals.newVisits AS new_visits",
    "transactions": "totals.transactions AS transactions",
    "revenue": "totals.totalTransactionRevenue / 1000000 AS revenue",
}

DEFAULT_COLUMNS: list[str] = [
    "session_date",
    "session_start_ts",
    "user_id",
    "session_id",
    "visit_number",
    "channel_grouping",
    "device_category",
    "is_mobile",
    "browser",
    "operating_system",
    "device_language",
    "continent",
    "subcontinent",
    "country",
    "region",
    "city",
    "network_domain",
    "traffic_source",
    "traffic_medium",
    "campaign",
    "keyword",
    "ad_content",
    "visits",
    "hits",
    "pageviews",
    "time_on_site",
    "bounces",
    "new_visits",
    "transactions",
    "revenue",
]

COLUMN_PRESETS: dict[str, list[str]] = {
    "base": [
        "session_date",
        "session_start_ts",
        "user_id",
        "session_id",
        "visit_number",
        "channel_grouping",
        "visits",
        "hits",
        "pageviews",
        "time_on_site",
        "bounces",
    ],
    "marketing": [
        "traffic_source",
        "traffic_medium",
        "campaign",
        "keyword",
        "ad_content",
        "channel_grouping",
    ],
    "device": [
        "device_category",
        "is_mobile",
        "browser",
        "operating_system",
        "device_language",
    ],
    "geo": [
        "continent",
        "subcontinent",
        "country",
        "region",
        "city",
        "network_domain",
    ],
    "commerce": [
        "transactions",
        "revenue",
        "pageviews",
        "hits",
        "time_on_site",
    ],
}


def _deduplicate_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for item in items:
        normalized = item.strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)

    return result


def load_columns_from_file(path: str | None) -> list[str]:
    """
    Load column identifiers from a plain text file.

    Expected format:
    - one column per line
    - blank lines ignored
    - lines starting with '#' treated as comments
    """
    if not path:
        return []

    content = Path(path).read_text(encoding="utf-8").splitlines()
    columns: list[str] = []

    for line in content:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        columns.append(line)

    return columns


def validate_columns(columns: list[str]) -> list[str]:
    """
    Validate that all requested columns exist in AVAILABLE_COLUMNS.
    """
    invalid = [column for column in columns if column not in AVAILABLE_COLUMNS]
    if invalid:
        valid = ", ".join(sorted(AVAILABLE_COLUMNS))
        raise ValueError(
            f"Unsupported columns: {invalid}. Valid columns are: {valid}"
        )
    return columns


def resolve_columns(
    extra_columns: list[str] | None = None,
    columns_file: str | None = None,
    include_defaults: bool = True,
    presets: list[str] | None = None,
) -> list[str]:
    """
    Resolve the final ordered list of column identifiers.

    Resolution order:
    1. default columns, if enabled
    2. preset columns
    3. extra columns passed directly
    4. columns loaded from file
    """
    resolved: list[str] = []

    if include_defaults:
        resolved.extend(DEFAULT_COLUMNS)

    if presets:
        for preset in presets:
            if preset not in COLUMN_PRESETS:
                valid = ", ".join(sorted(COLUMN_PRESETS))
                raise ValueError(
                    f"Unsupported preset '{preset}'. Valid presets are: {valid}"
                )
            resolved.extend(COLUMN_PRESETS[preset])

    if extra_columns:
        resolved.extend(extra_columns)

    resolved.extend(load_columns_from_file(columns_file))

    resolved = _deduplicate_preserve_order(resolved)
    return validate_columns(resolved)


def get_select_expressions(columns: list[str]) -> list[str]:
    """
    Convert validated column identifiers into SQL SELECT expressions.
    """
    validated = validate_columns(columns)
    return [AVAILABLE_COLUMNS[column] for column in validated]
