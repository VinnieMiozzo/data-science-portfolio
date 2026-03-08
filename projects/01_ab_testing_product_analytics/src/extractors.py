from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from google.cloud import bigquery
import pandas as pd

from .column_sets import resolve_columns
from .io_utils import build_metadata_path, utc_now_iso, write_dataframe, write_metadata
from .query_builders import build_ga_sessions_query


@dataclass(frozen=True)
class ExtractionConfig:
    project_id: str
    start_date: str
    end_date: str
    output_path: str
    file_format: str = "parquet"
    limit: int | None = None
    where_clause: str | None = None
    extra_columns: list[str] | None = None
    columns_file: str | None = None
    include_defaults: bool = True
    presets: list[str] | None = None


@dataclass(frozen=True)
class ExtractionResult:
    data_path: str
    metadata_path: str
    row_count: int
    column_count: int
    selected_columns: list[str]
    query: str


class GASessionsExtractor:
    """
    Extractor for the public Google Analytics sample sessions dataset.
    """

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL and return a pandas DataFrame.
        """
        return self.client.query(query).to_dataframe()

    def extract(self, config: ExtractionConfig) -> ExtractionResult:
        """
        Run the full extraction flow:
        - resolve columns
        - build SQL
        - execute query
        - write dataset
        - write metadata
        """
        selected_columns = resolve_columns(
            extra_columns=config.extra_columns,
            columns_file=config.columns_file,
            include_defaults=config.include_defaults,
            presets=config.presets,
        )

        query = build_ga_sessions_query(
            start_date=config.start_date,
            end_date=config.end_date,
            columns=selected_columns,
            where_clause=config.where_clause,
            limit=config.limit,
        )

        df = self.run_query(query)

        data_path = write_dataframe(
            df=df,
            output_path=config.output_path,
            file_format=config.file_format,
        )
        metadata_path = build_metadata_path(data_path)

        metadata = {
            "extracted_at_utc": utc_now_iso(),
            "project_id": config.project_id,
            "start_date": config.start_date,
            "end_date": config.end_date,
            "output_path": str(data_path),
            "file_format": config.file_format,
            "row_count": len(df),
            "column_count": len(df.columns),
            "selected_columns": selected_columns,
            "query": query,
            "config": asdict(config),
            "dataframe_columns": list(df.columns),
            "dataframe_dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }

        write_metadata(metadata, metadata_path)

        return ExtractionResult(
            data_path=str(data_path),
            metadata_path=str(metadata_path),
            row_count=len(df),
            column_count=len(df.columns),
            selected_columns=selected_columns,
            query=query,
        )
