from __future__ import annotations

import tempfile
from typing import Optional

import pandas as pd

from parsing import build_block_key, parse_merchant


def prepare_blocking_dataframe(
    df: pd.DataFrame,
    col_name: str,
    source_label: str,
    row_offset: int = 0,
) -> pd.DataFrame:
    """Parse merchants in a column into a blocking-ready dataframe slice."""
    records = []
    for i, (idx, raw) in enumerate(df[col_name].items()):
        # Keep a stable row id across chunks by adding row_offset.
        try:
            row_id = row_offset + int(idx)
        except Exception:
            row_id = row_offset + i

        parsed = parse_merchant(raw)
        records.append(
            {
                "source": source_label,
                "row_id": row_id,
                "raw_name": parsed.raw_name,
                "normalized": parsed.normalized,
                "merchant_type": parsed.mtype.value,
                "core": parsed.core,
                "suffix": " ".join(parsed.suffix_tokens),
                "block_key": build_block_key(parsed),
            }
        )
    return pd.DataFrame(records)


def build_candidate_pairs(df_block_1: pd.DataFrame, df_block_2: pd.DataFrame) -> pd.DataFrame:
    """Join two blocking dataframes on block_key to produce candidate pairs."""
    candidates = df_block_1.merge(
        df_block_2,
        on="block_key",
        how="inner",
        suffixes=("_1", "_2"),
    )
    return candidates


def run_blocking_pandas(
    input_path: str,
    col_name_1: str,
    col_name_2: str,
    output_path: str,
) -> None:
    """In-memory pandas blocking: parse both columns and inner join on block_key."""
    df = pd.read_csv(input_path)
    df_b1 = prepare_blocking_dataframe(df, col_name_1, "col1")
    df_b2 = prepare_blocking_dataframe(df, col_name_2, "col2")
    candidates = build_candidate_pairs(df_b1, df_b2)
    candidates.to_csv(output_path, index=False)
    print("Done.")
    print(f"Records col1: {len(df_b1)}")
    print(f"Records col2: {len(df_b2)}")
    print(f"Candidate pairs: {len(candidates)}")
    print(f"Output saved to: {output_path}")


def run_blocking_duckdb(
    input_path: str,
    col_name_1: str,
    col_name_2: str,
    output_path: str,
    chunksize: Optional[int] = 200_000,
    duckdb_path: Optional[str] = None,
) -> None:
    """Chunked blocking using DuckDB for on-disk join to handle larger-than-RAM data."""
    try:
        import duckdb  # type: ignore
    except ImportError as exc:  # pragma: no cover - helpful error for users
        raise SystemExit(
            "DuckDB engine requested but duckdb package is not installed. "
            "pip install duckdb to use --engine duckdb"
        ) from exc

    effective_chunksize = chunksize if chunksize and chunksize > 0 else 200_000
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = duckdb_path or f"{tmpdir}/blocking.duckdb"
        conn = duckdb.connect(db_file)

        first_chunk = True
        rows_processed = 0
        for chunk in pd.read_csv(input_path, chunksize=effective_chunksize):
            df_b1 = prepare_blocking_dataframe(chunk, col_name_1, "col1", row_offset=rows_processed)
            df_b2 = prepare_blocking_dataframe(chunk, col_name_2, "col2", row_offset=rows_processed)

            conn.register("df_b1", df_b1)
            conn.register("df_b2", df_b2)

            if first_chunk:
                conn.execute("CREATE TABLE b1 AS SELECT * FROM df_b1")
                conn.execute("CREATE TABLE b2 AS SELECT * FROM df_b2")
                first_chunk = False
            else:
                conn.execute("INSERT INTO b1 SELECT * FROM df_b1")
                conn.execute("INSERT INTO b2 SELECT * FROM df_b2")

            rows_processed += len(chunk)

        if first_chunk:
            print("No data found in input.")
            return

        join_query = """
            COPY (
                SELECT
                    b1.source AS source_1,
                    b1.row_id AS row_id_1,
                    b1.raw_name AS raw_name_1,
                    b1.normalized AS normalized_1,
                    b1.merchant_type AS merchant_type_1,
                    b1.core AS core_1,
                    b1.suffix AS suffix_1,
                    b1.block_key AS block_key,
                    b2.source AS source_2,
                    b2.row_id AS row_id_2,
                    b2.raw_name AS raw_name_2,
                    b2.normalized AS normalized_2,
                    b2.merchant_type AS merchant_type_2,
                    b2.core AS core_2,
                    b2.suffix AS suffix_2
                FROM b1
                INNER JOIN b2 USING (block_key)
            ) TO ? WITH (FORMAT CSV, HEADER TRUE)
        """
        conn.execute(join_query, [output_path])

        candidates_count = conn.execute(
            "SELECT COUNT(*) FROM b1 INNER JOIN b2 USING (block_key)"
        ).fetchone()[0]
        df_b1_count = conn.execute("SELECT COUNT(*) FROM b1").fetchone()[0]
        df_b2_count = conn.execute("SELECT COUNT(*) FROM b2").fetchone()[0]

        conn.close()

    print("Done (DuckDB engine).")
    print(f"Records col1: {df_b1_count}")
    print(f"Records col2: {df_b2_count}")
    print(f"Candidate pairs: {candidates_count}")
    print(f"Output saved to: {output_path}")


def run_blocking(
    input_path: str,
    col_name_1: str = "Merchant_Name_1",
    col_name_2: str = "Merchant_Name_2",
    output_path: str = "merchant_candidate_pairs_blocked.csv",
    engine: str = "pandas",
    chunksize: Optional[int] = None,
    duckdb_path: Optional[str] = None,
) -> None:
    """Dispatch to the chosen blocking engine (pandas or duckdb)."""
    normalized_engine = engine.lower()
    if normalized_engine == "pandas":
        run_blocking_pandas(
            input_path=input_path,
            col_name_1=col_name_1,
            col_name_2=col_name_2,
            output_path=output_path,
        )
        return

    if normalized_engine == "duckdb":
        run_blocking_duckdb(
            input_path=input_path,
            col_name_1=col_name_1,
            col_name_2=col_name_2,
            output_path=output_path,
            chunksize=chunksize,
            duckdb_path=duckdb_path,
        )
        return

    raise ValueError(f"Unknown engine: {engine}")
