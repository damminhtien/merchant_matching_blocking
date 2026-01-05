from __future__ import annotations

from blocking import run_blocking, run_blocking_duckdb, run_blocking_pandas
from parsing import (
    MerchantType,
    ParsedMerchant,
    build_block_key,
    detect_type,
    extract_core,
    extract_suffix,
    normalize_name,
    parse_merchant,
    tokenize,
)

__all__ = [
    "run_blocking",
    "run_blocking_pandas",
    "run_blocking_duckdb",
    "parse_merchant",
    "build_block_key",
    "normalize_name",
    "tokenize",
    "detect_type",
    "extract_core",
    "extract_suffix",
    "MerchantType",
    "ParsedMerchant",
]


def main() -> None:
    """CLI entrypoint for merchant blocking."""
    import argparse

    parser = argparse.ArgumentParser(description="Merchant name blocking for data matching.")
    parser.add_argument("--input", required=True, help="Input CSV path.")
    parser.add_argument("--col1", default="Merchant_Name_1", help="Column name for first merchant list.")
    parser.add_argument("--col2", default="Merchant_Name_2", help="Column name for second merchant list.")
    parser.add_argument("--output", default="merchant_candidate_pairs_blocked.csv", help="Output CSV path.")
    parser.add_argument(
        "--engine",
        default="pandas",
        choices=["pandas", "duckdb"],
        help="Execution engine. pandas = in-memory. duckdb = chunked + on-disk join.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=None,
        help="Rows per chunk when using --engine duckdb (default: 200k). Ignored for pandas.",
    )
    parser.add_argument(
        "--duckdb-path",
        default=None,
        help="Optional path to a DuckDB file for temporary tables. Defaults to a temp file.",
    )

    args = parser.parse_args()
    run_blocking(
        input_path=args.input,
        col_name_1=args.col1,
        col_name_2=args.col2,
        output_path=args.output,
        engine=args.engine,
        chunksize=args.chunksize,
        duckdb_path=args.duckdb_path,
    )


if __name__ == "__main__":
    main()
