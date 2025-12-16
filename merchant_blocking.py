
from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass
from enum import Enum
from typing import List

import pandas as pd


class MerchantType(str, Enum):
    COMPANY_CT = "COMPANY_CT"
    HOUSEHOLD_HKD = "HOUSEHOLD_HKD"
    PHARMACY = "PHARMACY"
    GAS = "GAS"
    SHOP = "SHOP"
    CAFE = "CAFE"
    RESTAURANT_QUAN = "RESTAURANT_QUAN"
    HAIR_SALON = "HAIR_SALON"
    OFFICE_VP = "OFFICE_VP"
    OTHER = "OTHER"


GENERIC_TOKENS = {
    "CH", "CUA", "HANG", "TIEM",
    "SHOP", "STORE", "MART", "POS",
    "QUAN", "AN"
}

SUFFIX_CANDIDATES = {
    "BTL", "Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9",
    "Q10", "Q11", "Q12", "GO", "VAP", "GV", "OCP", "CPC"
}


@dataclass
class ParsedMerchant:
    raw_name: str
    normalized: str
    tokens: List[str]
    mtype: MerchantType
    core: str
    suffix_tokens: List[str]


def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.upper().strip()
    s = s.replace("CO.OP", "COOP")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize(name: str) -> List[str]:
    if not name:
        return []
    return name.split()


def _has_sequence(tokens: List[str], seq: List[str]) -> bool:
    if not tokens or not seq:
        return False
    n, m = len(tokens), len(seq)
    for i in range(n - m + 1):
        if tokens[i:i + m] == seq:
            return True
    return False


def detect_type(tokens: List[str]) -> MerchantType:
    if not tokens:
        return MerchantType.OTHER

    if "HKD" in tokens or _has_sequence(tokens, ["HO", "KINH", "DOANH"]):
        return MerchantType.HOUSEHOLD_HKD

    if _has_sequence(tokens, ["NHA", "THUOC"]):
        return MerchantType.PHARMACY

    if _has_sequence(tokens, ["QUAN", "AN"]) or _has_sequence(tokens, ["NHA", "HANG"]):
        return MerchantType.RESTAURANT_QUAN

    if ("SALON" in tokens and "TOC" in tokens) or _has_sequence(tokens, ["TIEM", "TOC"]):
        return MerchantType.HAIR_SALON

    if "GAS" in tokens:
        return MerchantType.GAS

    if "CAFE" in tokens or "COFFEE" in tokens:
        return MerchantType.CAFE

    if (("CUA" in tokens and "HANG" in tokens)
        or "SHOP" in tokens
        or "STORE" in tokens
        or "MART" in tokens):
        return MerchantType.SHOP

    if "VP" in tokens or _has_sequence(tokens, ["VAN", "PHONG"]):
        return MerchantType.OFFICE_VP

    if ("CT" in tokens
        or "CTY" in tokens
        or ("CONG" in tokens and "TY" in tokens)
        or "TNHH" in tokens):
        return MerchantType.COMPANY_CT

    return MerchantType.OTHER


def _strip_type_prefix(tokens: List[str], mtype: MerchantType) -> List[str]:
    t = tokens[:]

    def strip_sequence(seq: List[str]) -> List[str]:
        nonlocal t
        if _has_sequence(t, seq):
            n, m = len(t), len(seq)
            for i in range(n - m + 1):
                if t[i:i + m] == seq:
                    t = t[i + m:]
                    break
        return t

    if mtype == MerchantType.HOUSEHOLD_HKD:
        if "HKD" in t:
            idx = t.index("HKD")
            return t[idx + 1:]
        return strip_sequence(["HO", "KINH", "DOANH"])

    if mtype == MerchantType.PHARMACY:
        return strip_sequence(["NHA", "THUOC"])

    if mtype == MerchantType.RESTAURANT_QUAN:
        if _has_sequence(t, ["QUAN", "AN"]):
            return strip_sequence(["QUAN", "AN"])
        return strip_sequence(["NHA", "HANG"])

    if mtype == MerchantType.HAIR_SALON:
        if _has_sequence(t, ["SALON", "TOC"]):
            return strip_sequence(["SALON", "TOC"])
        return strip_sequence(["TIEM", "TOC"])

    if mtype == MerchantType.GAS:
        if t and t[0] == "GAS":
            return t[1:]
        return t

    if mtype == MerchantType.CAFE:
        if "CAFE" in t:
            idx = t.index("CAFE")
            return t[idx + 1:]
        if "COFFEE" in t:
            idx = t.index("COFFEE")
            return t[idx + 1:]
        return t

    if mtype == MerchantType.SHOP:
        if _has_sequence(t, ["CUA", "HANG"]):
            return strip_sequence(["CUA", "HANG"])
        if t and t[0] == "CH":
            return t[1:]
        if t and t[0] == "TIEM":
            return t[1:]
        return t

    if mtype == MerchantType.OFFICE_VP:
        if t and t[0] == "VP":
            return t[1:]
        if _has_sequence(t, ["VAN", "PHONG"]):
            return strip_sequence(["VAN", "PHONG"])
        return t

    if mtype == MerchantType.COMPANY_CT:
        i = 0
        while i < len(t) and t[i] in {"CT", "CTY", "CONG", "TY", "TNHH"}:
            i += 1
        return t[i:]

    return t


def extract_core(tokens: List[str], mtype: MerchantType) -> str:
    if not tokens:
        return ""
    t = _strip_type_prefix(tokens, mtype)
    filtered = [tok for tok in t if tok not in GENERIC_TOKENS]
    return filtered[0] if filtered else ""


def extract_suffix(tokens: List[str]) -> List[str]:
    suffix = []
    for tok in reversed(tokens):
        if tok.isdigit():
            suffix.append(tok)
        elif re.match(r"^T\d+$", tok):
            suffix.append(tok)
        elif tok in SUFFIX_CANDIDATES:
            suffix.append(tok)
        else:
            break
    return list(reversed(suffix))


def parse_merchant(name: str) -> ParsedMerchant:
    normalized = normalize_name(name)
    tokens = tokenize(normalized)
    mtype = detect_type(tokens)
    core = extract_core(tokens, mtype)
    suffix_tokens = extract_suffix(tokens)
    return ParsedMerchant(
        raw_name=name,
        normalized=normalized,
        tokens=tokens,
        mtype=mtype,
        core=core,
        suffix_tokens=suffix_tokens,
    )


def build_block_key(parsed: ParsedMerchant) -> str:
    t = parsed.mtype.value
    c = parsed.core or ""
    return f"{t}|{c}"


def prepare_blocking_dataframe(
    df: pd.DataFrame,
    col_name: str,
    source_label: str,
    row_offset: int = 0,
) -> pd.DataFrame:
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
    chunksize: int | None = 200_000,
    duckdb_path: str | None = None,
) -> None:
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
    chunksize: int | None = None,
    duckdb_path: str | None = None,
) -> None:
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


if __name__ == "__main__":
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
