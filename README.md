
# Merchant Name Blocking for Data Matching

This package implements a blocking step for merchant name matching between two columns
(e.g. `Merchant_Name_1` and `Merchant_Name_2`).

## Features

- Vietnamese merchant name normalization (uppercase, punctuation removal, CO.OP -> COOP).
- Merchant type detection:
  - COMPANY_CT, HOUSEHOLD_HKD, PHARMACY, GAS, SHOP, CAFE,
    RESTAURANT_QUAN, HAIR_SALON, OFFICE_VP, OTHER.
- Core name extraction (brand / main token after removing type prefixes and generic tokens).
- Blocking key = `(merchant_type, core)`.
- Generates candidate pairs by joining two merchant lists on the blocking key.

The implementation is written in pure Python + pandas, so it is easy to:
- Scale out later using Spark / Dask by turning `parse_merchant` into a UDF.
- Integrate into an existing data pipeline.

## Usage

```bash
pip install -r requirements.txt

python merchant_blocking.py \\
    --input merchant_names_randomized.csv \\
    --col1 Merchant_Name_1 \\
    --col2 Merchant_Name_2 \\
    --output merchant_candidate_pairs_blocked.csv
```

The output CSV contains candidate pairs with:
- row_id_1, raw_name_1, merchant_type_1, core_1, suffix_1
- row_id_2, raw_name_2, merchant_type_2, core_2, suffix_2
- block_key

## Notes on Large-scale Data

- For millions of rows, the main bottlenecks will be:
  - String processing in `parse_merchant`.
  - The join on `block_key`.
- To scale:
  - Port the `parse_merchant` logic into a Spark UDF or a Dask map step.
  - Perform the blocking join (`block_key`) using distributed joins.
  - Keep `block_key` as a short string or hashed integer key to reduce memory.

This repository is deliberately kept simple so you can easily lift the core logic
into a bigger data platform.
