from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import List


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
    """Normalize merchant name to uppercase, strip punctuation/extra spaces, and special-case CO.OP."""
    if not isinstance(name, str):
        return ""
    s = name.upper().strip()
    s = s.replace("CO.OP", "COOP")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize(name: str) -> List[str]:
    """Split a normalized name into whitespace-delimited tokens."""
    if not name:
        return []
    return name.split()


def _has_sequence(tokens: List[str], seq: List[str]) -> bool:
    """Return True if tokens contains the exact contiguous sequence seq."""
    if not tokens or not seq:
        return False
    n, m = len(tokens), len(seq)
    for i in range(n - m + 1):
        if tokens[i:i + m] == seq:
            return True
    return False


def detect_type(tokens: List[str]) -> MerchantType:
    """Heuristically detect merchant type from token patterns."""
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
    """Remove leading words that belong to the detected merchant type."""
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
    """Extract the first non-generic token after removing the type prefix."""
    if not tokens:
        return ""
    t = _strip_type_prefix(tokens, mtype)
    filtered = [tok for tok in t if tok not in GENERIC_TOKENS]
    return filtered[0] if filtered else ""


def extract_suffix(tokens: List[str]) -> List[str]:
    """Pull trailing numeric/district-style suffix tokens (e.g., Q1, T2)."""
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
    """Full parse pipeline: normalize, tokenize, classify type, core, and suffix."""
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
    """Construct the blocking key from merchant type and core."""
    t = parsed.mtype.value
    c = parsed.core or ""
    return f"{t}|{c}"
