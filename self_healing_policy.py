from __future__ import annotations

import difflib
import re
from typing import Any, Dict, List, Optional, Tuple


def norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def split_email(email: Any) -> Tuple[str, str]:
    text = str(email or "").strip().casefold()
    if "@" not in text:
        return text, ""
    local, domain = text.split("@", 1)
    return local, domain


def number_tokens(value: Any) -> set[str]:
    return set(re.findall(r"\d+", str(value or "")))


def deal_title_candidate_allowed(query: Any, candidate: Any) -> bool:
    query_numbers = number_tokens(query)
    candidate_numbers = number_tokens(candidate)
    if query_numbers and candidate_numbers and query_numbers != candidate_numbers:
        return False
    return True


def similarity(left: Any, right: Any) -> float:
    return difflib.SequenceMatcher(None, norm_text(left), norm_text(right)).ratio()


def best_fuzzy_match(
    target: str,
    candidates: List[Tuple[int, str]],
    min_score: float,
) -> Tuple[Optional[int], Optional[str], float]:
    if not target or not candidates:
        return None, None, 0.0

    best_id: Optional[int] = None
    best_label: Optional[str] = None
    best_score = 0.0

    for cid, label in candidates:
        score = similarity(target, label)
        if score > best_score:
            best_score = score
            best_id = cid
            best_label = label

    if best_score >= min_score:
        return best_id, best_label, best_score
    return None, None, best_score


def classify_self_heal(role: str, confidence: float, details: str = "") -> Dict[str, Any]:
    role_n = str(role or "").strip().casefold()
    details_n = str(details or "").casefold()
    risk = "safe"
    reason = "deterministic_or_local_normalization"

    if role_n == "deal_create":
        risk = "policy"
        reason = "existing_record_reuse_policy"
    if role_n in {"deal_title", "deal_spec", "deals_input", "stage_name", "salesperson", "user_login", "contact_lookup"}:
        risk = "risky"
        reason = "semantic_substitution"
    if any(x in details_n for x in ["fuzzy", "fallback to first", "current authenticated user", "ctx.deals", "single ctx.deals"]):
        risk = "risky"
        reason = "heuristic_fallback"
    if confidence < 0.8 and risk != "policy":
        risk = "risky"
        reason = "low_confidence"

    return {
        "risk": risk,
        "risk_reason": reason,
        "requires_confirmation": bool(risk == "risky"),
    }


def self_heal_allowed(
    role: str,
    confidence: float,
    details: str = "",
    *,
    enabled: bool,
    mode: str,
) -> bool:
    if not enabled:
        return False
    meta = classify_self_heal(role, confidence, details)
    if str(mode or "").casefold() == "confirm" and meta["risk"] == "risky":
        return False
    return True
