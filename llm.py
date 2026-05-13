from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
import requests
import yaml
from tqdm import tqdm

from runtime_config import env_float, env_int, env_str

GROQ_CHAT_COMPLETIONS_URL = "https://api.groq.com/openai/v1/chat/completions"
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"

ALLOWED_OPS = [
    "contact.create",
    "contact.find_or_create",
    "contact.update",
    "deal.create",
    "deal.search",
    "deal.search_stale",
    "deal.update",
    "deal.add_tags",
    "deal.update_stage",
    "deal.mark_lost",
    "activity.create",
    "meeting.schedule",
    "deal.create_quotation",
    "report.sales_daily",
    "report.export",
    "notify.email",
    "watchdog",
]

OP_INPUT_ALLOWED: Dict[str, Set[str]] = {
    "contact.create": {"name"},
    "contact.find_or_create": {"phone", "email"},
    "contact.update": {"contact_id", "tags"},
    "deal.create": {"title", "budget", "tags", "force_create", "use_existing"},
    "deal.search": {"min_budget", "title"},
    "deal.search_stale": {"days_inactive", "stages"},
    "deal.update": {"deals", "salesperson"},
    "deal.add_tags": {"deals", "tags"},
    "deal.update_stage": {"deal", "stage", "probability"},
    "deal.mark_lost": {"deal", "reason"},
    "activity.create": {"deals", "deal", "type", "summary", "due"},
    "meeting.schedule": {"deal", "when"},
    "deal.create_quotation": {"deal", "amount"},
    "report.sales_daily": {"period", "group_by"},
    "report.export": {"format"},
    "notify.email": {"to", "subject", "attach"},
    "watchdog": {"deal", "condition"},
}

OP_REQUIRED_KEYS: Dict[str, Set[str]] = {
    "contact.create": {"name"},
    "contact.find_or_create": set(),
    "contact.update": {"contact_id"},
    "deal.create": {"title"},
    "deal.search": set(),
    "deal.search_stale": {"days_inactive"},
    "deal.update": {"deals"},
    "deal.add_tags": {"deals"},
    "deal.update_stage": {"deal", "stage"},
    "deal.mark_lost": {"deal"},
    "activity.create": set(),
    "meeting.schedule": {"deal"},
    "deal.create_quotation": {"deal"},
    "report.sales_daily": set(),
    "report.export": set(),
    "notify.email": {"to"},
    "watchdog": {"deal"},
}

OP_ONE_OF_REQUIREMENTS: Dict[str, List[Set[str]]] = {
    "contact.find_or_create": [{"phone", "email"}],
    "activity.create": [{"deals", "deal"}],
}

OP_DESCRIPTIONS: Dict[str, str] = {
    "contact.create": "Create a new CRM contact.",
    "contact.find_or_create": "Find contact by phone/email, or create if missing.",
    "contact.update": "Update existing contact fields/tags.",
    "deal.create": "Create a CRM deal/opportunity. If exact title already exists, set use_existing=true to reuse or force_create=true to create a duplicate.",
    "deal.search": "Search deals by title or budget and return matching deals (prefer title for one specific deal).",
    "deal.search_stale": "Find stale deals by inactivity and stages.",
    "deal.update": "Bulk update deal owner or fields for selected deals.",
    "deal.add_tags": "Add tags to selected deals.",
    "deal.update_stage": "Move a deal to another stage with optional probability.",
    "deal.mark_lost": "Mark deal as lost and store reason.",
    "activity.create": "Create follow-up activities/calls/tasks for deal(s).",
    "meeting.schedule": "Schedule meeting for one deal.",
    "deal.create_quotation": "Create quotation document for deal. Optional amount sets quotation total; if omitted, use deal budget.",
    "report.sales_daily": "Build sales report (today/yesterday/week/month).",
    "report.export": "Export report to PDF/CSV file.",
    "notify.email": "Send email notification, optionally with attachment.",
    "watchdog": "Check deal state/condition and emit watchdog result.",
}

OP_KEYWORDS: Dict[str, List[str]] = {
    "contact.create": ["create contact", "new contact", "создай контакт", "добавь контакт", "partner"],
    "contact.find_or_create": [
        "find or create",
        "find contact",
        "lookup contact",
        "найди или создай",
        "найди контакт",
        "phone",
        "email",
    ],
    "contact.update": ["update contact", "change contact", "обнови контакт", "contact_id", "tags"],
    "deal.create": [
        "create deal",
        "new deal",
        "create new separate deal",
        "force_create",
        "use_existing",
        "reuse existing",
        "duplicate",
        "сделка",
        "создай сделку",
        "opportunity",
    ],
    "deal.search": ["search deal", "find deals", "find deal by title", "deal title", "найди сделку по названию", "budget", "min budget"],
    "deal.search_stale": ["stale deals", "inactive deals", "неактивные сделки", "days inactive"],
    "deal.update": ["update deals", "change salesperson", "смени менеджера", "assign owner"],
    "deal.add_tags": ["add tags", "tag deals", "проставь теги", "метки"],
    "deal.update_stage": ["move stage", "change stage", "переведи в стадию", "probability"],
    "deal.mark_lost": ["mark lost", "lost reason", "сделка проиграна", "закрой как проигранную"],
    "activity.create": ["create activity", "task", "call", "todo", "создай активность", "напоминание"],
    "meeting.schedule": ["schedule meeting", "meeting", "встреча", "назначь встречу"],
    "deal.create_quotation": [
        "quotation",
        "quote",
        "кп",
        "коммерческое предложение",
        "quote amount",
        "quotation amount",
        "цена кп",
        "сумма кп",
    ],
    "report.sales_daily": ["sales report", "daily report", "отчет", "выгрузка продаж"],
    "report.export": ["export report", "pdf", "csv", "экспорт"],
    "notify.email": ["send email", "notify", "почта", "email", "уведомление"],
    "watchdog": ["watchdog", "monitor deal", "контроль сделки", "проверь условие"],
}

RESULT_COLUMNS = [
    "id",
    "parse_ok",
    "schema_ok",
    "contract_ok",
    "flow_snake_ok",
    "op_acc",
    "op_set_f1",
    "op_bag_f1",
    "op_seq_f1",
    "input_key_f1",
    "input_value_acc",
    "input_pair_f1",
    "vars_key_f1",
    "vars_value_acc",
    "vars_pair_f1",
    "attempts_used",
    "repair_used",
    "repair_success",
    "fewshot_ids",
    "retrieved_op_hints",
    "relevant_ops",
    "ref_steps",
    "pred_steps",
    "error",
]

TOKEN_RE = re.compile(r"[\w.]+", flags=re.UNICODE)


class LLMRateLimitError(RuntimeError):
    def __init__(self, message: str, retry_after_s: float = 0.0):
        super().__init__(message)
        self.retry_after_s = float(retry_after_s or 0.0)


def _split_api_keys(api_key: str) -> List[str]:
    keys = [x.strip() for x in re.split(r"[,\n;]+", str(api_key or "")) if x.strip()]
    return keys or [str(api_key or "").strip()]


def _extract_retry_after_seconds(body_text: str, headers: Optional[Dict[str, Any]] = None, default_s: float = 3.0) -> float:
    if headers:
        raw = headers.get("Retry-After")
        if raw is not None:
            try:
                v = float(str(raw).strip())
                if v > 0:
                    return v
            except Exception:
                pass

    txt = str(body_text or "")
    patterns = [
        r"try again in\s*([0-9]*\.?[0-9]+)\s*s",
        r"retry after\s*([0-9]*\.?[0-9]+)\s*seconds?",
    ]
    for p in patterns:
        m = re.search(p, txt, flags=re.IGNORECASE)
        if m:
            try:
                v = float(m.group(1))
                if v > 0:
                    return v
            except Exception:
                pass
    return max(0.5, float(default_s))


def clean_model_text(s: str) -> str:
    s = (s or "").strip().replace("\r\n", "\n")
    if not s:
        return s

    fenced = re.findall(r"```(?:yaml|yml)?\s*(.*?)```", s, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        s = fenced[0].strip()

    if s.startswith("---"):
        s = s[3:].lstrip()

    lines = s.splitlines()
    start_idx: Optional[int] = None
    for i, line in enumerate(lines):
        if re.match(r"^\s*(dsl|flow|vars|steps)\s*:", line):
            start_idx = i
            break
    if start_idx is not None and start_idx > 0:
        s = "\n".join(lines[start_idx:]).strip()

    if "\n..." in s:
        s = s.split("\n...", 1)[0].strip()

    return s.strip()


def _quote_unquoted_var_refs(s: str) -> str:
    # `${...}` в flow-формате YAML часто ломает парсер, если не взять ссылку в кавычки.
    return re.sub(r'(?<!["\'])\$\{([^}\n]+)\}(?!["\'])', r'"${\1}"', s)


def _strip_step_level_blocks(s: str) -> str:
    out = str(s or "")
    for key in ("vars", "when"):
        out = re.sub(rf"(?ms)^\s{{2}}{key}:\s*\n(?:^\s{{4}}.*\n?)+", "", out)
    return out


def _sanitize_model_yaml_text(s: str) -> str:
    t = str(s or "")
    t = _strip_step_level_blocks(t)
    t = _quote_unquoted_var_refs(t)
    # Частый некорректный шаблон от малых моделей:
    # {dsl: v0.3, flow: x, vars: {}, steps:
    #   - id: ...
    #   ...
    # }
    lines = t.splitlines()
    if lines:
        first = lines[0].strip()
        if first.startswith("{dsl:") and "steps:" in first:
            m_dsl = re.search(r"dsl:\s*([^,}]+)", first)
            m_flow = re.search(r"flow:\s*([^,}]+)", first)
            m_vars = re.search(r"vars:\s*(\{[^}]*\})", first)
            dsl_val = (m_dsl.group(1).strip() if m_dsl else "v0.3").strip("'\"")
            flow_val = (m_flow.group(1).strip() if m_flow else "manual_template").strip("'\"")
            vars_val = m_vars.group(1).strip() if m_vars else "{}"
            rest = lines[1:]
            if rest and rest[-1].strip() == "}":
                rest = rest[:-1]
            # Если закрывающая скобка корня прилипла к последнему шагу, убираем одну лишнюю скобку.
            for i in range(len(rest) - 1, -1, -1):
                if rest[i].strip():
                    if rest[i].rstrip().endswith("}}"):
                        rest[i] = rest[i].rstrip()[:-1]
                    break
            t = "\n".join(
                [
                    f"dsl: {dsl_val}",
                    f"flow: {flow_val}",
                    f"vars: {vars_val}",
                    "steps:",
                    *rest,
                ]
            )
    return t


def parse_yaml(text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    raw = str(text or "")
    try:
        obj = yaml.safe_load(raw)
        if not isinstance(obj, dict):
            return None, "YAML parsed but is not a mapping"
        return obj, None
    except Exception as e1:
        sanitized = _sanitize_model_yaml_text(raw)
        if sanitized != raw:
            try:
                obj = yaml.safe_load(sanitized)
                if not isinstance(obj, dict):
                    return None, "YAML parsed but is not a mapping"
                return obj, None
            except Exception as e2:
                return None, f"YAML parse error: {e2}"
        return None, f"YAML parse error: {e1}"


def load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    df = pd.read_csv(path)
    required = {"id", "nl_plain"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Dataset missing columns: {missing}. Found: {list(df.columns)}")
    if "dsl_yaml" not in df.columns:
        # Pipeline-режим может использовать датасеты только с запросом, без эталонного YAML.
        # Для совместимости добавляем пустую эталонную колонку.
        df["dsl_yaml"] = ""
    return df


def schema_ok(obj: Dict[str, Any]) -> Tuple[bool, str]:
    for k in ["dsl", "flow", "vars", "steps"]:
        if k not in obj:
            return False, f"missing key: {k}"
    if not isinstance(obj["vars"], dict):
        return False, "vars is not a mapping"
    if not isinstance(obj["steps"], list):
        return False, "steps is not a list"
    for i, st in enumerate(obj["steps"]):
        if not isinstance(st, dict):
            return False, f"step[{i}] not a mapping"
        for k in ["id", "op", "input"]:
            if k not in st:
                return False, f"step[{i}] missing key: {k}"
        if st["op"] not in ALLOWED_OPS:
            return False, f"step[{i}] op not allowed: {st['op']}"
        if not isinstance(st["input"], dict):
            return False, f"step[{i}].input not a mapping"
    return True, "ok"


def contract_ok(obj: Dict[str, Any]) -> Tuple[bool, str]:
    steps = obj.get("steps")
    if not isinstance(steps, list):
        return False, "steps is not a list"

    seen_step_ids: Set[str] = set()
    for i, st in enumerate(steps):
        step_id = str(st.get("id", ""))
        if not step_id:
            return False, f"step[{i}] empty id"
        if step_id in seen_step_ids:
            return False, f"duplicate step id: {step_id}"
        seen_step_ids.add(step_id)

        op = st.get("op")
        inp = st.get("input")
        if op not in OP_INPUT_ALLOWED:
            return False, f"step[{i}] unknown op for contract check: {op}"
        if not isinstance(inp, dict):
            return False, f"step[{i}].input is not a mapping"

        extra = sorted(set(inp.keys()) - OP_INPUT_ALLOWED[op])
        if extra:
            return False, f"step[{i}] extra input keys for {op}: {extra}"

        if op == "contact.create" and "name" not in inp:
            return False, f"step[{i}] contact.create requires name"
        if op == "contact.find_or_create":
            phone = inp.get("phone")
            email = inp.get("email")
            if (phone is None or str(phone).strip() == "") and (email is None or str(email).strip() == ""):
                return False, f"step[{i}] contact.find_or_create requires phone or email"
        if op == "contact.update" and "contact_id" not in inp:
            return False, f"step[{i}] contact.update requires contact_id"
        if op == "deal.create" and "title" not in inp:
            return False, f"step[{i}] deal.create requires title"
        if op == "deal.search_stale" and "days_inactive" not in inp:
            return False, f"step[{i}] deal.search_stale requires days_inactive"
        if op == "deal.update" and "deals" not in inp:
            return False, f"step[{i}] deal.update requires deals"
        if op == "deal.add_tags" and "deals" not in inp:
            return False, f"step[{i}] deal.add_tags requires deals"
        if op == "deal.update_stage":
            if "deal" not in inp or "stage" not in inp:
                return False, f"step[{i}] deal.update_stage requires deal and stage"
        if op == "deal.mark_lost" and "deal" not in inp:
            return False, f"step[{i}] deal.mark_lost requires deal"
        if op == "activity.create" and ("deals" not in inp and "deal" not in inp):
            return False, f"step[{i}] activity.create requires deals or deal"
        if op == "meeting.schedule" and "deal" not in inp:
            return False, f"step[{i}] meeting.schedule requires deal"
        if op == "deal.create_quotation" and "deal" not in inp:
            return False, f"step[{i}] deal.create_quotation requires deal"
        if op == "notify.email" and "to" not in inp:
            return False, f"step[{i}] notify.email requires to"
        if op == "watchdog" and "deal" not in inp:
            return False, f"step[{i}] watchdog requires deal"

    return True, "ok"


def _coerce_deal_spec(v: Any) -> Any:
    if isinstance(v, dict):
        return v
    if isinstance(v, (int, float)):
        return {"id": int(v)}
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        if s.isdigit() or "${" in s:
            return {"id": s}
        return {"by_title": s}
    return v


def _merge_one_of_input(inp: Dict[str, Any]) -> Dict[str, Any]:
    if "one_of" not in inp:
        return inp
    merged: Dict[str, Any] = {}
    one_of = inp.get("one_of")
    if isinstance(one_of, list):
        for item in one_of:
            if isinstance(item, dict):
                for k, v in item.items():
                    if k not in merged:
                        merged[k] = v
    out = {k: v for k, v in inp.items() if k != "one_of"}
    for k, v in merged.items():
        out.setdefault(k, v)
    return out


def _normalize_pred_obj(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        return obj

    out = dict(obj)
    vars_obj = out.get("vars")
    if not isinstance(vars_obj, dict):
        out["vars"] = {}

    steps = out.get("steps")
    if isinstance(steps, dict):
        # Принимаем steps в виде словаря: {step_id: {op,input}}
        steps_list: List[Dict[str, Any]] = []
        for sid, body in steps.items():
            if not isinstance(body, dict):
                continue
            steps_list.append(
                {
                    "id": str(sid),
                    "op": str(body.get("op") or ""),
                    "input": body.get("input") if isinstance(body.get("input"), dict) else {},
                }
            )
        steps = steps_list
    if not isinstance(steps, list):
        return out

    norm_steps: List[Dict[str, Any]] = []
    for i, raw_step in enumerate(steps):
        if not isinstance(raw_step, dict):
            continue

        op = str(raw_step.get("op") or "")
        step_id = str(raw_step.get("id") or f"s{i+1}")
        inp = raw_step.get("input")
        if not isinstance(inp, dict):
            inp = {}

        inp = _merge_one_of_input(inp)

        if op in {"activity.create", "deal.update_stage", "deal.mark_lost", "meeting.schedule", "deal.create_quotation", "watchdog"}:
            if "deal" in inp:
                inp["deal"] = _coerce_deal_spec(inp.get("deal"))

        if op in {"deal.update", "deal.add_tags", "activity.create"} and "deals" in inp:
            deals_val = inp.get("deals")
            if isinstance(deals_val, (int, float, dict)):
                inp["deals"] = [deals_val]

        allowed = OP_INPUT_ALLOWED.get(op)
        if isinstance(allowed, set):
            inp = {k: v for k, v in inp.items() if k in allowed}

        norm_steps.append({"id": step_id, "op": op, "input": inp})

    out["steps"] = norm_steps
    return out


def dump_yaml_obj(obj: Dict[str, Any]) -> str:
    return yaml.safe_dump(obj, sort_keys=False, allow_unicode=True).strip()


def flow_is_snake(flow: Any) -> bool:
    if not isinstance(flow, str):
        return False
    return bool(re.fullmatch(r"[a-z][a-z0-9_]*", flow.strip()))


def tokenize(text: str) -> List[str]:
    return [t.casefold() for t in TOKEN_RE.findall(text or "")]


def _tokenize_for_similarity(text: str) -> Set[str]:
    return set(tokenize(text))


def infer_op_hints(nl_text: str) -> Set[str]:
    low = (nl_text or "").casefold()
    hints: Set[str] = set()

    for op in ALLOWED_OPS:
        if op in low:
            hints.add(op)

    for op, words in OP_KEYWORDS.items():
        for w in words:
            if w.casefold() in low:
                hints.add(op)
                break
    return hints


def _stable_hash_token(token: str) -> int:
    return int(hashlib.blake2b(token.encode("utf-8"), digest_size=8).hexdigest(), 16)


def build_sparse_vector(text: str, dims: int = 1536) -> Dict[int, float]:
    toks = tokenize(text)
    if not toks:
        return {}

    tf = Counter(toks)
    vec: Dict[int, float] = {}

    for tok, cnt in tf.items():
        idx = _stable_hash_token(tok) % dims
        w = 1.0 + math.log1p(cnt)
        vec[idx] = vec.get(idx, 0.0) + w

    if len(toks) > 1:
        for a, b in zip(toks, toks[1:]):
            idx = _stable_hash_token(f"{a}__{b}") % dims
            vec[idx] = vec.get(idx, 0.0) + 0.35

    norm = math.sqrt(sum(v * v for v in vec.values()))
    if norm <= 0:
        return {}

    for k in list(vec.keys()):
        vec[k] = vec[k] / norm
    return vec


def sparse_dot(a: Dict[int, float], b: Dict[int, float]) -> float:
    if not a or not b:
        return 0.0
    if len(a) > len(b):
        a, b = b, a
    return sum(v * b.get(k, 0.0) for k, v in a.items())


def sparse_to_json(vec: Dict[int, float]) -> str:
    pairs = [[int(k), float(v)] for k, v in sorted(vec.items(), key=lambda x: x[0])]
    return json.dumps(pairs, ensure_ascii=False)


def sparse_from_json(s: str) -> Dict[int, float]:
    out: Dict[int, float] = {}
    try:
        pairs = json.loads(s)
        for pair in pairs:
            if isinstance(pair, list) and len(pair) == 2:
                out[int(pair[0])] = float(pair[1])
    except Exception:
        return {}
    return out


def extract_ops_from_yaml_text(yaml_text: str) -> Set[str]:
    obj, _ = parse_yaml(yaml_text)
    if not isinstance(obj, dict):
        return set()
    steps = obj.get("steps")
    if not isinstance(steps, list):
        return set()
    ops: Set[str] = set()
    for st in steps:
        if isinstance(st, dict):
            op = st.get("op")
            if isinstance(op, str) and op:
                ops.add(op)
    return ops


def dataset_fingerprint(df: pd.DataFrame, nl_col: str) -> str:
    h = hashlib.sha256()
    h.update(nl_col.encode("utf-8"))
    h.update(str(len(df)).encode("utf-8"))
    for _, row in df.iterrows():
        sid = str(row.get("id", ""))
        nl = str(row.get(nl_col, ""))
        dsl = str(row.get("dsl_yaml", ""))
        h.update(sid.encode("utf-8", errors="ignore"))
        h.update(b"\x1f")
        h.update(nl.encode("utf-8", errors="ignore"))
        h.update(b"\x1f")
        h.update(dsl.encode("utf-8", errors="ignore"))
        h.update(b"\x1e")
    return h.hexdigest()


def jaccard_tokens(a: Iterable[str], b: Iterable[str]) -> float:
    sa = set(a)
    sb = set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / float(len(sa | sb))


class SQLiteVectorStore:
    def __init__(self, db_path: Path, dims: int = 1536) -> None:
        self.db_path = db_path
        self.dims = int(dims)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._cache: List[Dict[str, Any]] = []
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS docs (
                doc_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                nl_text TEXT NOT NULL,
                dsl_yaml TEXT NOT NULL,
                ops_json TEXT NOT NULL,
                vec_json TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_docs_source_id ON docs(source_id)")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def _meta_get(self, key: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return str(row["value"]) if row else None

    def _meta_set_many(self, values: Dict[str, str]) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [(k, v) for k, v in values.items()],
        )
        self.conn.commit()

    def _doc_count(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM docs")
        row = cur.fetchone()
        return int(row["n"]) if row else 0

    def ensure_index(self, df: pd.DataFrame, nl_col: str) -> None:
        fp = dataset_fingerprint(df, nl_col)
        meta_fp = self._meta_get("dataset_fp")
        meta_nl_col = self._meta_get("nl_col")
        meta_dims = self._meta_get("dims")
        count = self._doc_count()

        if (
            meta_fp == fp
            and meta_nl_col == nl_col
            and str(self.dims) == (meta_dims or "")
            and count == len(df)
        ):
            self._load_cache()
            return

        self.rebuild_index(df, nl_col, fp)

    def rebuild_index(self, df: pd.DataFrame, nl_col: str, fp: Optional[str] = None) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM docs")

        inserts: List[Tuple[str, str, str, str, str, str]] = []
        for _, row in df.iterrows():
            sid = str(row.get("id", "")).strip()
            if not sid:
                continue
            nl_text = str(row.get(nl_col, ""))
            dsl_yaml = str(row.get("dsl_yaml", ""))
            ops = sorted(extract_ops_from_yaml_text(dsl_yaml))
            op_words: List[str] = []
            for op in ops:
                op_words.append(op)
                op_words.extend(OP_KEYWORDS.get(op, []))
            retrieval_text = "\n".join([nl_text, " ".join(op_words)])
            vec = build_sparse_vector(retrieval_text, dims=self.dims)

            inserts.append(
                (
                    sid,
                    sid,
                    nl_text,
                    dsl_yaml,
                    json.dumps(ops, ensure_ascii=False),
                    sparse_to_json(vec),
                )
            )

        cur.executemany(
            "INSERT OR REPLACE INTO docs(doc_id, source_id, nl_text, dsl_yaml, ops_json, vec_json) VALUES(?, ?, ?, ?, ?, ?)",
            inserts,
        )
        self.conn.commit()

        self._meta_set_many(
            {
                "dataset_fp": fp or dataset_fingerprint(df, nl_col),
                "nl_col": nl_col,
                "dims": str(self.dims),
                "updated_at": dt.datetime.now().isoformat(),
            }
        )
        self._load_cache()

    def _load_cache(self) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT source_id, nl_text, dsl_yaml, ops_json, vec_json FROM docs")
        rows = cur.fetchall()
        cache: List[Dict[str, Any]] = []
        for row in rows:
            ops: Set[str] = set()
            try:
                raw_ops = json.loads(row["ops_json"])
                if isinstance(raw_ops, list):
                    ops = {str(x) for x in raw_ops}
            except Exception:
                ops = set()
            cache.append(
                {
                    "source_id": str(row["source_id"]),
                    "nl_text": str(row["nl_text"]),
                    "dsl_yaml": str(row["dsl_yaml"]),
                    "ops": ops,
                    "vec": sparse_from_json(str(row["vec_json"])),
                    "tokens": _tokenize_for_similarity(str(row["nl_text"])),
                }
            )
        self._cache = cache

    def search(
        self,
        query_text: str,
        top_k: int = 10,
        exclude_source_id: Optional[str] = None,
        hinted_ops: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not self._cache:
            self._load_cache()

        q_vec = build_sparse_vector(query_text, dims=self.dims)
        q_tokens = _tokenize_for_similarity(query_text)
        hinted_ops = hinted_ops or set()

        scored: List[Dict[str, Any]] = []
        for doc in self._cache:
            if exclude_source_id and doc["source_id"] == str(exclude_source_id):
                continue
            vec_sim = sparse_dot(q_vec, doc["vec"])
            lex_sim = jaccard_tokens(q_tokens, doc["tokens"])
            op_bonus = 0.06 * float(len(hinted_ops & doc["ops"]))
            score = vec_sim + 0.25 * lex_sim + op_bonus

            scored.append(
                {
                    "source_id": doc["source_id"],
                    "nl_text": doc["nl_text"],
                    "dsl_yaml": doc["dsl_yaml"],
                    "ops": set(doc["ops"]),
                    "score": score,
                }
            )

        scored.sort(key=lambda x: (-x["score"], x["source_id"]))
        return scored[: max(1, top_k)]


def _select_lexical_candidates(
    df: pd.DataFrame,
    current_id: str,
    current_nl: str,
    nl_col: str,
    top_k: int,
    hinted_ops: Set[str],
) -> List[Dict[str, Any]]:
    current_tokens = _tokenize_for_similarity(current_nl)
    scored: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        rid = str(row["id"])
        if rid == str(current_id):
            continue
        nl = str(row.get(nl_col, ""))
        y = str(row.get("dsl_yaml", ""))
        if not nl or not y:
            continue
        cand_tokens = _tokenize_for_similarity(nl)
        union = current_tokens | cand_tokens
        score = (len(current_tokens & cand_tokens) / float(len(union))) if union else 0.0
        ops = extract_ops_from_yaml_text(y)
        score += 0.06 * len(hinted_ops & ops)
        scored.append(
            {
                "source_id": rid,
                "nl_text": nl,
                "dsl_yaml": y,
                "ops": ops,
                "score": score,
            }
        )

    scored.sort(key=lambda x: (-x["score"], x["source_id"]))
    return scored[: max(1, top_k)]


def _pick_with_op_coverage(
    candidates: Sequence[Dict[str, Any]],
    k: int,
    hinted_ops: Set[str],
) -> List[Dict[str, Any]]:
    if k <= 0 or not candidates:
        return []

    selected: List[Dict[str, Any]] = []
    selected_ids: Set[str] = set()
    covered_ops: Set[str] = set()

    def add_candidate(c: Dict[str, Any]) -> None:
        selected.append(c)
        selected_ids.add(str(c["source_id"]))
        covered_ops.update(set(c.get("ops") or set()))

    base_take = max(1, min(k, (k + 1) // 2))
    for c in candidates:
        if len(selected) >= base_take:
            break
        add_candidate(c)

    while len(selected) < k:
        missing = hinted_ops - covered_ops
        if not missing:
            break
        best: Optional[Dict[str, Any]] = None
        best_key: Tuple[int, float] = (-1, -1.0)
        for c in candidates:
            cid = str(c["source_id"])
            if cid in selected_ids:
                continue
            gain = len(set(c.get("ops") or set()) & missing)
            if gain <= 0:
                continue
            key = (gain, float(c.get("score", 0.0)))
            if key > best_key:
                best_key = key
                best = c
        if best is None:
            break
        add_candidate(best)

    for c in candidates:
        if len(selected) >= k:
            break
        cid = str(c["source_id"])
        if cid in selected_ids:
            continue
        add_candidate(c)

    return selected


def select_fewshot(
    df: pd.DataFrame,
    current_id: str,
    k: int,
    nl_col: str,
    retriever: str,
    fewshot_pool: int,
    vector_store: Optional[SQLiteVectorStore],
    current_nl_override: Optional[str] = None,
) -> Tuple[List[Tuple[str, str]], List[Dict[str, Any]], Set[str]]:
    if k <= 0 or retriever == "none":
        return [], [], set()

    if current_nl_override is not None:
        current_nl = str(current_nl_override)
    else:
        current_rows = df[df["id"].astype(str) == str(current_id)]
        current_nl = str(current_rows.iloc[0][nl_col]) if len(current_rows) > 0 else ""
    hinted_ops = infer_op_hints(current_nl)

    pool_k = max(fewshot_pool, k * 4, 6)
    if retriever in {"vector", "hybrid"} and vector_store is not None:
        candidates = vector_store.search(
            query_text=current_nl,
            top_k=pool_k,
            exclude_source_id=current_id,
            hinted_ops=hinted_ops,
        )
        if not candidates and retriever == "hybrid":
            candidates = _select_lexical_candidates(df, current_id, current_nl, nl_col, pool_k, hinted_ops)
    else:
        candidates = _select_lexical_candidates(df, current_id, current_nl, nl_col, pool_k, hinted_ops)

    picked = _pick_with_op_coverage(candidates, k=k, hinted_ops=hinted_ops)
    fewshot = [(str(x["nl_text"]), str(x["dsl_yaml"])) for x in picked]
    return fewshot, picked, hinted_ops


def format_contract_line(op: str) -> str:
    allowed = sorted(OP_INPUT_ALLOWED.get(op, set()))
    required = sorted(OP_REQUIRED_KEYS.get(op, set()))
    optional = [k for k in allowed if k not in required]
    one_of_groups = OP_ONE_OF_REQUIREMENTS.get(op, [])

    parts: List[str] = []
    if required:
        parts.append(f"required: {', '.join(required)}")
    if one_of_groups:
        alt = ["(" + " OR ".join(sorted(group)) + ")" for group in one_of_groups]
        parts.append(f"one_of: {'; '.join(alt)}")
    if optional:
        parts.append(f"optional: {', '.join(optional)}")
    if not parts:
        parts.append("all input keys optional")
    return f"- {op}: " + "; ".join(parts)


def select_relevant_ops(
    nl_instruction: str,
    picked_fewshot_meta: Sequence[Dict[str, Any]],
    hinted_ops: Set[str],
    op_doc_k: int,
) -> List[str]:
    nl_tokens = _tokenize_for_similarity(nl_instruction)
    fewshot_ops: Set[str] = set()
    for item in picked_fewshot_meta:
        fewshot_ops.update(set(item.get("ops") or set()))

    scored: List[Tuple[float, str]] = []
    for op in ALLOWED_OPS:
        desc_tokens = _tokenize_for_similarity(
            OP_DESCRIPTIONS.get(op, "") + " " + " ".join(OP_KEYWORDS.get(op, []))
        )
        lex = jaccard_tokens(nl_tokens, desc_tokens)
        score = lex
        if op in fewshot_ops:
            score += 0.50
        if op in hinted_ops:
            score += 1.20
        scored.append((score, op))

    scored.sort(key=lambda x: (-x[0], x[1]))

    selected: List[str] = []
    for op in sorted(hinted_ops):
        if op in ALLOWED_OPS and op not in selected:
            selected.append(op)

    for _, op in scored:
        if len(selected) >= op_doc_k:
            break
        if op not in selected:
            selected.append(op)

    if not selected:
        selected = ALLOWED_OPS[: max(1, op_doc_k)]
    return selected[: max(1, op_doc_k)]


def op_reference_block(relevant_ops: Sequence[str]) -> str:
    lines: List[str] = []
    for op in relevant_ops:
        desc = OP_DESCRIPTIONS.get(op, "")
        lines.append(f"- {op}: {desc}")
        lines.append(f"  {format_contract_line(op)[2:]}")
    return "\n".join(lines)


def system_prompt(relevant_ops: Sequence[str]) -> str:
    ops = "\n- ".join(ALLOWED_OPS)
    contracts = "\n".join(format_contract_line(op) for op in ALLOWED_OPS)
    refs = op_reference_block(relevant_ops)

    return (
        "You compile natural language instructions into Odoo CRM DSL YAML.\n"
        "Output only YAML. No markdown, no comments, no explanations.\n"
        "Use minimum necessary steps. Do not add unrelated or speculative actions.\n"
        "Do not invent operations outside the allowed list.\n\n"
        "Allowed operations (op):\n"
        f"- {ops}\n\n"
        "Input contracts (all ops):\n"
        f"{contracts}\n\n"
        "Relevant operation reference (retrieved for this request):\n"
        f"{refs}\n\n"
        "DSL format:\n"
        "dsl: v0.3\n"
        "flow: <snake_case>\n"
        "vars: <mapping>\n"
        "steps:\n"
        "  - id: <snake_case>\n"
        "    op: <operation>\n"
        "    input: <mapping>\n\n"
        "Variable references:\n"
        "- Use ${var} for vars.\n"
        "- Use ${step_id.field} for step outputs.\n"
        "- Do not use unsupported input keys.\n"
        "- If user asks to work with one named deal, call deal.search with input.title and pass ${search_step.deal_id} to next steps.\n"
        "- If a specific deal title is provided, avoid broad deal.search without filters.\n"
        "- For report-only requests, use report.sales_daily -> report.export -> notify.email; do not add deal operations.\n"
        "- Do not add deal.update_stage unless the user explicitly asks to move/change stage.\n"
        "- Do not add deal.create unless the user explicitly asks to create a deal.\n"
        "- Do not add deal.create_quotation unless the user explicitly asks to create quotation.\n"
        "- For deal.create duplicates: use use_existing=true to reuse existing title, or force_create=true only when user explicitly asks for a duplicate/new separate deal.\n"
    )


def build_messages(
    nl_instruction: str,
    fewshot: Sequence[Tuple[str, str]],
    relevant_ops: Sequence[str],
) -> List[Dict[str, str]]:
    msgs: List[Dict[str, str]] = [{"role": "system", "content": system_prompt(relevant_ops)}]
    for nl, y in fewshot:
        msgs.append({"role": "user", "content": nl})
        msgs.append({"role": "assistant", "content": y})
    msgs.append({"role": "user", "content": nl_instruction})
    return msgs


def build_repair_messages(
    nl_instruction: str,
    fewshot: Sequence[Tuple[str, str]],
    relevant_ops: Sequence[str],
    previous_yaml: str,
    validation_error: str,
) -> List[Dict[str, str]]:
    msgs = build_messages(nl_instruction, fewshot, relevant_ops)
    msgs.append({"role": "assistant", "content": previous_yaml})
    msgs.append(
        {
            "role": "user",
            "content": (
                "Your previous YAML failed validation.\n"
                f"Error: {validation_error}\n"
                "Fix YAML so it passes parse/schema/contract checks.\n"
                "Return only corrected YAML."
            ),
        }
    )
    return msgs


def call_groq(
    api_key: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float = 0.0,
    max_tokens: int = 2200,
    timeout_s: int = 60,
    retries: int = 5,
    wait_on_429: bool = True,
) -> str:
    api_keys = [k for k in _split_api_keys(api_key) if k]
    if not api_keys:
        raise RuntimeError("Groq API key is empty")
    payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}

    backoff = 1.0
    last_err: Optional[str] = None

    for attempt in range(retries):
        key = api_keys[min(attempt, len(api_keys) - 1)]
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        try:
            r = requests.post(GROQ_CHAT_COMPLETIONS_URL, headers=headers, json=payload, timeout=timeout_s)
            if r.status_code == 429:
                body = r.text[:500]
                retry_s = _extract_retry_after_seconds(body, headers=dict(r.headers), default_s=backoff)
                if attempt + 1 < len(api_keys):
                    last_err = f"HTTP 429 on Groq key #{attempt + 1}: {body}"
                    continue
                if wait_on_429 and attempt < retries - 1:
                    time.sleep(retry_s + 0.2)
                    backoff = max(backoff * 1.6, retry_s)
                    continue
                raise LLMRateLimitError(f"Groq rate limit: HTTP 429: {body}", retry_after_s=retry_s)
            if r.status_code in (500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}: {r.text[:500]}"
                time.sleep(backoff)
                backoff *= 1.7
                continue
            r.raise_for_status()
            data = r.json()
            text = data["choices"][0]["message"]["content"]
            return clean_model_text(text)
        except Exception as e:
            last_err = str(e)
            if isinstance(e, LLMRateLimitError):
                raise
            if "HTTP 429" in last_err or "rate limit" in last_err.lower():
                retry_s = _extract_retry_after_seconds(last_err, default_s=backoff)
                if wait_on_429 and attempt < retries - 1:
                    time.sleep(retry_s + 0.2)
                    backoff = max(backoff * 1.6, retry_s)
                    continue
                raise LLMRateLimitError(f"Groq rate limit: {last_err}", retry_after_s=retry_s)
            time.sleep(backoff)
            backoff *= 1.7

    raise RuntimeError(f"Groq call failed after {retries} retries: {last_err}")


def call_openai(
    api_key: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float = 0.0,
    max_tokens: int = 2200,
    timeout_s: int = 60,
    retries: int = 5,
    wait_on_429: bool = True,
) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}

    backoff = 1.0
    last_err: Optional[str] = None

    for attempt in range(retries):
        try:
            r = requests.post(OPENAI_CHAT_COMPLETIONS_URL, headers=headers, json=payload, timeout=timeout_s)
            if r.status_code == 429:
                body = r.text[:500]
                retry_s = _extract_retry_after_seconds(body, headers=dict(r.headers), default_s=backoff)
                if wait_on_429 and attempt < retries - 1:
                    time.sleep(retry_s + 0.2)
                    backoff = max(backoff * 1.6, retry_s)
                    continue
                raise LLMRateLimitError(f"OpenAI rate limit: HTTP 429: {body}", retry_after_s=retry_s)
            if r.status_code in (500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}: {r.text[:500]}"
                time.sleep(backoff)
                backoff *= 1.7
                continue
            r.raise_for_status()
            data = r.json()
            text = data["choices"][0]["message"]["content"]
            return clean_model_text(text)
        except Exception as e:
            last_err = str(e)
            if isinstance(e, LLMRateLimitError):
                raise
            if "HTTP 429" in last_err or "rate limit" in last_err.lower():
                retry_s = _extract_retry_after_seconds(last_err, default_s=backoff)
                if wait_on_429 and attempt < retries - 1:
                    time.sleep(retry_s + 0.2)
                    backoff = max(backoff * 1.6, retry_s)
                    continue
                raise LLMRateLimitError(f"OpenAI rate limit: {last_err}", retry_after_s=retry_s)
            time.sleep(backoff)
            backoff *= 1.7

    raise RuntimeError(f"OpenAI call failed after {retries} retries: {last_err}")


def call_llm(
    provider: str,
    api_key: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float = 0.0,
    max_tokens: int = 2200,
    timeout_s: int = 60,
    retries: int = 5,
    wait_on_429: bool = True,
) -> str:
    p = str(provider or "groq").strip().lower()
    if p == "groq":
        return call_groq(
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_s=timeout_s,
            retries=retries,
            wait_on_429=wait_on_429,
        )
    if p == "openai":
        return call_openai(
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_s=timeout_s,
            retries=retries,
            wait_on_429=wait_on_429,
        )
    raise ValueError(f"Unsupported provider: {provider}")


def validate_prediction(pred_text: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    pred_obj, parse_err = parse_yaml(pred_text)
    result = {
        "parse_ok": 0,
        "schema_ok": 0,
        "contract_ok": 0,
        "flow_snake_ok": 0,
        "error": parse_err or "",
    }
    if pred_obj is None:
        return None, result

    pred_obj = _normalize_pred_obj(pred_obj)
    result["parse_ok"] = 1
    sch_pass, sch_msg = schema_ok(pred_obj)
    result["schema_ok"] = 1 if sch_pass else 0
    result["flow_snake_ok"] = 1 if flow_is_snake(pred_obj.get("flow")) else 0
    if not sch_pass:
        result["error"] = sch_msg
        return pred_obj, result

    ctr_pass, ctr_msg = contract_ok(pred_obj)
    result["contract_ok"] = 1 if ctr_pass else 0
    if not ctr_pass:
        result["error"] = ctr_msg
    else:
        result["error"] = ""
    return pred_obj, result


def generate_with_repair(
    provider: str,
    api_key: str,
    model: str,
    nl_instruction: str,
    fewshot: Sequence[Tuple[str, str]],
    relevant_ops: Sequence[str],
    repair_attempts: int,
    temperature: float,
    max_tokens: int,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    attempts: List[Dict[str, Any]] = []
    current_yaml = ""
    current_error = ""
    pred_obj: Optional[Dict[str, Any]] = None
    validation: Dict[str, Any] = {"parse_ok": 0, "schema_ok": 0, "contract_ok": 0, "flow_snake_ok": 0, "error": ""}

    for attempt in range(max(0, repair_attempts) + 1):
        if attempt == 0:
            msgs = build_messages(nl_instruction, fewshot, relevant_ops)
        else:
            msgs = build_repair_messages(
                nl_instruction=nl_instruction,
                fewshot=fewshot,
                relevant_ops=relevant_ops,
                previous_yaml=current_yaml,
                validation_error=current_error or "unknown validation failure",
            )

        raw = call_llm(
            provider=provider,
            api_key=api_key,
            model=model,
            messages=msgs,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        current_yaml = clean_model_text(raw)
        pred_obj, validation = validate_prediction(current_yaml)
        current_error = str(validation.get("error") or "")

        attempts.append(
            {
                "attempt": attempt,
                "parse_ok": int(validation["parse_ok"]),
                "schema_ok": int(validation["schema_ok"]),
                "contract_ok": int(validation["contract_ok"]),
                "error": current_error[:300],
            }
        )

        if validation["parse_ok"] and validation["schema_ok"] and validation["contract_ok"]:
            break

    return current_yaml, pred_obj, validation, attempts


def op_accuracy(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    ref_ops = [s.get("op") for s in ref.get("steps", []) if isinstance(s, dict)]
    pred_ops = [s.get("op") for s in pred.get("steps", []) if isinstance(s, dict)]
    if not ref_ops:
        return 1.0
    n = min(len(ref_ops), len(pred_ops))
    match = sum(1 for i in range(n) if ref_ops[i] == pred_ops[i])
    return match / float(len(ref_ops))


def op_set_f1(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    ref_ops = [s.get("op") for s in ref.get("steps", []) if isinstance(s, dict) and s.get("op")]
    pred_ops = [s.get("op") for s in pred.get("steps", []) if isinstance(s, dict) and s.get("op")]
    if not ref_ops and not pred_ops:
        return 1.0
    ref_set = set(ref_ops)
    pred_set = set(pred_ops)
    tp = len(ref_set & pred_set)
    prec = tp / float(len(pred_set)) if pred_set else 0.0
    rec = tp / float(len(ref_set)) if ref_set else 0.0
    return (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0


def op_bag_f1(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    ref_ops = [s.get("op") for s in ref.get("steps", []) if isinstance(s, dict) and s.get("op")]
    pred_ops = [s.get("op") for s in pred.get("steps", []) if isinstance(s, dict) and s.get("op")]
    if not ref_ops and not pred_ops:
        return 1.0
    ref_ctr = Counter(ref_ops)
    pred_ctr = Counter(pred_ops)
    common = sum(min(ref_ctr[k], pred_ctr[k]) for k in (set(ref_ctr.keys()) | set(pred_ctr.keys())))
    prec = common / float(sum(pred_ctr.values())) if pred_ctr else 0.0
    rec = common / float(sum(ref_ctr.values())) if ref_ctr else 0.0
    return (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0


def _lcs_len(a: Sequence[str], b: Sequence[str]) -> int:
    if not a or not b:
        return 0
    m = len(a)
    n = len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        ai = a[i - 1]
        for j in range(1, n + 1):
            if ai == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
    return dp[m][n]


def op_seq_f1(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    ref_ops = [s.get("op") for s in ref.get("steps", []) if isinstance(s, dict) and s.get("op")]
    pred_ops = [s.get("op") for s in pred.get("steps", []) if isinstance(s, dict) and s.get("op")]
    if not ref_ops and not pred_ops:
        return 1.0
    lcs = _lcs_len(ref_ops, pred_ops)
    prec = lcs / float(len(pred_ops)) if pred_ops else 0.0
    rec = lcs / float(len(ref_ops)) if ref_ops else 0.0
    return (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0


def input_key_f1(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    ref_steps = [s for s in ref.get("steps", []) if isinstance(s, dict)]
    pred_steps = [s for s in pred.get("steps", []) if isinstance(s, dict)]
    if not ref_steps:
        return 1.0

    n = min(len(ref_steps), len(pred_steps))
    if n == 0:
        return 0.0

    f1s: List[float] = []
    for i in range(n):
        rk = set((ref_steps[i].get("input") or {}).keys())
        pk = set((pred_steps[i].get("input") or {}).keys())
        if not rk and not pk:
            f1s.append(1.0)
            continue
        if not pk:
            f1s.append(0.0)
            continue
        tp = len(rk & pk)
        prec = tp / float(len(pk)) if pk else 0.0
        rec = tp / float(len(rk)) if rk else 0.0
        f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
        f1s.append(f1)
    return sum(f1s) / len(f1s)


def vars_key_f1(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    rv = ref.get("vars") if isinstance(ref.get("vars"), dict) else {}
    pv = pred.get("vars") if isinstance(pred.get("vars"), dict) else {}
    rk = set(rv.keys())
    pk = set(pv.keys())
    if not rk and not pk:
        return 1.0
    tp = len(rk & pk)
    prec = tp / float(len(pk)) if pk else 0.0
    rec = tp / float(len(rk)) if rk else 0.0
    return (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0


def _norm_str(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def canonical_value(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, int):
        return float(x)
    if isinstance(x, float):
        return round(float(x), 6)
    if isinstance(x, str):
        return _norm_str(x)
    if isinstance(x, list):
        items = [canonical_value(v) for v in x]
        try:
            return tuple(sorted(items))
        except Exception:
            return tuple(items)
    if isinstance(x, dict):
        items = [(str(k), canonical_value(v)) for k, v in x.items()]
        try:
            return tuple(sorted(items))
        except Exception:
            return tuple(items)
    return _norm_str(str(x))


def flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k)
            p = f"{prefix}.{key}" if prefix else key
            out.update(flatten(v, p))
        return out
    if isinstance(obj, list):
        if all(not isinstance(v, (dict, list)) for v in obj):
            out[prefix] = canonical_value(obj)
            return out
        for i, v in enumerate(obj):
            p = f"{prefix}[{i}]"
            out.update(flatten(v, p))
        return out
    out[prefix] = canonical_value(obj)
    return out


def pair_f1(ref_map: Dict[str, Any], pred_map: Dict[str, Any]) -> float:
    ref_pairs = set((k, ref_map[k]) for k in ref_map.keys())
    pred_pairs = set((k, pred_map[k]) for k in pred_map.keys())
    if not ref_pairs and not pred_pairs:
        return 1.0
    tp = len(ref_pairs & pred_pairs)
    prec = tp / float(len(pred_pairs)) if pred_pairs else 0.0
    rec = tp / float(len(ref_pairs)) if ref_pairs else 0.0
    return (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0


def value_acc_on_common_keys(ref_map: Dict[str, Any], pred_map: Dict[str, Any]) -> float:
    common = set(ref_map.keys()) & set(pred_map.keys())
    if not common:
        return 0.0
    ok = sum(1 for k in common if ref_map.get(k) == pred_map.get(k))
    return ok / float(len(common))


def vars_value_acc(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    rv = ref.get("vars") if isinstance(ref.get("vars"), dict) else {}
    pv = pred.get("vars") if isinstance(pred.get("vars"), dict) else {}
    ref_map = {str(k): canonical_value(v) for k, v in rv.items()}
    pred_map = {str(k): canonical_value(v) for k, v in pv.items()}
    return value_acc_on_common_keys(ref_map, pred_map)


def vars_pair_f1(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    rv = ref.get("vars") if isinstance(ref.get("vars"), dict) else {}
    pv = pred.get("vars") if isinstance(pred.get("vars"), dict) else {}
    ref_map = {str(k): canonical_value(v) for k, v in rv.items()}
    pred_map = {str(k): canonical_value(v) for k, v in pv.items()}
    return pair_f1(ref_map, pred_map)


def input_value_acc(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    ref_steps = [s for s in ref.get("steps", []) if isinstance(s, dict)]
    pred_steps = [s for s in pred.get("steps", []) if isinstance(s, dict)]
    n = min(len(ref_steps), len(pred_steps))
    if n == 0:
        return 0.0
    accs: List[float] = []
    for i in range(n):
        r_in = ref_steps[i].get("input") or {}
        p_in = pred_steps[i].get("input") or {}
        r_flat = flatten(r_in, "")
        p_flat = flatten(p_in, "")
        r_flat = {k: v for k, v in r_flat.items() if k != ""}
        p_flat = {k: v for k, v in p_flat.items() if k != ""}
        accs.append(value_acc_on_common_keys(r_flat, p_flat))
    return sum(accs) / len(accs)


def input_pair_f1(ref: Dict[str, Any], pred: Dict[str, Any]) -> float:
    ref_steps = [s for s in ref.get("steps", []) if isinstance(s, dict)]
    pred_steps = [s for s in pred.get("steps", []) if isinstance(s, dict)]
    n = min(len(ref_steps), len(pred_steps))
    if n == 0:
        return 0.0
    f1s: List[float] = []
    for i in range(n):
        r_in = ref_steps[i].get("input") or {}
        p_in = pred_steps[i].get("input") or {}
        r_flat = flatten(r_in, "")
        p_flat = flatten(p_in, "")
        r_flat = {k: v for k, v in r_flat.items() if k != ""}
        p_flat = {k: v for k, v in p_flat.items() if k != ""}
        f1s.append(pair_f1(r_flat, p_flat))
    return sum(f1s) / len(f1s)


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def run_eval(
    data_path: Path,
    outdir: Path,
    provider: str,
    model: str,
    nl_col: str,
    fewshot_k: int,
    limit: int,
    sleep_s: float,
    retriever: str,
    vector_db: Path,
    vector_dims: int,
    fewshot_pool: int,
    op_doc_k: int,
    repair_attempts: int,
    temperature: float,
    max_tokens: int,
) -> Path:
    p = str(provider or "groq").strip().lower()
    api_env = "GROQ_API_KEY" if p == "groq" else "OPENAI_API_KEY"
    api_key = os.environ.get("GROQ_API_KEYS" if p == "groq" else api_env, "").strip()
    if not api_key:
        api_key = os.environ.get(api_env, "").strip()
    if not api_key:
        raise RuntimeError(f"{api_env} is not set. Set it in PowerShell before run.")

    df = load_dataset(data_path)
    if nl_col not in df.columns:
        raise ValueError(f"Column '{nl_col}' not found in dataset. Available columns: {list(df.columns)}")

    vector_store: Optional[SQLiteVectorStore] = None
    if retriever in {"vector", "hybrid"} and fewshot_k > 0:
        vector_store = SQLiteVectorStore(vector_db, dims=vector_dims)
        vector_store.ensure_index(df, nl_col)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    run_dir = outdir / f"{p}_{model.replace('/', '_')}_{ts}"
    preds_dir = run_dir / "preds"
    run_dir.mkdir(parents=True, exist_ok=True)
    preds_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "config.json").write_text(
        json.dumps(
            {
                "provider": p,
                "model": model,
                "nl_col": nl_col,
                "fewshot_k": fewshot_k,
                "limit": limit,
                "sleep_s": sleep_s,
                "data_path": str(data_path),
                "retriever": retriever,
                "vector_db": str(vector_db),
                "vector_dims": vector_dims,
                "fewshot_pool": fewshot_pool,
                "op_doc_k": op_doc_k,
                "repair_attempts": repair_attempts,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    rows: List[Dict[str, Any]] = []
    work_df = df.head(limit) if limit > 0 else df

    try:
        for _, row in tqdm(work_df.iterrows(), total=len(work_df), desc="LLM -> DSL"):
            sid = str(row["id"])
            nl = str(row[nl_col])
            ref_yaml = str(row["dsl_yaml"])

            ref_obj, ref_err = parse_yaml(ref_yaml)
            if ref_obj is None:
                rows.append(
                    {
                        "id": sid,
                        "parse_ok": 0,
                        "schema_ok": 0,
                        "contract_ok": 0,
                        "flow_snake_ok": 0,
                        "op_acc": 0.0,
                        "op_set_f1": 0.0,
                        "op_bag_f1": 0.0,
                        "op_seq_f1": 0.0,
                        "input_key_f1": 0.0,
                        "input_value_acc": 0.0,
                        "input_pair_f1": 0.0,
                        "vars_key_f1": 0.0,
                        "vars_value_acc": 0.0,
                        "vars_pair_f1": 0.0,
                        "attempts_used": 0,
                        "repair_used": 0,
                        "repair_success": 0,
                        "fewshot_ids": "",
                        "retrieved_op_hints": "",
                        "relevant_ops": "",
                        "ref_steps": "",
                        "pred_steps": "",
                        "error": f"REFERENCE YAML ERROR: {ref_err}",
                    }
                )
                continue

            fewshot, fewshot_meta, hinted_ops = select_fewshot(
                df=df,
                current_id=sid,
                k=fewshot_k,
                nl_col=nl_col,
                retriever=retriever,
                fewshot_pool=fewshot_pool,
                vector_store=vector_store,
            )
            relevant_ops = select_relevant_ops(
                nl_instruction=nl,
                picked_fewshot_meta=fewshot_meta,
                hinted_ops=hinted_ops,
                op_doc_k=op_doc_k,
            )

            pred_text = ""
            pred_obj: Optional[Dict[str, Any]] = None
            validation = {"parse_ok": 0, "schema_ok": 0, "contract_ok": 0, "flow_snake_ok": 0, "error": ""}
            attempt_records: List[Dict[str, Any]] = []

            try:
                pred_text, pred_obj, validation, attempt_records = generate_with_repair(
                    provider=p,
                    api_key=api_key,
                    model=model,
                    nl_instruction=nl,
                    fewshot=fewshot,
                    relevant_ops=relevant_ops,
                    repair_attempts=repair_attempts,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            except Exception as e:
                validation = {
                    "parse_ok": 0,
                    "schema_ok": 0,
                    "contract_ok": 0,
                    "flow_snake_ok": 0,
                    "error": str(e),
                }
                attempt_records = [
                    {
                        "attempt": 0,
                        "parse_ok": 0,
                        "schema_ok": 0,
                        "contract_ok": 0,
                        "error": str(e)[:300],
                    }
                ]

            oa = op_accuracy(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            osf1 = op_set_f1(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            obf1 = op_bag_f1(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            oqf1 = op_seq_f1(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            kf1 = input_key_f1(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            iva = input_value_acc(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            ipf1 = input_pair_f1(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            vkf1 = vars_key_f1(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            vva = vars_value_acc(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0
            vpf1 = vars_pair_f1(ref_obj, pred_obj) if (ref_obj and pred_obj) else 0.0

            (preds_dir / f"{sid}.yaml").write_text(pred_text or "", encoding="utf-8")

            attempts_used = max(0, len(attempt_records) - 1)
            repair_used = 1 if attempts_used > 0 else 0
            repair_success = 1 if (repair_used and validation["contract_ok"] == 1) else 0

            rows.append(
                {
                    "id": sid,
                    "parse_ok": int(validation["parse_ok"]),
                    "schema_ok": int(validation["schema_ok"]),
                    "contract_ok": int(validation["contract_ok"]),
                    "flow_snake_ok": int(validation["flow_snake_ok"]),
                    "op_acc": round(oa, 4),
                    "op_set_f1": round(osf1, 4),
                    "op_bag_f1": round(obf1, 4),
                    "op_seq_f1": round(oqf1, 4),
                    "input_key_f1": round(kf1, 4),
                    "input_value_acc": round(iva, 4),
                    "input_pair_f1": round(ipf1, 4),
                    "vars_key_f1": round(vkf1, 4),
                    "vars_value_acc": round(vva, 4),
                    "vars_pair_f1": round(vpf1, 4),
                    "attempts_used": attempts_used,
                    "repair_used": repair_used,
                    "repair_success": repair_success,
                    "fewshot_ids": ",".join(str(x["source_id"]) for x in fewshot_meta),
                    "retrieved_op_hints": ",".join(sorted(hinted_ops)),
                    "relevant_ops": ",".join(relevant_ops),
                    "ref_steps": len(ref_obj.get("steps", [])),
                    "pred_steps": len(pred_obj.get("steps", [])) if pred_obj else "",
                    "error": str(validation.get("error", ""))[:800],
                }
            )

            if sleep_s > 0:
                time.sleep(sleep_s)
    finally:
        if vector_store is not None:
            vector_store.close()

    out_csv = run_dir / "results.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RESULT_COLUMNS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in RESULT_COLUMNS})

    n = len(rows)
    denom = max(1, n)
    parse_rate = sum(int(r["parse_ok"]) for r in rows) / denom
    schema_rate = sum(int(r["schema_ok"]) for r in rows) / denom
    contract_rate = sum(int(r["contract_ok"]) for r in rows) / denom
    flow_rate = sum(int(r["flow_snake_ok"]) for r in rows) / denom
    avg_op = sum(_safe_float(r["op_acc"]) for r in rows) / denom
    avg_op_set_f1 = sum(_safe_float(r["op_set_f1"]) for r in rows) / denom
    avg_op_bag_f1 = sum(_safe_float(r["op_bag_f1"]) for r in rows) / denom
    avg_op_seq_f1 = sum(_safe_float(r["op_seq_f1"]) for r in rows) / denom
    avg_in_key_f1 = sum(_safe_float(r["input_key_f1"]) for r in rows) / denom
    avg_in_val_acc = sum(_safe_float(r["input_value_acc"]) for r in rows) / denom
    avg_in_pair_f1 = sum(_safe_float(r["input_pair_f1"]) for r in rows) / denom
    avg_vars_key = sum(_safe_float(r["vars_key_f1"]) for r in rows) / denom
    avg_vars_val_acc = sum(_safe_float(r["vars_value_acc"]) for r in rows) / denom
    avg_vars_pair_f1 = sum(_safe_float(r["vars_pair_f1"]) for r in rows) / denom
    avg_attempts = sum(_safe_float(r["attempts_used"]) for r in rows) / denom

    repair_used_cnt = sum(int(r["repair_used"]) for r in rows)
    repair_success_cnt = sum(int(r["repair_success"]) for r in rows)
    repair_trigger_rate = repair_used_cnt / denom
    repair_success_rate = (repair_success_cnt / repair_used_cnt) if repair_used_cnt > 0 else 0.0

    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "n": n,
                "parse_rate": round(parse_rate, 4),
                "schema_rate": round(schema_rate, 4),
                "contract_rate": round(contract_rate, 4),
                "flow_snake_rate": round(flow_rate, 4),
                "avg_op_acc": round(avg_op, 4),
                "avg_op_set_f1": round(avg_op_set_f1, 4),
                "avg_op_bag_f1": round(avg_op_bag_f1, 4),
                "avg_op_seq_f1": round(avg_op_seq_f1, 4),
                "avg_input_key_f1": round(avg_in_key_f1, 4),
                "avg_input_value_acc": round(avg_in_val_acc, 4),
                "avg_input_pair_f1": round(avg_in_pair_f1, 4),
                "avg_vars_key_f1": round(avg_vars_key, 4),
                "avg_vars_value_acc": round(avg_vars_val_acc, 4),
                "avg_vars_pair_f1": round(avg_vars_pair_f1, 4),
                "avg_attempts_used": round(avg_attempts, 4),
                "repair_trigger_rate": round(repair_trigger_rate, 4),
                "repair_success_rate": round(repair_success_rate, 4),
                "retriever": retriever,
                "vector_db": str(vector_db),
                "op_doc_k": op_doc_k,
                "fewshot_pool": fewshot_pool,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("\n=== SUMMARY ===")
    print(f"run_dir: {run_dir}")
    print(f"parse_rate: {parse_rate:.3f}")
    print(f"schema_rate: {schema_rate:.3f}")
    print(f"contract_rate: {contract_rate:.3f}")
    print(f"flow_snake_rate: {flow_rate:.3f}")
    print(f"avg_op_acc: {avg_op:.3f}")
    print(f"avg_op_set_f1: {avg_op_set_f1:.3f}")
    print(f"avg_op_bag_f1: {avg_op_bag_f1:.3f}")
    print(f"avg_op_seq_f1: {avg_op_seq_f1:.3f}")
    print(f"avg_input_key_f1: {avg_in_key_f1:.3f}")
    print(f"avg_input_value_acc: {avg_in_val_acc:.3f}")
    print(f"avg_input_pair_f1: {avg_in_pair_f1:.3f}")
    print(f"avg_vars_key_f1: {avg_vars_key:.3f}")
    print(f"avg_vars_value_acc: {avg_vars_val_acc:.3f}")
    print(f"avg_vars_pair_f1: {avg_vars_pair_f1:.3f}")
    print(f"avg_attempts_used: {avg_attempts:.3f}")
    print(f"repair_trigger_rate: {repair_trigger_rate:.3f}")
    print(f"repair_success_rate: {repair_success_rate:.3f}")
    print(f"results: {out_csv}")

    return run_dir


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=str, default="nl_dsl_scenarios_odoo_API.csv", help="CSV с колонками id,nl_plain,dsl_yaml")
    ap.add_argument("--outdir", type=str, default="llm_runs", help="Папка для результатов запусков")
    ap.add_argument("--provider", type=str, default=env_str("LLM_PROVIDER", "groq"), choices=["groq", "openai"], help="LLM-провайдер")
    ap.add_argument("--model", type=str, default=env_str("LLM_MODEL", "llama-3.3-70b-versatile"), help="ID модели для выбранного провайдера")
    ap.add_argument("--nl_col", type=str, default="nl_plain", help="Какая NL-колонка используется")
    ap.add_argument("--fewshot_k", type=int, default=env_int("LLM_FEWSHOT_K", 2), help="Сколько few-shot примеров добавлять")
    ap.add_argument("--limit", type=int, default=0, help="Лимит строк, 0 означает все строки")
    ap.add_argument("--sleep", type=float, default=env_float("LLM_SLEEP", 1.0), help="Пауза в секундах между запросами")

    ap.add_argument(
        "--retriever",
        type=str,
        default=env_str("LLM_RETRIEVER", "hybrid"),
        choices=["none", "lexical", "vector", "hybrid"],
        help="Режим few-shot retriever",
    )
    ap.add_argument("--vector_db", type=str, default=".cache/llm_vector_store.sqlite", help="Путь к SQLite vector DB")
    ap.add_argument("--vector_dims", type=int, default=env_int("LLM_VECTOR_DIMS", 1536), help="Размерность hashed vector")
    ap.add_argument("--fewshot_pool", type=int, default=env_int("LLM_FEWSHOT_POOL", 12), help="Пул retriever-кандидатов перед финальным выбором few-shot")
    ap.add_argument("--op_doc_k", type=int, default=env_int("LLM_OP_DOC_K", 8), help="Сколько описаний операций добавлять в prompt")
    ap.add_argument("--repair_attempts", type=int, default=env_int("LLM_REPAIR_ATTEMPTS", 2), help="Сколько repair-итераций делать при ошибке YAML-валидации")
    ap.add_argument("--temperature", type=float, default=env_float("LLM_TEMPERATURE", 0.0), help="Температура генерации")
    ap.add_argument("--max_tokens", type=int, default=env_int("LLM_MAX_TOKENS", 2200), help="Максимум completion-токенов")

    args = ap.parse_args()

    run_eval(
        data_path=Path(args.data),
        outdir=Path(args.outdir),
        provider=args.provider,
        model=args.model,
        nl_col=args.nl_col,
        fewshot_k=args.fewshot_k,
        limit=args.limit,
        sleep_s=args.sleep,
        retriever=args.retriever,
        vector_db=Path(args.vector_db),
        vector_dims=args.vector_dims,
        fewshot_pool=args.fewshot_pool,
        op_doc_k=args.op_doc_k,
        repair_attempts=args.repair_attempts,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
    )


if __name__ == "__main__":
    main()
