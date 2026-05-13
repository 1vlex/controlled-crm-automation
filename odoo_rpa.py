"""
Исполнитель RPA-сценариев Odoo CRM через XML-RPC.

Модуль выполняет шаги YAML DSL, пишет audit-файлы run_context и сохраняет
rollback actions. Расчет self-healing score и policy риска находятся в
self_healing_policy.py; здесь остаются handlers операций Odoo.
"""


import os
import re
import csv
import json
import time
import copy
import logging
import datetime as dt
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, Callable, List, Optional, Tuple
import xmlrpc.client
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - запасной путь для старых Python runtime
    ZoneInfo = None

from runtime_config import BASE_DIR, env_bool, env_float, env_int, env_str
import self_healing_policy as heal_policy

# ===================== КОНФИГУРАЦИЯ =====================
RAW_URL = env_str(
    "ODOO_CRM_URL",
    env_str("ODOO_URL", "http://localhost:8069"),
)
ODOO_URL = env_str("ODOO_URL", RAW_URL.split("/web")[0].rstrip("/")).rstrip("/")
ODOO_DB = env_str("ODOO_DB", "")

ODOO_EMAIL = env_str("ODOO_EMAIL", "")
ODOO_PASSWORD = env_str("ODOO_PASSWORD", "")

# Куда сохранять технические логи. Пользовательские выгрузки лежат отдельно.
LOG_DIR = str(BASE_DIR / env_str("RPA_LOG_DIR", "logs"))
STRICT = env_bool("RPA_STRICT", False)

TRY_SEND_ODOO_EMAIL = env_bool("RPA_TRY_SEND_ODOO_EMAIL", False)
SELF_HEALING_ENABLED = env_bool("RPA_SELF_HEALING_ENABLED", True)
SELF_HEAL_MODE = env_str("RPA_SELF_HEAL_MODE", "auto").casefold()
SELF_HEAL_MIN_SCORE = env_float("RPA_SELF_HEAL_MIN_SCORE", 0.85)
AUTO_CREATE_MISSING_DEALS = env_bool("RPA_AUTO_CREATE_MISSING_DEALS", False)
AUTO_CREATE_ACTIVITY_TYPE = env_bool("RPA_AUTO_CREATE_ACTIVITY_TYPE", True)
FALLBACK_TO_CURRENT_USER = env_bool("RPA_FALLBACK_TO_CURRENT_USER", False)
FALLBACK_TO_FIRST_STAGE = env_bool("RPA_FALLBACK_TO_FIRST_STAGE", False)
ALERT_ON_STEP_FAILURE = env_bool("RPA_ALERT_ON_STEP_FAILURE", True)
MAX_ALIAS_FALLBACK_DEALS = env_int("RPA_MAX_ALIAS_FALLBACK_DEALS", 5)
DEAL_TITLE_SEARCH_MIN_SCORE = env_float("RPA_DEAL_TITLE_SEARCH_MIN_SCORE", 0.88)
PHONE_HEAL_MIN_SCORE = env_float("RPA_PHONE_HEAL_MIN_SCORE", 0.96)
# Если False, дубликаты названий сделок требуют явного подтверждения:
# use_existing=true (переиспользовать) или force_create=true (создать дубликат).
DEAL_CREATE_REUSE_EXISTING = env_bool("RPA_DEAL_CREATE_REUSE_EXISTING", True)
# Интерпретируем пользовательские datetime-вводы (today/tomorrow HH:MM) в этой таймзоне.
# Можно переопределить через env: RPA_INPUT_TIMEZONE=Europe/Moscow
RPA_INPUT_TIMEZONE = env_str("RPA_INPUT_TIMEZONE", "Europe/Moscow")

# ================================================

VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")

def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def _resolve_local_dir(raw: str, default_value: str) -> Path:
    text = str(raw or "").strip() or default_value
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = BASE_DIR / path
    return path.resolve()


def _initial_artifact_dir() -> Path:
    configured = env_str("RPA_ARTIFACT_DIR", "").strip()
    return _resolve_local_dir(configured, "artifacts")


ARTIFACT_DIR = str(_initial_artifact_dir())


ensure_dir(LOG_DIR)
ensure_dir(ARTIFACT_DIR)

# --- логирование в консоль и в файл ---
log = logging.getLogger("odoo-rpa")
log.setLevel(logging.INFO)

_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

ch = logging.StreamHandler()
ch.setFormatter(_fmt)
log.addHandler(ch)

try:
    fh = logging.FileHandler(os.path.join(LOG_DIR, "run.log"), encoding="utf-8")
except Exception:
    # Запасной путь для окружений, где Unicode-путь проекта ломается shell/codepage.
    LOG_DIR = str(Path(tempfile.gettempdir()) / "odoo_rpa_logs")
    ensure_dir(LOG_DIR)
    fh = logging.FileHandler(os.path.join(LOG_DIR, "run.log"), encoding="utf-8")
fh.setFormatter(_fmt)
log.addHandler(fh)


def _parse_log_level(level: str, default_level: int = logging.INFO) -> int:
    text = str(level or "").strip().upper()
    if not text:
        return default_level
    return getattr(logging, text, default_level)


def set_console_log_level(level: str = "INFO") -> None:
    ch.setLevel(_parse_log_level(level, logging.INFO))


def set_file_log_level(level: str = "INFO") -> None:
    fh.setLevel(_parse_log_level(level, logging.INFO))

def soft_fail(msg: str):
    if STRICT:
        raise RuntimeError(msg)
    log.warning("%s (skip)", msg)
    return {"skipped": True, "reason": msg}

def utcnow_stamp() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")


_norm_text = heal_policy.norm_text
_split_email = heal_policy.split_email
_deal_title_candidate_allowed = heal_policy.deal_title_candidate_allowed
classify_self_heal = heal_policy.classify_self_heal


def _email_domain_compatible(query: str, candidate: str) -> bool:
    _, q_domain = _split_email(query)
    _, c_domain = _split_email(candidate)
    if not q_domain or not c_domain:
        return False
    if q_domain == c_domain:
        return True
    return heal_policy.similarity(q_domain, c_domain) >= 0.90


def _best_fuzzy_match(
    target: str,
    candidates: List[Tuple[int, str]],
    min_score: float = SELF_HEAL_MIN_SCORE,
) -> Tuple[Optional[int], Optional[str], float]:
    return heal_policy.best_fuzzy_match(target, candidates, min_score)


def _has_ambiguous_top_match(
    target: str,
    candidates: List[Tuple[int, str]],
    best_id: Optional[int],
    best_score: float,
    margin: float = 0.02,
) -> bool:
    if best_id is None:
        return False
    for cand_id, label in candidates:
        if int(cand_id) == int(best_id):
            continue
        if heal_policy.similarity(target, label) >= best_score - margin:
            return True
    return False


def _self_heal_allowed(role: str, confidence: float, details: str = "") -> bool:
    return heal_policy.self_heal_allowed(
        role,
        confidence,
        details,
        enabled=SELF_HEALING_ENABLED,
        mode=SELF_HEAL_MODE,
    )


class OdooClient:
    def __init__(self, url: str, db: str, email: str, password: str):
        self.url = url.rstrip("/")
        self.db = db
        self.email = email
        self.password = password

        missing = [
            name
            for name, value in {
                "ODOO_URL": self.url,
                "ODOO_DB": self.db,
                "ODOO_EMAIL": self.email,
                "ODOO_PASSWORD": self.password,
            }.items()
            if not str(value or "").strip()
        ]
        if missing:
            raise RuntimeError(f"Missing Odoo configuration: {', '.join(missing)}. Set values in .env.")

        self.common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        self.models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")

        uid = self.common.authenticate(self.db, self.email, self.password, {})
        if not uid:
            raise RuntimeError("AUTH FAILED (check DB/email/password)")
        self.uid = uid

        try:
            ver = self.common.version()
            log.info("Connected to Odoo %s | uid=%s | db=%s", ver.get("server_version"), uid, db)
        except Exception:
            log.info("Connected to Odoo | uid=%s | db=%s", uid, db)

    def execute(self, model: str, method: str, *args, **kwargs):
        return self.models.execute_kw(
            self.db, self.uid, self.password,
            model, method,
            list(args),
            kwargs or {}
        )

    def search(self, model: str, domain: List, limit: int = 0):
        if limit:
            return self.execute(model, "search", domain, limit=limit)
        return self.execute(model, "search", domain)

    def read(self, model: str, ids: List[int], fields: Optional[List[str]] = None):
        if fields:
            return self.execute(model, "read", ids, fields)
        return self.execute(model, "read", ids)

    def create(self, model: str, vals: Dict[str, Any]):
        return self.execute(model, "create", vals)

    def write(self, model: str, ids: List[int], vals: Dict[str, Any]):
        return self.execute(model, "write", ids, vals)

    def fields_get(self, model: str):
        return self.execute(model, "fields_get", [], ["string", "type"])

def model_has_field(odoo: OdooClient, model: str, field_name: str) -> bool:
    try:
        fg = odoo.fields_get(model)
        return field_name in fg
    except Exception:
        return False

def model_exists(odoo: OdooClient, model: str) -> bool:
    try:
        ids = odoo.search('ir.model', [('model', '=', model)], limit=1)
        return bool(ids)
    except Exception:
        return False

# -------------------- Контекст выполнения и резолвер переменных --------------------

@dataclass
class ExecutionContext:
    vars: Dict[str, Any]
    steps: Dict[str, Any] = field(default_factory=dict)
    aliases: Dict[str, Any] = field(default_factory=dict)
    step_traces: List[Dict[str, Any]] = field(default_factory=list)
    self_heal_events: List[Dict[str, Any]] = field(default_factory=list)
    rollback_actions: List[Dict[str, Any]] = field(default_factory=list)
    alerts: List[Dict[str, Any]] = field(default_factory=list)
    current_step_id: str = ""
    current_op: str = ""

    def set_step_result(self, step_id: str, result: Any):
        self.steps[step_id] = result

    def set_alias(self, name: str, value: Any):
        self.aliases[name] = value

    def record_self_heal(
        self,
        role: str,
        original: Any,
        healed: Any,
        confidence: float,
        details: str = "",
    ):
        risk_meta = classify_self_heal(role, float(confidence), details)
        self.self_heal_events.append(
            {
                "ts_utc": dt.datetime.utcnow().isoformat() + "Z",
                "step_id": self.current_step_id,
                "op": self.current_op,
                "role": role,
                "original": original,
                "healed": healed,
                "confidence": round(float(confidence), 4),
                **risk_meta,
                "details": details,
            }
        )

    def add_step_trace(self, trace: Dict[str, Any]):
        self.step_traces.append(trace)

    def add_rollback_action(
        self,
        action_type: str,
        model: str,
        record_id: int,
        vals: Optional[Dict[str, Any]] = None,
        details: str = "",
    ):
        try:
            rid = int(record_id)
        except Exception:
            return
        if rid <= 0:
            return
        self.rollback_actions.append(
            {
                "ts_utc": dt.datetime.utcnow().isoformat() + "Z",
                "step_id": self.current_step_id,
                "op": self.current_op,
                "type": action_type,
                "model": model,
                "record_id": rid,
                "vals": vals or {},
                "details": details,
            }
        )

    def add_alert(self, message: str, severity: str = "warning", details: str = ""):
        alert = {
            "ts_utc": dt.datetime.utcnow().isoformat() + "Z",
            "step_id": self.current_step_id,
            "op": self.current_op,
            "severity": severity,
            "message": message,
            "details": details,
        }
        self.alerts.append(alert)
        if severity == "error":
            log.error("ALERT | %s | %s", message, details)
        else:
            log.warning("ALERT | %s | %s", message, details)

    def get_path(self, path: str) -> Any:
        parts = path.split(".")
        root = parts[0]

        if root in self.aliases:
            cur = self.aliases[root]
        elif root in self.steps:
            cur = self.steps[root]
        else:
            cur = self.vars.get(root)

        for p in parts[1:]:
            if cur is None:
                return None
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                cur = getattr(cur, p, None)
        return cur

def resolve_value(value: Any, ctx: ExecutionContext) -> Any:
    if isinstance(value, str):
        m = VAR_PATTERN.fullmatch(value.strip())
        if m:
            return ctx.get_path(m.group(1).strip())

        def repl(m2):
            expr = m2.group(1).strip()
            got = ctx.get_path(expr)
            return "" if got is None else str(got)

        return VAR_PATTERN.sub(repl, value)

    if isinstance(value, list):
        return [resolve_value(v, ctx) for v in value]
    if isinstance(value, dict):
        return {k: resolve_value(v, ctx) for k, v in value.items()}
    return value

# -------------------- Реестр операций --------------------

OpFunc = Callable[[Dict[str, Any], ExecutionContext, OdooClient], Any]
OPS: Dict[str, OpFunc] = {}

def register(name: str):
    def deco(fn: OpFunc):
        OPS[name] = fn
        return fn
    return deco

# -------------------- Вспомогательные функции --------------------

def ensure_partner_category(odoo: OdooClient, name: str) -> int:
    ids = odoo.search("res.partner.category", [("name", "=", name)], limit=1)
    return ids[0] if ids else odoo.create("res.partner.category", {"name": name})

def ensure_crm_tag(odoo: OdooClient, name: str) -> int:
    ids = odoo.search("crm.tag", [("name", "=", name)], limit=1)
    return ids[0] if ids else odoo.create("crm.tag", {"name": name})

def find_stage_id(odoo: OdooClient, stage_name: str, ctx: Optional[ExecutionContext] = None) -> Optional[int]:
    ids = odoo.search("crm.stage", [("name", "=", stage_name)], limit=1)
    if ids:
        return ids[0]
    if not SELF_HEALING_ENABLED:
        return None

    # Легкий self-healing: ilike + fuzzy-подбор.
    ilike_ids = odoo.search("crm.stage", [("name", "ilike", stage_name)], limit=5)
    if ilike_ids:
        rec = odoo.read("crm.stage", [ilike_ids[0]], ["id", "name"])[0]
        if not _self_heal_allowed("stage_name", 0.85, "stage ilike fallback"):
            return None
        if ctx is not None:
            ctx.record_self_heal("stage_name", stage_name, rec.get("name"), 0.85, "stage ilike fallback")
        return ilike_ids[0]

    all_ids = odoo.search("crm.stage", [], limit=200)
    if not all_ids:
        return None
    rows = odoo.read("crm.stage", all_ids, ["id", "name"])
    candidates = [(int(r["id"]), str(r.get("name") or "")) for r in rows if r.get("id")]
    best_id, best_label, score = _best_fuzzy_match(stage_name, candidates)
    if best_id is not None:
        if not _self_heal_allowed("stage_name", score, "stage fuzzy fallback"):
            return None
        if ctx is not None:
            ctx.record_self_heal("stage_name", stage_name, best_label, score, "stage fuzzy fallback")
        return best_id

    if FALLBACK_TO_FIRST_STAGE and rows:
        if not _self_heal_allowed("stage_name", 0.5, "fallback to first available stage"):
            return None
        first = rows[0]
        try:
            sid = int(first["id"])
            sname = str(first.get("name") or sid)
            if ctx is not None:
                ctx.record_self_heal("stage_name", stage_name, sname, 0.5, "fallback to first available stage")
            return sid
        except Exception:
            return None
    return None


def find_deal_id_by_title(odoo: OdooClient, title: str, ctx: Optional[ExecutionContext] = None) -> Optional[int]:
    title = str(title or "").strip()
    if not title:
        return None

    ids = odoo.search("crm.lead", [("name", "=", title), ("type", "=", "opportunity")], limit=1)
    if ids:
        return ids[0]
    if not SELF_HEALING_ENABLED:
        return None

    # Шаг 1: ilike-кандидаты + fuzzy-порог.
    ilike_ids = odoo.search("crm.lead", [("name", "ilike", title), ("type", "=", "opportunity")], limit=20)
    if ilike_ids:
        rows = odoo.read("crm.lead", ilike_ids, ["id", "name"])
        candidates = [
            (int(r["id"]), str(r.get("name") or ""))
            for r in rows
            if r.get("id") and r.get("name") and _deal_title_candidate_allowed(title, r.get("name"))
        ]
        best_id, best_label, score = _best_fuzzy_match(
            title,
            candidates,
            min_score=max(SELF_HEAL_MIN_SCORE, DEAL_TITLE_SEARCH_MIN_SCORE),
        )
        if best_id is not None:
            if not _self_heal_allowed("deal_title", score, "deal ilike+fuzzy fallback"):
                return None
            if ctx is not None:
                ctx.record_self_heal("deal_title", title, best_label, score, "deal ilike+fuzzy fallback")
            return best_id

    # Шаг 2: fuzzy-поиск среди недавних сделок.
    recent_ids = odoo.search("crm.lead", [("type", "=", "opportunity")], limit=300)
    if not recent_ids:
        return None
    rows = odoo.read("crm.lead", recent_ids, ["id", "name"])
    candidates = [
        (int(r["id"]), str(r.get("name") or ""))
        for r in rows
        if r.get("id") and r.get("name") and _deal_title_candidate_allowed(title, r.get("name"))
    ]
    best_id, best_label, score = _best_fuzzy_match(title, candidates)
    if best_id is not None:
        if not _self_heal_allowed("deal_title", score, "deal fuzzy fallback"):
            return None
        if ctx is not None:
            ctx.record_self_heal("deal_title", title, best_label, score, "deal fuzzy fallback")
        return best_id
    return None


def suggest_similar_deal_titles(odoo: OdooClient, title: str, top_k: int = 3) -> List[str]:
    recent_ids = odoo.search("crm.lead", [("type", "=", "opportunity")], limit=300)
    if not recent_ids:
        return []
    rows = odoo.read("crm.lead", recent_ids, ["name"])
    scored: List[Tuple[float, str]] = []
    for r in rows:
        name = str(r.get("name") or "")
        if not name:
            continue
        score = heal_policy.similarity(title, name)
        scored.append((score, name))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [name for _, name in scored[:top_k]]


def find_user_by_name_or_login(odoo: OdooClient, s: str, ctx: Optional[ExecutionContext] = None) -> Optional[int]:
    if not s:
        return None

    is_login = "@" in s
    if "@" in s:
        ids = odoo.search("res.users", [("login", "=", s)], limit=1)
        if ids:
            return ids[0]
        if not SELF_HEALING_ENABLED:
            return None
        ilike_ids = odoo.search("res.users", [("login", "ilike", s)], limit=5)
        if ilike_ids:
            rows = odoo.read("res.users", ilike_ids, ["id", "login"])
            candidates = [
                (int(r["id"]), str(r.get("login") or ""))
                for r in rows
                if r.get("id") and r.get("login") and _email_domain_compatible(s, str(r.get("login") or ""))
            ]
            best_id, best_label, score = _best_fuzzy_match(s, candidates, min_score=max(SELF_HEAL_MIN_SCORE, 0.96))
            if best_id is None:
                return None
            if _has_ambiguous_top_match(s, candidates, best_id, score):
                return None
            if not _self_heal_allowed("user_login", score, "user login ilike+domain fallback"):
                return None
            rec = {"login": best_label}
            if ctx is not None:
                ctx.record_self_heal("user_login", s, rec.get("login"), score, "user login ilike+domain fallback")
            return best_id
    else:
        ids = odoo.search("res.users", [("name", "=", s)], limit=1)
        if ids:
            return ids[0]
        if not SELF_HEALING_ENABLED:
            return None
        ids = odoo.search("res.users", [("name", "ilike", s)], limit=1)
        if ids:
            if not _self_heal_allowed("salesperson", 0.85, "user name ilike fallback"):
                return None
            rec = odoo.read("res.users", [ids[0]], ["id", "name"])[0]
            if ctx is not None:
                ctx.record_self_heal("salesperson", s, rec.get("name"), 0.85, "user name ilike fallback")
            return ids[0]

    all_ids = odoo.search("res.users", [], limit=200)
    if not all_ids:
        return None
    rows = odoo.read("res.users", all_ids, ["id", "name", "login"])
    candidates: List[Tuple[int, str]] = []
    for r in rows:
        rid = int(r["id"])
        if r.get("name") and not is_login:
            candidates.append((rid, str(r["name"])))
        login = str(r.get("login") or "")
        if login and (not is_login or _email_domain_compatible(s, login)):
            candidates.append((rid, login))
    min_score = max(SELF_HEAL_MIN_SCORE, 0.96) if is_login else SELF_HEAL_MIN_SCORE
    best_id, best_label, score = _best_fuzzy_match(s, candidates, min_score=min_score)
    if best_id is not None:
        if _has_ambiguous_top_match(s, candidates, best_id, score):
            return None
        if not _self_heal_allowed("salesperson", score, "user fuzzy fallback"):
            return None
        if ctx is not None:
            ctx.record_self_heal("salesperson", s, best_label, score, "user fuzzy fallback")
        return best_id
    return None


def suggest_similar_users(odoo: OdooClient, s: str, top_k: int = 3) -> List[str]:
    all_ids = odoo.search("res.users", [], limit=200)
    if not all_ids:
        return []
    rows = odoo.read("res.users", all_ids, ["name", "login"])
    scored: List[Tuple[float, str]] = []
    for r in rows:
        for key in ("name", "login"):
            val = str(r.get(key) or "")
            if not val:
                continue
            score = heal_policy.similarity(s, val)
            scored.append((score, val))
    scored.sort(key=lambda x: x[0], reverse=True)
    uniq: List[str] = []
    for _, val in scored:
        if val not in uniq:
            uniq.append(val)
        if len(uniq) >= top_k:
            break
    return uniq


def _normalize_deal_spec(deal_spec: Any) -> Dict[str, Any]:
    if isinstance(deal_spec, dict):
        return deal_spec
    if isinstance(deal_spec, (int, float)):
        return {"id": int(deal_spec)}
    if isinstance(deal_spec, str):
        s = deal_spec.strip()
        if not s:
            return {}
        if s.isdigit():
            return {"id": int(s)}
        return {"by_title": s}
    return {}


def _deal_title_hint(deal_spec: Any) -> str:
    spec = _normalize_deal_spec(deal_spec)
    if spec.get("by_title"):
        return str(spec.get("by_title"))
    if spec.get("title"):
        return str(spec.get("title"))
    if spec.get("name"):
        return str(spec.get("name"))
    return ""


def _normalize_ids(v: Any) -> List[int]:
    if v is None:
        return []
    if isinstance(v, (int, float)):
        return [int(v)]
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return [int(s)]
        return []
    if isinstance(v, dict):
        for key in ("deals", "ids"):
            if key in v:
                nested = _normalize_ids(v.get(key))
                if nested:
                    return nested
        for key in ("deal_id", "id"):
            try:
                n = int(v.get(key))
            except Exception:
                continue
            if n > 0:
                return [n]
        return []
    if not isinstance(v, list):
        return []
    out: List[int] = []
    for x in v:
        if isinstance(x, (int, float)):
            out.append(int(x))
        elif isinstance(x, str) and x.strip().isdigit():
            out.append(int(x.strip()))
        elif isinstance(x, dict) and x.get("id") is not None:
            try:
                out.append(int(x.get("id")))
            except Exception:
                pass
    return out


def _resolve_deals_from_input_or_alias(
    input_deals: Any,
    ctx: ExecutionContext,
    op_name: str,
) -> Tuple[List[int], Optional[Dict[str, Any]]]:
    alias_deals = _normalize_ids(ctx.aliases.get("deals"))

    if input_deals is not None:
        deals = _normalize_ids(input_deals)
        if deals:
            return deals, None
        # Если явное выражение deals превратилось в пустой список (часто из-за сломанной ссылки на шаг),
        # разрешаем безопасный self-heal только когда alias содержит ровно одну сделку.
        if len(alias_deals) == 1 and _self_heal_allowed(
            "deals_input",
            0.75,
            f"{op_name}: fallback to single ctx.deals after empty explicit deals",
        ):
            ctx.record_self_heal(
                "deals_input",
                input_deals,
                alias_deals,
                0.75,
                f"{op_name}: fallback to single ctx.deals after empty explicit deals",
            )
            return alias_deals, None
        # Иначе сохраняем строгое безопасное поведение.
        return [], soft_fail(f"{op_name}: deals input resolved to empty; provide valid explicit deals")

    if not alias_deals:
        return [], None

    # Защитная проверка: избегаем массовых правок, если модель выдала некорректную ссылку на deals.
    if MAX_ALIAS_FALLBACK_DEALS > 0 and len(alias_deals) > MAX_ALIAS_FALLBACK_DEALS:
        return [], soft_fail(
            f"{op_name}: alias fallback blocked for {len(alias_deals)} deals "
            f"(limit={MAX_ALIAS_FALLBACK_DEALS}); provide explicit deals"
        )

    if SELF_HEALING_ENABLED:
        ctx.record_self_heal("deals_input", input_deals, alias_deals, 0.78, "fallback to ctx.deals alias")
    return alias_deals, None


def resolve_deal_id_from_spec(
    odoo: OdooClient,
    deal_spec: Any,
    ctx: ExecutionContext,
    op_name: str,
) -> Optional[int]:
    spec = _normalize_deal_spec(deal_spec)
    if not isinstance(spec, dict):
        return None

    for key in ("id", "deal_id"):
        if key in spec and spec.get(key):
            try:
                return int(spec.get(key))
            except Exception:
                pass

    by_title = spec.get("by_title") or spec.get("title") or spec.get("name")
    if not by_title:
        alias_deals = _normalize_ids(ctx.aliases.get("deals"))
        if len(alias_deals) == 1 and _self_heal_allowed(
            "deal_spec",
            0.72,
            f"{op_name}: fallback to single ctx.deals alias",
        ):
            ctx.record_self_heal(
                "deal_spec",
                deal_spec,
                {"id": alias_deals[0]},
                0.72,
                f"{op_name}: fallback to single ctx.deals alias",
            )
            return alias_deals[0]
        return None

    deal_id = find_deal_id_by_title(odoo, str(by_title), ctx=ctx)
    if deal_id:
        return deal_id

    if AUTO_CREATE_MISSING_DEALS and SELF_HEALING_ENABLED:
        # Запасной self-healing: создаем отсутствующую сделку, чтобы следующие действия могли продолжиться.
        vals: Dict[str, Any] = {"name": str(by_title), "type": "opportunity"}
        contact = ctx.aliases.get("contact")
        if isinstance(contact, dict) and contact.get("id"):
            vals["partner_id"] = int(contact["id"])
        try:
            new_id = odoo.create("crm.lead", vals)
            ctx.record_self_heal("deal_title_missing", by_title, by_title, 1.0, f"{op_name}: auto-created missing deal")
            return int(new_id)
        except Exception:
            return None

    return None

def get_model_id(odoo: OdooClient, model: str) -> int:
    ids = odoo.search("ir.model", [("model", "=", model)], limit=1)
    if not ids:
        raise RuntimeError(f"ir.model not found for {model}")
    return ids[0]

def parse_relative_datetime(s: str) -> dt.datetime:
    """Минимальный парсер для: 'today HH:MM', 'tomorrow HH:MM', 'yesterday', 'YYYY-MM-DD HH:MM'.
    Если распарсить не удалось, возвращает now()."""
    s = (s or "").strip().lower()
    now = dt.datetime.now()
    base = now.date()

    def parse_hhmm(txt: str) -> dt.time:
        m = re.search(r"(\d{1,2}):(\d{2})", txt)
        if not m:
            return dt.time(9, 0)
        return dt.time(int(m.group(1)), int(m.group(2)))

    if s.startswith("today"):
        t = parse_hhmm(s)
        return dt.datetime.combine(base, t)
    if s.startswith("tomorrow"):
        t = parse_hhmm(s)
        return dt.datetime.combine(base + dt.timedelta(days=1), t)
    if s.startswith("yesterday"):
        t = parse_hhmm(s)
        return dt.datetime.combine(base - dt.timedelta(days=1), t)

    m = re.match(r"(\d{4}-\d{2}-\d{2})(?:\s+(\d{1,2}:\d{2}))?", s)
    if m:
        d = dt.date.fromisoformat(m.group(1))
        t = dt.time(9, 0) if not m.group(2) else parse_hhmm(m.group(2))
        return dt.datetime.combine(d, t)

    return now

def fmt_odoo_dt(d: dt.datetime) -> str:

    return d.strftime("%Y-%m-%d %H:%M:%S")


def get_user_timezone(odoo: OdooClient) -> str:
    try:
        rows = odoo.read("res.users", [int(getattr(odoo, "uid", 0) or 0)], ["tz"])
        if rows:
            tz = rows[0].get("tz")
            if isinstance(tz, str) and tz.strip():
                return tz.strip()
    except Exception:
        pass
    return "UTC"


def to_utc_naive(local_dt: dt.datetime, tz_name: str) -> dt.datetime:
    if not tz_name or not ZoneInfo:
        return local_dt
    try:
        z = ZoneInfo(tz_name)
        if local_dt.tzinfo is None:
            aware = local_dt.replace(tzinfo=z)
        else:
            aware = local_dt.astimezone(z)
        return aware.astimezone(dt.timezone.utc).replace(tzinfo=None)
    except Exception:
        return local_dt

def write_text_file(name: str, content: str) -> str:
    ensure_dir(ARTIFACT_DIR)
    path = os.path.join(ARTIFACT_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path

def write_json_file(name: str, data: Any) -> str:
    ensure_dir(LOG_DIR)
    path = os.path.join(LOG_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    return path

def write_csv_file(name: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> str:
    ensure_dir(ARTIFACT_DIR)
    path = os.path.join(ARTIFACT_DIR, name)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})
    return path

def try_make_pdf_simple(title: str, lines: List[str], out_name: str) -> Optional[str]:
    """Создает простой PDF, если установлен reportlab; иначе возвращает None."""
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import A4
    except Exception:
        return None

    ensure_dir(ARTIFACT_DIR)
    out_path = os.path.join(ARTIFACT_DIR, out_name)
    c = canvas.Canvas(out_path, pagesize=A4)
    width, height = A4
    y = height - 50

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, title)
    y -= 30

    c.setFont("Helvetica", 10)
    for line in lines:
        if y < 60:
            c.showPage()
            c.setFont("Helvetica", 10)
            y = height - 50
        c.drawString(50, y, str(line)[:140])
        y -= 14

    c.save()
    return out_path


@register("contact.create")
def op_contact_create(inp, ctx, odoo):
    name = inp["name"]
    partner_id = odoo.create("res.partner", {"name": name, "company_type": "company"})
    res = {"id": partner_id, "name": name, "created": True, "reused": False}
    ctx.set_alias("company", res)
    return res

@register("contact.find_or_create")
def op_contact_find_or_create(inp, ctx, odoo):
    phone = inp.get("phone")
    email = inp.get("email")
    ids: List[int] = []

    if phone and email:
        ids = odoo.search("res.partner", ["|", ("phone", "=", phone), ("email", "=", email)], limit=1)
    elif phone:
        ids = odoo.search("res.partner", [("phone", "=", phone)], limit=1)
    elif email:
        ids = odoo.search("res.partner", [("email", "=", email)], limit=1)

    if ids:
        partner = odoo.read("res.partner", ids, ["id", "name", "phone", "email"])[0]
        partner["created"] = False
        partner["reused"] = True
    else:
        # Self-healing: пробуем fuzzy email/phone перед созданием нового контакта.
        healed_partner_id: Optional[int] = None
        healed_conf = 0.0
        healed_label = ""

        all_ids = odoo.search("res.partner", [], limit=500)
        if all_ids and SELF_HEALING_ENABLED:
            rows = odoo.read("res.partner", all_ids, ["id", "name", "phone", "mobile", "email"])
            candidates: List[Tuple[int, str]] = []
            if email:
                _, q_domain = _split_email(email)
                for r in rows:
                    em = str(r.get("email") or "")
                    if em:
                        _, c_domain = _split_email(em)
                        # Держим fuzzy-сравнение email внутри одного домена, чтобы не смешивать разные контакты.
                        if q_domain and c_domain and q_domain != c_domain:
                            continue
                        candidates.append((int(r["id"]), em))
                # Исправление опечаток в email должно быть строгим, иначе создаем новый контакт.
                healed_partner_id, healed_label, healed_conf = _best_fuzzy_match(
                    str(email),
                    candidates,
                    min_score=max(SELF_HEAL_MIN_SCORE, 0.96),
                )

            if healed_partner_id is None and phone:
                p_digits = re.sub(r"\D", "", str(phone))
                phone_cands: List[Tuple[int, str]] = []
                for r in rows:
                    p1 = re.sub(r"\D", "", str(r.get("phone") or ""))
                    p2 = re.sub(r"\D", "", str(r.get("mobile") or ""))
                    if p1:
                        phone_cands.append((int(r["id"]), p1))
                    if p2:
                        phone_cands.append((int(r["id"]), p2))
                healed_partner_id, healed_label, healed_conf = _best_fuzzy_match(
                    p_digits,
                    phone_cands,
                    min_score=max(SELF_HEAL_MIN_SCORE, PHONE_HEAL_MIN_SCORE),
                )

        if healed_partner_id is not None and _self_heal_allowed(
            "contact_lookup",
            healed_conf,
            "contact fuzzy fallback",
        ):
            partner = odoo.read("res.partner", [healed_partner_id], ["id", "name", "phone", "email"])[0]
            partner["created"] = False
            partner["reused"] = True
            ctx.record_self_heal("contact_lookup", f"phone={phone}, email={email}", healed_label, healed_conf, "contact fuzzy fallback")
        else:
            vals = {"name": email or phone or "Unknown"}
            if phone:
                vals["phone"] = phone
            if email:
                vals["email"] = email
            pid = odoo.create("res.partner", vals)
            partner = odoo.read("res.partner", [pid], ["id", "name", "phone", "email"])[0]
            partner["created"] = True
            partner["reused"] = False

    ctx.set_alias("contact", partner)
    return partner

@register("contact.update")
def op_contact_update(inp, ctx, odoo):
    raw_contact_id = inp.get("contact_id")
    contact_id: Optional[int] = None
    try:
        if raw_contact_id is not None and str(raw_contact_id).strip() != "":
            contact_id = int(raw_contact_id)
    except Exception:
        contact_id = None

    if not contact_id:
        for alias_name in ("contact", "company"):
            alias_contact = ctx.aliases.get(alias_name)
            if isinstance(alias_contact, dict) and alias_contact.get("id"):
                try:
                    contact_id = int(alias_contact.get("id"))
                    if SELF_HEALING_ENABLED:
                        ctx.record_self_heal(
                            "contact_id",
                            raw_contact_id,
                            contact_id,
                            0.82,
                            f"contact.update: fallback to ctx.{alias_name}.id",
                        )
                    break
                except Exception:
                    contact_id = None

    if not contact_id:
        return soft_fail("contact.update: contact_id is missing or invalid")

    tags = inp.get("tags", [])
    tag_ids = [ensure_partner_category(odoo, t) for t in tags]
    before = odoo.read("res.partner", [contact_id], ["id", "category_id"])
    old_tag_ids = []
    if before:
        old_tag_ids = [int(x) for x in (before[0].get("category_id") or [])]
    odoo.write("res.partner", [contact_id], {"category_id": [(6, 0, tag_ids)]})
    ctx.add_rollback_action(
        "write_restore",
        "res.partner",
        contact_id,
        {"category_id": [[6, 0, old_tag_ids]]},
        details="restore contact tags",
    )
    return {"updated": True, "contact_id": contact_id}

@register("deal.create")
def op_deal_create(inp, ctx, odoo):
    title = inp["title"]
    budget = float(inp.get("budget") or 0)
    tags = inp.get("tags", [])
    force_create = bool(inp.get("force_create"))
    use_existing = bool(inp.get("use_existing"))

    if force_create and use_existing:
        return soft_fail("deal.create: force_create and use_existing cannot both be true")

    tag_ids = [ensure_crm_tag(odoo, t) for t in tags]
    existing = odoo.search("crm.lead", [("type", "=", "opportunity"), ("name", "=", title)], limit=1)
    if existing and not force_create:
        lead_id = int(existing[0])
        if use_existing or DEAL_CREATE_REUSE_EXISTING:
            if tag_ids:
                lead_row = odoo.read("crm.lead", [lead_id], ["id", "tag_ids"])[0]
                existing_tags = set(lead_row.get("tag_ids") or [])
                merged = sorted(existing_tags | set(tag_ids))
                odoo.write("crm.lead", [lead_id], {"tag_ids": [(6, 0, merged)]})
                if merged != sorted(existing_tags):
                    ctx.add_rollback_action(
                        "write_restore",
                        "crm.lead",
                        lead_id,
                        {"tag_ids": [[6, 0, sorted(existing_tags)]]},
                        details="restore tags after deal.create reuse",
                    )
            if SELF_HEALING_ENABLED:
                ctx.record_self_heal("deal_create", title, f"id:{lead_id}", 1.0, "reused existing deal by exact title")
            ctx.set_alias("deals", [lead_id])
            lead = odoo.read("crm.lead", [lead_id], ["id", "name", "expected_revenue", "partner_id", "user_id", "stage_id"])[0]
            lead["deal_id"] = lead_id
            lead["created"] = False
            lead["reused"] = True
            return lead
        return soft_fail(
            f"deal.create: duplicate title exists: '{title}' (id={lead_id}); "
            "set use_existing=true to reuse or force_create=true to create a duplicate"
        )

    vals = {"name": title, "type": "opportunity", "expected_revenue": budget}
    if tag_ids:
        vals["tag_ids"] = [(6, 0, tag_ids)]

    contact = ctx.aliases.get("contact")
    if isinstance(contact, dict) and contact.get("id"):
        vals["partner_id"] = contact["id"]

    lead_id = odoo.create("crm.lead", vals)
    ctx.set_alias("deals", [lead_id])
    lead = odoo.read("crm.lead", [lead_id], ["id", "name", "expected_revenue", "partner_id", "user_id", "stage_id"])[0]
    lead["deal_id"] = lead_id
    lead["created"] = True
    lead["reused"] = False
    return lead

@register("deal.add_tags")
def op_deal_add_tags(inp, ctx, odoo):
    deals, fallback_block = _resolve_deals_from_input_or_alias(inp.get("deals"), ctx, "deal.add_tags")
    if fallback_block is not None:
        return fallback_block
    if not deals:
        return soft_fail("deal.add_tags: deals is empty")
    tags = inp.get("tags", [])
    tag_ids = [ensure_crm_tag(odoo, t) for t in tags]

    leads = odoo.read("crm.lead", deals, ["id", "tag_ids"])
    for lead in leads:
        existing = set(lead.get("tag_ids", []))
        merged = sorted(existing | set(tag_ids))
        odoo.write("crm.lead", [lead["id"]], {"tag_ids": [(6, 0, merged)]})
        if merged != sorted(existing):
            ctx.add_rollback_action(
                "write_restore",
                "crm.lead",
                int(lead["id"]),
                {"tag_ids": [[6, 0, sorted(existing)]]},
                details="restore tags after deal.add_tags",
            )

    return {"updated": True, "count": len(deals)}

@register("deal.update_stage")
def op_deal_update_stage(inp, ctx, odoo):
    deal_spec = inp["deal"]
    stage_name = inp["stage"]
    probability = inp.get("probability")

    deal_id = resolve_deal_id_from_spec(odoo, deal_spec, ctx, "deal.update_stage")

    if not deal_id:
        suggestions = suggest_similar_deal_titles(odoo, _deal_title_hint(deal_spec))
        msg = f"deal.update_stage: deal not found ({deal_spec})"
        if suggestions:
            msg += f"; suggestions={suggestions}"
        return soft_fail(msg)

    stage_id = find_stage_id(odoo, stage_name, ctx=ctx)
    if not stage_id:
        return soft_fail(f"deal.update_stage: stage not found: {stage_name}")

    vals = {"stage_id": stage_id}
    if probability is not None:
        vals["probability"] = float(probability)

    before_rows = odoo.read("crm.lead", [deal_id], ["id", "stage_id", "probability"])
    restore_vals: Dict[str, Any] = {}
    if before_rows:
        before = before_rows[0]
        old_stage = before.get("stage_id")
        restore_vals["stage_id"] = int(old_stage[0]) if isinstance(old_stage, list) and old_stage else False
        if "probability" in before:
            restore_vals["probability"] = before.get("probability")

    odoo.write("crm.lead", [deal_id], vals)
    if restore_vals:
        ctx.add_rollback_action(
            "write_restore",
            "crm.lead",
            int(deal_id),
            restore_vals,
            details="restore stage/probability after deal.update_stage",
        )
    return {"updated": True, "deal_id": deal_id, "stage": stage_name}

@register("deal.mark_lost")
def op_deal_mark_lost(inp, ctx, odoo):
    deal_spec = inp["deal"]
    reason = inp.get("reason", "")

    deal_id = resolve_deal_id_from_spec(odoo, deal_spec, ctx, "deal.mark_lost")

    if not deal_id:
        suggestions = suggest_similar_deal_titles(odoo, _deal_title_hint(deal_spec))
        msg = f"deal.mark_lost: deal not found ({deal_spec})"
        if suggestions:
            msg += f"; suggestions={suggestions}"
        return soft_fail(msg)

    fields = ["id", "probability", "stage_id", "active"]
    if model_has_field(odoo, "crm.lead", "lost_reason_id"):
        fields.append("lost_reason_id")
    before_rows = odoo.read("crm.lead", [deal_id], fields)
    restore_vals: Dict[str, Any] = {}
    if before_rows:
        before = before_rows[0]
        old_stage = before.get("stage_id")
        restore_vals["stage_id"] = int(old_stage[0]) if isinstance(old_stage, list) and old_stage else False
        if "probability" in before:
            restore_vals["probability"] = before.get("probability")
        if "active" in before:
            restore_vals["active"] = bool(before.get("active"))
        if "lost_reason_id" in before:
            old_reason = before.get("lost_reason_id")
            restore_vals["lost_reason_id"] = int(old_reason[0]) if isinstance(old_reason, list) and old_reason else False

    try:
        odoo.execute("crm.lead", "action_set_lost", [deal_id])
    except Exception:
        odoo.write("crm.lead", [deal_id], {"probability": 0})

    if reason:
        rr = odoo.search("crm.lost.reason", [("name", "=", reason)], limit=1)
        rid = rr[0] if rr else odoo.create("crm.lost.reason", {"name": reason})
        if model_has_field(odoo, "crm.lead", "lost_reason_id"):
            odoo.write("crm.lead", [deal_id], {"lost_reason_id": rid})

    if restore_vals:
        ctx.add_rollback_action(
            "write_restore",
            "crm.lead",
            int(deal_id),
            restore_vals,
            details="restore fields after deal.mark_lost",
        )
    return {"updated": True, "deal_id": deal_id, "lost": True}

@register("deal.search")
def op_deal_search(inp, ctx, odoo):
    title = str(inp.get("title") or "").strip()
    min_budget = inp.get("min_budget")

    if title:
        matched_id = find_deal_id_by_title(odoo, title, ctx=ctx)
        if not matched_id:
            return soft_fail(f"deal.search: no sufficiently similar deal found for title='{title}'")
        ids = [int(matched_id)]
        ctx.set_alias("deals", ids)
        return {"deals": ids, "deal_id": ids[0], "count": 1, "query": {"title": title}}

    domain = [("type", "=", "opportunity")]
    if min_budget is not None:
        try:
            domain.append(("expected_revenue", ">=", float(min_budget)))
        except Exception:
            return soft_fail(f"deal.search: invalid min_budget={min_budget}")

    ids = odoo.search("crm.lead", domain, limit=50)
    ctx.set_alias("deals", ids)
    if not ids:
        log.info("deal.search returned empty; cleared ctx.deals")

    return {"deals": ids, "deal_id": ids[0] if len(ids) == 1 else None, "count": len(ids), "query": {"min_budget": min_budget}}

@register("deal.search_stale")
def op_deal_search_stale(inp, ctx, odoo):
    days_inactive = int(inp["days_inactive"])
    stages = inp.get("stages", [])

    stage_ids = []
    for s in stages:
        sid = find_stage_id(odoo, s, ctx=ctx)
        if sid:
            stage_ids.append(sid)

    cutoff = (dt.datetime.now() - dt.timedelta(days=days_inactive)).strftime("%Y-%m-%d %H:%M:%S")
    domain = [("type", "=", "opportunity"), ("write_date", "<", cutoff)]
    if stage_ids:
        domain.append(("stage_id", "in", stage_ids))

    ids = odoo.search("crm.lead", domain, limit=50)
    ctx.set_alias("deals", ids)
    if not ids:
        log.info("deal.search_stale returned empty; cleared ctx.deals")

    return {
        "deals": ids,
        "deal_id": ids[0] if len(ids) == 1 else None,
        "count": len(ids),
        "query": {"days_inactive": days_inactive, "stages": stages},
    }

@register("deal.update")
def op_deal_update(inp, ctx, odoo):
    deals, fallback_block = _resolve_deals_from_input_or_alias(inp.get("deals"), ctx, "deal.update")
    if fallback_block is not None:
        return fallback_block
    salesperson = inp.get("salesperson")

    if not deals:
        return soft_fail("deal.update: deals is empty")

    if salesperson:
        before_rows = odoo.read("crm.lead", deals, ["id", "user_id"])
        uid = find_user_by_name_or_login(odoo, salesperson, ctx=ctx)
        if not uid:
            if (
                FALLBACK_TO_CURRENT_USER
                and getattr(odoo, "uid", None)
                and _self_heal_allowed("salesperson", 0.6, "fallback to current authenticated user")
            ):
                uid = int(odoo.uid)
                ctx.record_self_heal(
                    "salesperson",
                    salesperson,
                    f"uid:{uid}",
                    0.6,
                    "fallback to current authenticated user",
                )
            else:
                suggestions = suggest_similar_users(odoo, str(salesperson))
                msg = f"deal.update: salesperson not found: {salesperson}"
                if suggestions:
                    msg += f"; suggestions={suggestions}"
                return soft_fail(msg)
        odoo.write("crm.lead", deals, {"user_id": uid})
        for row in before_rows:
            old_user = row.get("user_id")
            old_uid = int(old_user[0]) if isinstance(old_user, list) and old_user else False
            ctx.add_rollback_action(
                "write_restore",
                "crm.lead",
                int(row["id"]),
                {"user_id": old_uid},
                details="restore owner after deal.update",
            )

    return {"updated": True, "count": len(deals)}

@register("activity.create")
def op_activity_create(inp, ctx, odoo):
    deals = _normalize_ids(inp.get("deals"))
    deal_spec = inp.get("deal")

    if not deals and deal_spec:
        did = resolve_deal_id_from_spec(odoo, deal_spec, ctx, "activity.create")
        if did:
            deals = [did]
        else:
            msg = f"activity.create: deal not found ({deal_spec})"
            suggestions = suggest_similar_deal_titles(odoo, _deal_title_hint(deal_spec))
            if suggestions:
                msg += f"; suggestions={suggestions}"
            return soft_fail(msg)

    if not deals:
        deals, fallback_block = _resolve_deals_from_input_or_alias(inp.get("deals"), ctx, "activity.create")
        if fallback_block is not None:
            return fallback_block

    if not deals:
        return soft_fail("activity.create: no deals")

    act_type = inp.get("type", "To Do")
    summary = inp.get("summary", "") or ""
    due = inp.get("due")

    type_ids = odoo.search("mail.activity.type", [("name", "ilike", act_type)], limit=1)
    if not type_ids:
        type_ids = odoo.search("mail.activity.type", [("name", "ilike", "To Do")], limit=1)
    if not type_ids:
        if AUTO_CREATE_ACTIVITY_TYPE and SELF_HEALING_ENABLED:
            create_name = str(act_type or "To Do")
            vals: Dict[str, Any] = {"name": create_name}
            if model_has_field(odoo, "mail.activity.type", "category"):
                vals["category"] = "default"
            try:
                created_tid = odoo.create("mail.activity.type", vals)
                type_ids = [int(created_tid)]
                if SELF_HEALING_ENABLED:
                    ctx.record_self_heal(
                        "activity_type",
                        act_type,
                        create_name,
                        0.72,
                        "auto-created missing mail.activity.type",
                    )
            except Exception:
                try:
                    created_tid = odoo.create("mail.activity.type", {"name": "To Do"})
                    type_ids = [int(created_tid)]
                    if SELF_HEALING_ENABLED:
                        ctx.record_self_heal(
                            "activity_type",
                            act_type,
                            "To Do",
                            0.7,
                            "auto-created fallback activity type",
                        )
                except Exception:
                    pass
    if not type_ids:
        return soft_fail("activity.create: mail.activity.type not found")

    activity_type_id = type_ids[0]
    crm_lead_model_id = get_model_id(odoo, "crm.lead")

    created = []
    for lead_id in deals:
        if not lead_id:
            continue
        vals = {
            "activity_type_id": activity_type_id,
            "summary": summary,
            "res_model_id": crm_lead_model_id,
            "res_id": int(lead_id),
        }
        if due:
            vals["date_deadline"] = parse_relative_datetime(str(due)).date().isoformat()
        act_id = odoo.create("mail.activity", vals)
        created.append(act_id)

    return {"created": len(created), "activity_ids": created}

@register("meeting.schedule")
def op_meeting_schedule(inp, ctx, odoo):

    deal_spec = inp.get("deal", {})
    when = inp.get("when") or ""

    deal_id = resolve_deal_id_from_spec(odoo, deal_spec, ctx, "meeting.schedule")

    if not deal_id:
        suggestions = suggest_similar_deal_titles(odoo, _deal_title_hint(deal_spec))
        msg = f"meeting.schedule: deal not found ({deal_spec})"
        if suggestions:
            msg += f"; suggestions={suggestions}"
        return soft_fail(msg)

    # Парсим пользовательское время в настроенной входной таймзоне, затем сохраняем UTC в Odoo.
    start_local = parse_relative_datetime(str(when))
    input_tz = RPA_INPUT_TIMEZONE or "Europe/Moscow"
    start = to_utc_naive(start_local, input_tz)
    stop = start + dt.timedelta(minutes=30)

    partner_ids = []
    contact = ctx.aliases.get("contact")
    if isinstance(contact, dict) and contact.get("id"):
        partner_ids.append(int(contact["id"]))

    organizer_uid = int(getattr(odoo, "uid", 0) or 0)
    organizer_partner_id: Optional[int] = None
    if organizer_uid:
        try:
            user_rows = odoo.read("res.users", [organizer_uid], ["partner_id"])
            if user_rows:
                raw_partner = user_rows[0].get("partner_id")
                if isinstance(raw_partner, list) and raw_partner:
                    organizer_partner_id = int(raw_partner[0])
                elif raw_partner:
                    organizer_partner_id = int(raw_partner)
        except Exception:
            organizer_partner_id = None
    if organizer_partner_id:
        partner_ids.append(int(organizer_partner_id))
    partner_ids = sorted(set(int(x) for x in partner_ids if int(x) > 0))

    vals = {
        "name": f"Meeting for deal {deal_id}",
        "start": fmt_odoo_dt(start),
        "stop": fmt_odoo_dt(stop),
    }
    # Привязываем встречу к CRM-сделке, чтобы она отображалась в шапке сделки.
    if model_has_field(odoo, "calendar.event", "opportunity_id"):
        vals["opportunity_id"] = int(deal_id)
    if model_has_field(odoo, "calendar.event", "res_model"):
        vals["res_model"] = "crm.lead"
    if model_has_field(odoo, "calendar.event", "res_id"):
        vals["res_id"] = int(deal_id)
    if model_has_field(odoo, "calendar.event", "res_model_id"):
        try:
            vals["res_model_id"] = int(get_model_id(odoo, "crm.lead"))
        except Exception:
            pass
    if organizer_uid and model_has_field(odoo, "calendar.event", "user_id"):
        vals["user_id"] = organizer_uid
    if partner_ids and model_has_field(odoo, "calendar.event", "partner_ids"):
        vals["partner_ids"] = [(6, 0, partner_ids)]

    try:
        event_id = odoo.create("calendar.event", vals)
        return {"created": True, "event_id": event_id, "start": vals["start"], "stop": vals["stop"]}
    except Exception as e:
        return soft_fail(f"meeting.schedule failed: {e}")

@register("deal.create_quotation")
def op_deal_create_quotation(inp, ctx, odoo):

    deal_spec = inp.get("deal", {})
    deal_id = resolve_deal_id_from_spec(odoo, deal_spec, ctx, "deal.create_quotation")

    if not deal_id:
        suggestions = suggest_similar_deal_titles(odoo, _deal_title_hint(deal_spec))
        msg = f"deal.create_quotation: deal not found ({deal_spec})"
        if suggestions:
            msg += f"; suggestions={suggestions}"
        return soft_fail(msg)

    lead = odoo.read("crm.lead", [deal_id], ["id", "name", "partner_id", "expected_revenue"])[0]
    partner_id = None
    if lead.get("partner_id"):
        if isinstance(lead["partner_id"], list):
            partner_id = lead["partner_id"][0]
        else:
            partner_id = lead["partner_id"]

    if not partner_id:
        contact = ctx.aliases.get("contact")
        if isinstance(contact, dict) and contact.get("id"):
            partner_id = int(contact["id"])

    if not model_exists(odoo, "sale.order"):
        lines = [
            "Sales module not installed in this DB (model sale.order missing).",
            f"Deal ID: {deal_id}",
            f"Deal title: {lead.get('name','')}",
            f"Partner ID: {partner_id}",
        ]
        out_name = f"quotation_stub_no_sales_{deal_id}_{utcnow_stamp()}.pdf"
        pdf_path = try_make_pdf_simple("Quotation (no Sales module)", lines, out_name)
        if not pdf_path:
            txt_name = f"quotation_stub_no_sales_{deal_id}_{utcnow_stamp()}.txt"
            pdf_path = write_text_file(txt_name, "\n".join(lines))

        quotation = {"sale_order_id": None, "pdf_path": pdf_path, "note": "Sales module missing in DB"}
        ctx.set_alias("quotation", quotation)
        return quotation

    if not partner_id:
        return soft_fail("deal.create_quotation: no partner/contact to attach")

    vals = {
        "partner_id": int(partner_id),
        "origin": f"CRM:{deal_id} {lead.get('name','')}",
    }
    if model_has_field(odoo, "sale.order", "opportunity_id"):
        vals["opportunity_id"] = int(deal_id)

    so_id = odoo.create("sale.order", vals)

    # Проверяем, что сумма коммерческого предложения не нулевая: создаем строку на основе бюджета сделки.
    if model_exists(odoo, "sale.order.line") and model_exists(odoo, "product.product"):
        product_ids = odoo.search("product.product", [("sale_ok", "=", True)], limit=1)
        if not product_ids and model_exists(odoo, "product.template"):
            tmpl_vals: Dict[str, Any] = {
                "name": "RPA Service",
                "sale_ok": True,
                "purchase_ok": False,
                "list_price": 1.0,
            }
            if model_has_field(odoo, "product.template", "detailed_type"):
                tmpl_vals["detailed_type"] = "service"
            elif model_has_field(odoo, "product.template", "type"):
                tmpl_vals["type"] = "service"
            try:
                tmpl_id = odoo.create("product.template", tmpl_vals)
                product_ids = odoo.search("product.product", [("product_tmpl_id", "=", int(tmpl_id))], limit=1)
            except Exception:
                product_ids = []

        if product_ids:
            raw_amount = inp.get("amount")
            if raw_amount is not None:
                try:
                    amount = float(raw_amount)
                except Exception:
                    amount = float(lead.get("expected_revenue") or 0.0)
            else:
                amount = float(lead.get("expected_revenue") or 0.0)
            if amount <= 0:
                amount = 1.0
            line_vals: Dict[str, Any] = {
                "order_id": int(so_id),
                "product_id": int(product_ids[0]),
                "product_uom_qty": 1.0,
                "price_unit": float(amount),
            }
            if model_has_field(odoo, "sale.order.line", "name"):
                line_vals["name"] = f"CRM Deal {deal_id}: {lead.get('name','')}"
            try:
                odoo.create("sale.order.line", line_vals)
            except Exception:
                # Оставляем создание коммерческого предложения успешным, даже если создание строки не удалось.
                pass

    so = odoo.read("sale.order", [so_id], ["id", "name", "partner_id", "amount_total", "state"])[0]
    lines = [
        f"Sale Order ID: {so.get('id')}",
        f"Name: {so.get('name')}",
        f"Partner: {so.get('partner_id')}",
        f"State: {so.get('state')}",
        f"Amount Total: {so.get('amount_total')}",
        f"Source Deal ID: {deal_id}",
    ]
    pdf_name = f"quotation_{so_id}_{utcnow_stamp()}.pdf"
    pdf_path = try_make_pdf_simple("Quotation (stub)", lines, pdf_name)
    if not pdf_path:
        txt_name = f"quotation_{so_id}_{utcnow_stamp()}.txt"
        pdf_path = write_text_file(txt_name, "\n".join(lines))

    quotation = {"sale_order_id": so_id, "pdf_path": pdf_path}
    ctx.set_alias("quotation", quotation)
    return quotation

@register("notify.email")
def op_notify_email(inp, ctx, odoo):
    to = inp.get("to")
    subject = inp.get("subject") or "(no subject)"
    attach = inp.get("attach")

    eml = []
    eml.append(f"To: {to}")
    eml.append(f"Subject: {subject}")
    eml.append("")
    eml.append("This is a local stub email created by your RPA.")
    if attach:
        attach_display = str(attach)
        try:
            attach_path = Path(attach_display)
            if attach_path.is_absolute():
                attach_display = os.path.relpath(str(attach_path), str(BASE_DIR))
        except Exception:
            pass
        eml.append(f"Attachment path: {attach_display}")
    eml_text = "\n".join(eml)

    eml_name = f"email_{utcnow_stamp()}.eml"
    eml_path = write_text_file(eml_name, eml_text)

    result = {"saved_eml": eml_path}

    if TRY_SEND_ODOO_EMAIL:
        try:
            mail_vals = {
                "subject": subject,
                "body_html": "<p>This message was generated by RPA.</p>",
                "email_to": to,
            }
            mail_id = odoo.create("mail.mail", mail_vals)
            odoo.execute("mail.mail", "send", [mail_id])
            result["odoo_mail_id"] = mail_id
            result["sent"] = True
        except Exception as e:
            result["sent"] = False
            result["send_error"] = str(e)

    return result

def _period_bounds(period: str):
    p = (period or "").strip().lower()
    today = dt.datetime.utcnow().date()
    if p == "yesterday":
        d0 = today - dt.timedelta(days=1)
        d1 = today
        return d0, d1
    if p == "today":
        return today, today + dt.timedelta(days=1)
    if p in {"week", "this week"}:
        # Скользящее окно 7 дней, включая сегодня.
        d0 = today - dt.timedelta(days=6)
        d1 = today + dt.timedelta(days=1)
        return d0, d1
    if p in {"month", "this month"}:
        # Скользящее окно 30 дней, включая сегодня.
        d0 = today - dt.timedelta(days=29)
        d1 = today + dt.timedelta(days=1)
        return d0, d1
    d0 = today - dt.timedelta(days=1)
    d1 = today
    return d0, d1

@register("report.sales_daily")
def op_report_sales_daily(inp, ctx, odoo):
    period = inp.get("period", "yesterday")
    group_by = inp.get("group_by", "salesperson")

    if not model_exists(odoo, "sale.order"):
        rows = []
        csv_name = f"sales_report_{period}_{utcnow_stamp()}.csv"
        csv_path = write_csv_file(csv_name, rows, ["salesperson", "orders", "amount_total"])
        report_obj = {"period": period, "csv_path": csv_path, "rows": rows, "note": "Sales module missing in DB"}
        ctx.set_alias("report", report_obj)
        return report_obj

    d0, d1 = _period_bounds(str(period))
    start = dt.datetime.combine(d0, dt.time.min).strftime("%Y-%m-%d %H:%M:%S")
    end = dt.datetime.combine(d1, dt.time.min).strftime("%Y-%m-%d %H:%M:%S")

    domain = [("date_order", ">=", start), ("date_order", "<", end)]
    so_ids = odoo.search("sale.order", domain, limit=200)

    fields = ["id", "name", "user_id", "amount_total", "date_order", "state"]
    orders = odoo.read("sale.order", so_ids, fields) if so_ids else []

    agg: Dict[str, Dict[str, Any]] = {}
    for o in orders:
        u = o.get("user_id")
        key = "Unknown"
        if isinstance(u, list) and u:
            key = u[1]
        elif u:
            key = str(u)

        a = agg.setdefault(key, {"salesperson": key, "orders": 0, "amount_total": 0.0})
        a["orders"] += 1
        a["amount_total"] += float(o.get("amount_total") or 0)

    rows = sorted(agg.values(), key=lambda x: x["amount_total"], reverse=True)
    csv_name = f"sales_report_{period}_{utcnow_stamp()}.csv"
    csv_path = write_csv_file(csv_name, rows, ["salesperson", "orders", "amount_total"])

    report_obj = {"period": period, "csv_path": csv_path, "rows": rows}
    ctx.set_alias("report", report_obj)
    return report_obj

@register("report.export")
def op_report_export(inp, ctx, odoo):
    fmt = (inp.get("format") or "pdf").lower()
    report = ctx.aliases.get("report")

    if not isinstance(report, dict):
        return soft_fail("report.export: no report data (run report.sales_daily first)")

    if fmt != "pdf":
        csv_path = report.get("csv_path")
        if not csv_path:
            csv_name = f"sales_report_{report.get('period','unknown')}_{utcnow_stamp()}.csv"
            csv_path = write_csv_file(csv_name, [], ["salesperson", "orders", "amount_total"])
        file_obj = {"path": csv_path, "format": "csv"}
        ctx.set_alias("file", file_obj)
        return file_obj

    lines = [f"Period: {report.get('period')}"]
    lines.append("")
    rows = report.get("rows") or []
    if rows:
        for r in rows:
            lines.append(f"{r['salesperson']}: orders={r['orders']} total={r['amount_total']:.2f}")
    else:
        lines.append("No sales rows for selected period.")

    pdf_name = f"sales_report_{report.get('period')}_{utcnow_stamp()}.pdf"
    pdf_path = try_make_pdf_simple("Sales report", lines, pdf_name)
    if not pdf_path:
        txt_name = f"sales_report_{report.get('period')}_{utcnow_stamp()}.txt"
        pdf_path = write_text_file(txt_name, "\n".join(lines))

    file_obj = {"path": pdf_path, "format": "pdf"}
    ctx.set_alias("file", file_obj)
    return file_obj

@register("watchdog")
def op_watchdog(inp, ctx, odoo):
    deal_spec = inp.get("deal", {})
    condition = inp.get("condition", "")

    deal_id = resolve_deal_id_from_spec(odoo, deal_spec, ctx, "watchdog")

    if not deal_id:
        suggestions = suggest_similar_deal_titles(odoo, _deal_title_hint(deal_spec))
        msg = f"watchdog: deal not found ({deal_spec})"
        if suggestions:
            msg += f"; suggestions={suggestions}"
        return soft_fail(msg)

    lead = odoo.read("crm.lead", [deal_id], ["id", "name", "probability", "stage_id"])[0]
    is_won = False

    prob = float(lead.get("probability") or 0)
    if prob >= 100:
        is_won = True
    st = lead.get("stage_id")
    if isinstance(st, list) and len(st) >= 2 and "won" in str(st[1]).lower():
        is_won = True

    return {"deal_id": deal_id, "condition": condition, "won_detected": is_won, "probability": prob, "stage": st}

# -------------------- Исполнитель сценариев --------------------

def run_scenario(scenario: Dict[str, Any], odoo: OdooClient) -> ExecutionContext:
    ctx = ExecutionContext(vars=copy.deepcopy(scenario.get("vars", {})))
    for idx, step in enumerate(scenario.get("steps", [])):
        if not isinstance(step, dict):
            step_id = f"step_{idx + 1}"
            op_name = ""
            raw_input = {}
            ctx.current_step_id = step_id
            ctx.current_op = op_name
            out = soft_fail(f"Invalid step format at index {idx}: expected mapping")
            ctx.set_step_result(step_id, out)
            ctx.add_step_trace(
                {
                    "step_id": step_id,
                    "op": op_name,
                    "status": "error",
                    "duration_ms": 0,
                    "self_heal_triggered": False,
                    "self_heal_events_added": 0,
                    "error": str(out.get("reason") if isinstance(out, dict) else out),
                }
            )
            if ALERT_ON_STEP_FAILURE:
                ctx.add_alert(
                    message=f"Step {step_id} has invalid format and was skipped",
                    severity="error",
                    details=str(out),
                )
            continue

        step_id = str(step.get("id") or f"step_{idx + 1}")
        op_name = str(step.get("op") or "")
        raw_input = step.get("input", {}) if isinstance(step.get("input"), dict) else {}

        inp = resolve_value(raw_input, ctx)

        ctx.current_step_id = step_id
        ctx.current_op = op_name
        started = time.time()
        heal_before = len(ctx.self_heal_events)

        log.info("STEP %-18s %s", step_id, op_name)
        fn = OPS.get(op_name)
        status = "success"
        out: Any

        if not fn:
            out = soft_fail(f"Unknown op: {op_name}")
            status = "skipped"
        else:
            try:
                out = fn(inp, ctx, odoo)
            except Exception as e:
                if STRICT:
                    raise
                status = "error"
                out = {"error": str(e), "exception": True}
                log.exception("STEP FAILED %s %s: %s", step_id, op_name, e)

        if isinstance(out, dict) and out.get("skipped"):
            status = "skipped"

        ctx.set_step_result(step_id, out)
        log.info(" -> %s", str(out)[:200])

        heal_after = len(ctx.self_heal_events)
        duration_ms = int((time.time() - started) * 1000)
        ctx.add_step_trace(
            {
                "step_id": step_id,
                "op": op_name,
                "status": status,
                "duration_ms": duration_ms,
                "self_heal_triggered": heal_after > heal_before,
                "self_heal_events_added": max(0, heal_after - heal_before),
                "error": (out.get("reason") if isinstance(out, dict) and out.get("skipped") else out.get("error")) if isinstance(out, dict) else "",
            }
        )
        if ALERT_ON_STEP_FAILURE and status != "success":
            err_text = ""
            if isinstance(out, dict):
                err_text = str(out.get("reason") or out.get("error") or out)
            else:
                err_text = str(out)
            if (heal_after > heal_before):
                ctx.add_alert(
                    message=f"Step {step_id} failed after self-healing attempt",
                    severity="error",
                    details=err_text,
                )
            else:
                ctx.add_alert(
                    message=f"Step {step_id} failed without self-healing",
                    severity="warning",
                    details=err_text,
                )

        if step_id == "c2_find_or_create":
            ctx.set_alias("contact", out)

    dump = {
        "scenario_id": scenario.get("id", "scenario"),
        "scenario_flow": scenario.get("flow", ""),
        "scenario": copy.deepcopy(scenario),
        "log_dir": LOG_DIR,
        "artifact_dir": ARTIFACT_DIR,
        "vars": ctx.vars,
        "aliases": ctx.aliases,
        "steps": ctx.steps,
        "step_traces": ctx.step_traces,
        "self_heal_events": ctx.self_heal_events,
        "rollback_actions": ctx.rollback_actions,
        "alerts": ctx.alerts,
    }
    write_json_file(f"run_context_{scenario.get('id','scenario')}_{utcnow_stamp()}.json", dump)
    return ctx


def summarize_execution(ctx: ExecutionContext) -> Dict[str, Any]:
    total = len(ctx.step_traces)
    success = sum(1 for t in ctx.step_traces if t.get("status") == "success")
    skipped = sum(1 for t in ctx.step_traces if t.get("status") == "skipped")
    errors = sum(1 for t in ctx.step_traces if t.get("status") == "error")
    heal_steps = sum(1 for t in ctx.step_traces if t.get("self_heal_triggered"))
    heal_success_steps = sum(
        1 for t in ctx.step_traces if t.get("self_heal_triggered") and t.get("status") == "success"
    )
    heal_failed_steps = sum(
        1 for t in ctx.step_traces if t.get("self_heal_triggered") and t.get("status") != "success"
    )
    return {
        "steps_total": total,
        "steps_success": success,
        "steps_skipped": skipped,
        "steps_error": errors,
        "self_heal_events": len(ctx.self_heal_events),
        "self_heal_steps": heal_steps,
        "self_heal_success_steps": heal_success_steps,
        "self_heal_failed_steps": heal_failed_steps,
        "alerts_total": len(ctx.alerts),
        "scenario_success": (total > 0 and success == total),
    }

# -------------------- сценарии --------------------

SCENARIOS_DATA: List[Dict[str, Any]] = [
    {
        "id": "ALL_OPS_FULL",
        "flow": "odoo_crm_all_ops_full_coverage",
        "vars": {
            "phone": "+7 900 000-00-00",
            "email": "ivan.petrov@example.com",

            "budget": 250000,
            "budget_big": 1500000,

            # Эти title мы САМИ создаём в сценарии, чтобы by_title всегда находил сделки
            "deal_title_for_stage": "5 VP Chairs",
            "deal_title_for_quote": "Office Design and Architecture",
            "deal_title_won": "Distributor Contract",
            "deal_title_lost": "Quote for 12 Tables",
            "deal_title_big": "Big Enterprise Deal",

            # Поиск “дорогих” сделок
            "min_budget": 1000000,

            # stale-поиск: 0 дней, чтобы гарантированно что-то нашлось (по write_date)
            "days_inactive": 0,

            # Стадии могут отличаться по языку, попробуем обе
            "stages": ["New", "Новая"],
            "stage_try_1": "New",
            "stage_try_2": "Новая",

            "due_call": "tomorrow 12:00",
            "due_meeting": "tomorrow 11:00",
            "due_urgent": "today 16:00",

            "period": "today",
            "email_to": "director@example.com",

            # ВАЖНО: укажи логин (email) реально существующего пользователя Odoo.
            # Самый безопасный вариант - твой ODOO_EMAIL из CONFIG.
            "to_salesperson_login": ODOO_EMAIL
        },
        "steps": [
            # --- Контакты ---
            {"id": "c1_create", "op": "contact.create", "input": {"name": "ООО Ромашка"}},
            {"id": "c2_find_or_create", "op": "contact.find_or_create", "input": {"phone": "${phone}", "email": "${email}"}},
            {"id": "c3_update", "op": "contact.update", "input": {"contact_id": "${contact.id}", "tags": ["VIP", "b2b"]}},

            # --- Сделки: создаём все, которые дальше используются через by_title ---
            {"id": "d1_create_stage_deal", "op": "deal.create", "input": {"title": "${deal_title_for_stage}", "budget": "${budget}", "tags": ["demo"]}},
            {"id": "d2_add_tags_last", "op": "deal.add_tags", "input": {"deals": "${deals}", "tags": ["risk"]}},

            {"id": "d3_create_quote_deal", "op": "deal.create", "input": {"title": "${deal_title_for_quote}", "budget": 120000, "tags": ["quote"]}},
            {"id": "d4_create_won_deal", "op": "deal.create", "input": {"title": "${deal_title_won}", "budget": 500000, "tags": ["core"]}},
            {"id": "d5_create_lost_deal", "op": "deal.create", "input": {"title": "${deal_title_lost}", "budget": 60000, "tags": ["lost"]}},
            {"id": "d6_create_big_deal", "op": "deal.create", "input": {"title": "${deal_title_big}", "budget": "${budget_big}", "tags": ["enterprise"]}},

            # update_stage: пробуем две стадии (вторая может skip - это нормально).
            {"id":"st1_update_stage_1","op":"deal.update_stage","input":{"deal":{"id":"${d1_create_stage_deal.id}"},"stage":"${stage_try_1}","probability":60}},
            {"id": "st2_update_stage_2", "op": "deal.update_stage", "input": {"deal": {"by_title": "${deal_title_for_stage}"}, "stage": "${stage_try_2}", "probability": 60}},

            # --- mark_lost ---
            {"id": "l1_mark_lost", "op": "deal.mark_lost", "input": {"deal": {"by_title": "${deal_title_lost}"}, "reason": "Конкурент предложил меньшую цену"}},

            # --- Поиски ---
            {"id": "s1_search_min_budget", "op": "deal.search", "input": {"min_budget": "${min_budget}"}},
            # Чтобы дальше точно были deals для массовых операций/активностей
            {"id": "s1b_search_all", "op": "deal.search", "input": {"min_budget": 0}},
            {"id": "s2_search_stale", "op": "deal.search_stale", "input": {"days_inactive": "${days_inactive}", "stages": "${stages}"}},

            # --- Массовый апдейт owner ---
            {"id": "u1_update_owner", "op": "deal.update", "input": {"deals": "${deals}", "salesperson": "${to_salesperson_login}"}},

            # --- Активности ---
            {"id": "a1_activity_call", "op": "activity.create", "input": {"deals": "${deals}", "type": "Call", "summary": "Перезвонить клиенту", "due": "${due_call}"}},
            # Проверяем fallback типа (если такого типа нет, код должен переключиться на To Do)
            {"id":"a2_activity_note","op":"activity.create","input":{"deal":{"id":"${d1_create_stage_deal.id}"},"type":"email","summary":"Обсудить условия"}},

            # --- Встреча ---
            {"id":"m1_meeting","op":"meeting.schedule","input":{"deal":{"id":"${d4_create_won_deal.id}"},"when":"${due_meeting}"}},

            # --- Квотация + письмо ---
            {"id":"q1_quotation","op":"deal.create_quotation","input":{"deal":{"id":"${d3_create_quote_deal.id}"}}},
            {"id": "n1_notify_quote", "op": "notify.email", "input": {"to": "${email_to}", "subject": "Коммерческое предложение", "attach": "${quotation.pdf_path}"}},

            # --- Отчёты ---
            {"id": "r1_sales_report", "op": "report.sales_daily", "input": {"period": "${period}", "group_by": "salesperson"}},
            {"id": "r2_export_csv", "op": "report.export", "input": {"format": "csv"}},
            {"id": "n2_send_report_csv", "op": "notify.email", "input": {"to": "${email_to}", "subject": "Отчёт (CSV) за ${period}", "attach": "${file.path}"}},
            {"id": "r3_export_pdf", "op": "report.export", "input": {"format": "pdf"}},
            {"id": "n3_send_report_pdf", "op": "notify.email", "input": {"to": "${email_to}", "subject": "Отчёт (PDF) за ${period}", "attach": "${file.path}"}},

            # --- Watchdog: сделаем “won_detected=True” через probability=100 ---
            {"id":"w0_prob_100_1","op":"deal.update_stage","input":{"deal":{"id":"${d4_create_won_deal.id}"},"stage":"Won","probability":100}},
            {"id":"w1_watch","op":"watchdog","input":{"deal":{"id":"${d4_create_won_deal.id}"},"condition":"deal.status == 'won'"}},
            {"id": "w2_watch_by_title", "op": "watchdog", "input": {"deal": {"by_title": "${deal_title_won}"}, "condition": "deal.status == 'won'"}}
        ]
    }
]


def main():
    log.info("Using ODOO_URL=%s | LOG_DIR=%s", ODOO_URL, LOG_DIR)
    odoo = OdooClient(ODOO_URL, ODOO_DB, ODOO_EMAIL, ODOO_PASSWORD)

    for sc in SCENARIOS_DATA:
        log.info("=== RUN SCENARIO %s ===", sc.get("id"))
        try:
            run_scenario(sc, odoo)
        except Exception as e:
            if STRICT:
                raise
            log.exception("Scenario failed but STRICT=False: %s", e)

    log.info("DONE. See logs in %s", LOG_DIR)

if __name__ == "__main__":
    main()
