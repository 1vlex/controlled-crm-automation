from __future__ import annotations

import copy
import datetime as dt
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import llm
import odoo_rollback
import odoo_rpa
import self_healing_policy as heal_policy
from runtime_config import BASE_DIR, env_float, env_int, env_str


UI_DIR = BASE_DIR / "rpa_ui"
ROLLBACK_HIDDEN_STORE = BASE_DIR / ".cache" / "rollback_hidden_sources.json"


class PreviewRequest(BaseModel):
    nl_text: str = ""
    yaml_text: str = ""
    provider: str = Field(default_factory=lambda: env_str("LLM_PROVIDER", "groq"))
    model: str = Field(default_factory=lambda: env_str("LLM_MODEL", "llama-3.3-70b-versatile"))
    fallback_provider: str = Field(default_factory=lambda: env_str("LLM_FALLBACK_PROVIDER", ""))
    fallback_model: str = Field(default_factory=lambda: env_str("LLM_FALLBACK_MODEL", ""))
    retrieval_data: str = "retrieval_pool_no_leak_odoo_API.csv"
    retriever: str = Field(default_factory=lambda: env_str("LLM_RETRIEVER", "hybrid"))
    fewshot_k: int = Field(default_factory=lambda: env_int("LLM_FEWSHOT_K", 3))
    repair_attempts: int = Field(default_factory=lambda: env_int("LLM_REPAIR_ATTEMPTS", 2))
    temperature: float = Field(default_factory=lambda: env_float("LLM_TEMPERATURE", 0.0))
    max_tokens: int = Field(default_factory=lambda: env_int("LLM_MAX_TOKENS", 2200))


class ExecuteRequest(BaseModel):
    scenario: Dict[str, Any]
    confirmations: List[Dict[str, Any]] = []
    decisions: Dict[str, str] = {}


class RollbackPreviewRequest(BaseModel):
    paths: List[str] = []


class RollbackApplyRequest(BaseModel):
    actions: List[Dict[str, Any]] = []


app = FastAPI(title="Odoo RPA Control Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _odoo() -> odoo_rpa.OdooClient:
    return odoo_rpa.OdooClient(odoo_rpa.ODOO_URL, odoo_rpa.ODOO_DB, odoo_rpa.ODOO_EMAIL, odoo_rpa.ODOO_PASSWORD)


def _score(query: str, value: str) -> float:
    return heal_policy.similarity(query, value)


def _read_rows(odoo: odoo_rpa.OdooClient, model: str, ids: List[int], fields: List[str]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    try:
        return odoo.read(model, [int(x) for x in ids], fields)
    except Exception:
        return []


def _safe_context_path(raw: Any) -> Optional[Path]:
    try:
        path = Path(str(raw or ""))
        if not path.is_absolute():
            path = BASE_DIR / path
        resolved = path.resolve()
        allowed_roots = [
            Path(odoo_rpa.LOG_DIR).resolve(),
            (BASE_DIR / "logs").resolve(),
        ]
        if not any(resolved == root or root in resolved.parents for root in allowed_roots):
            return None
        if not resolved.name.startswith("run_context_") or resolved.suffix.casefold() != ".json":
            return None
        if not resolved.exists():
            return None
        return resolved
    except Exception:
        return None


def _read_context_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        try:
            obj = yaml.safe_load(path.read_text(encoding="utf-8"))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}


def _hidden_key(path: Path) -> str:
    return str(path.resolve()).casefold()


def _load_hidden_rollback_sources() -> set[str]:
    try:
        raw = json.loads(ROLLBACK_HIDDEN_STORE.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return {str(x).casefold() for x in raw}
    except Exception:
        pass
    return set()


def _save_hidden_rollback_sources(values: set[str]) -> None:
    try:
        ROLLBACK_HIDDEN_STORE.parent.mkdir(parents=True, exist_ok=True)
        ROLLBACK_HIDDEN_STORE.write_text(json.dumps(sorted(values), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _mark_rollback_sources_hidden(paths: List[Path]) -> List[str]:
    if not paths:
        return []
    hidden = _load_hidden_rollback_sources()
    added: List[str] = []
    for path in paths:
        try:
            key = _hidden_key(path)
        except Exception:
            continue
        if key not in hidden:
            hidden.add(key)
            added.append(str(path))
    if added:
        _save_hidden_rollback_sources(hidden)
    return added


def _context_allowed_file_roots(path: Optional[Path]) -> List[Path]:
    roots: List[Path] = []
    if path and path.exists():
        obj = _read_context_obj(path)
        for key in ("artifact_dir", "log_dir"):
            value = obj.get(key)
            if value:
                try:
                    roots.append(Path(str(value)).resolve())
                except Exception:
                    pass
    return roots


def _safe_generated_file_path(raw: Any, source_path: Any = None) -> Optional[Path]:
    ctx_path = _safe_context_path(source_path) if source_path else None
    return odoo_rollback.safe_log_file_path(raw, extra_roots=_context_allowed_file_roots(ctx_path))


def _json_key(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(value)


def _action_identity(action: Dict[str, Any]) -> tuple[str, str, int, str]:
    kind = str(action.get("type") or "")
    if kind == "delete_file":
        path = _safe_generated_file_path(action.get("path"), action.get("source_path"))
        return (kind, "", 0, str(path) if path else "")
    model = str(action.get("model") or "")
    try:
        rid = int(action.get("record_id") or 0)
    except Exception:
        rid = 0
    tail = _json_key(action.get("vals") or {}) if kind == "write_restore" else ""
    return (kind, model, rid, tail)


def _record_display(odoo: odoo_rpa.OdooClient, model: str, rid: int) -> str:
    try:
        rows = odoo.read(model, [int(rid)], ["display_name"])
        if rows and rows[0].get("display_name"):
            return str(rows[0]["display_name"])
    except Exception:
        pass
    try:
        rows = odoo.read(model, [int(rid)], ["name"])
        if rows and rows[0].get("name"):
            return str(rows[0]["name"])
    except Exception:
        pass
    return f"#{rid}"


def _record_exists(odoo: odoo_rpa.OdooClient, model: str, rid: int) -> bool:
    try:
        return bool(odoo.search(model, [("id", "=", int(rid))], limit=1))
    except Exception:
        return False


def _m2m_restore_ids(value: Any) -> Optional[List[int]]:
    if not isinstance(value, list) or not value:
        return None
    command = value[0]
    if not isinstance(command, list) or len(command) < 3:
        return None
    if int(command[0] or 0) != 6:
        return None
    return sorted(int(x) for x in (command[2] or []))


def _restore_value_matches(current: Any, wanted: Any) -> bool:
    wanted_m2m = _m2m_restore_ids(wanted)
    if wanted_m2m is not None:
        if not isinstance(current, list):
            return wanted_m2m == []
        return sorted(int(x) for x in current) == wanted_m2m
    if wanted is False or wanted is None:
        return current in (False, None, [], "")
    if isinstance(current, list) and current:
        try:
            return int(current[0]) == int(wanted)
        except Exception:
            return current == wanted
    if isinstance(wanted, float) or isinstance(current, float):
        try:
            return abs(float(current or 0) - float(wanted or 0)) < 0.0001
        except Exception:
            return current == wanted
    return current == wanted


def _rollback_action_pending(odoo: odoo_rpa.OdooClient, action: Dict[str, Any]) -> bool:
    kind = str(action.get("type") or "")
    if kind == "delete_file":
        path = _safe_generated_file_path(action.get("path"), action.get("source_path"))
        return bool(path and path.exists() and path.is_file())
    if kind == "delete":
        return _record_exists(odoo, str(action.get("model") or ""), int(action.get("record_id") or 0))
    if kind == "write_restore":
        model = str(action.get("model") or "")
        rid = int(action.get("record_id") or 0)
        vals = dict(action.get("vals") or {})
        if not model or rid <= 0 or not vals:
            return False
        try:
            rows = odoo.read(model, [rid], list(vals.keys()))
        except Exception:
            return False
        if not rows:
            return False
        row = rows[0]
        return any(not _restore_value_matches(row.get(field), wanted) for field, wanted in vals.items())
    return False


def _context_rollback_actions(
    path: Path,
    odoo: Optional[odoo_rpa.OdooClient] = None,
    pending_only: bool = False,
) -> List[Dict[str, Any]]:
    source_label = path.name
    source_path = str(path)
    actions: List[Dict[str, Any]] = []
    n = 1

    created = odoo_rollback.collect_created_ids_from_context(path)
    created_records = {
        (ROLLBACK_MODEL_MAP[group], int(rid))
        for group, ids in created.items()
        for rid in ids
        if group in ROLLBACK_MODEL_MAP
    }
    for group in ("mails", "activities", "events", "sale_orders", "deals", "contacts"):
        model = ROLLBACK_MODEL_MAP[group]
        model_label = MODEL_LABELS.get(model, model)
        for rid in created.get(group, []):
            display = _record_display(odoo, model, int(rid)) if odoo else f"#{rid}"
            label = (
                f"Удалить или архивировать созданную запись: {model_label} {display} (id={rid})"
                if model == "res.partner"
                else f"Удалить созданную запись: {model_label} {display} (id={rid})"
            )
            actions.append(
                {
                    "id": f"{source_label}:a{n}",
                    "order": n,
                    "source": source_label,
                    "source_path": source_path,
                    "type": "delete",
                    "model": model,
                    "record_id": int(rid),
                    "label": label,
                }
            )
            n += 1

    for a in odoo_rollback.collect_rollback_actions_from_context(path):
        if (str(a["model"]), int(a["record_id"])) in created_records:
            continue
        model_label = MODEL_LABELS.get(a["model"], a["model"])
        display = _record_display(odoo, str(a["model"]), int(a["record_id"])) if odoo else f"#{a['record_id']}"
        actions.append(
            {
                "id": f"{source_label}:a{n}",
                "order": n,
                "source": source_label,
                "source_path": source_path,
                "type": "write_restore",
                "model": a["model"],
                "record_id": int(a["record_id"]),
                "vals": a.get("vals") or {},
                "step_id": a.get("step_id", ""),
                "op": a.get("op", ""),
                "label": (
                    f"Восстановить поля: {model_label} {display} (id={a['record_id']}) "
                    f"после {OP_LABELS.get(str(a.get('op') or ''), a.get('op', ''))}"
                ),
            }
        )
        n += 1

    for file_path in odoo_rollback.collect_created_files_from_context(path):
        safe_path = odoo_rollback.safe_log_file_path(file_path, extra_roots=_context_allowed_file_roots(path))
        if safe_path is None:
            continue
        actions.append(
            {
                "id": f"{source_label}:a{n}",
                "order": n,
                "source": source_label,
                "source_path": source_path,
                "type": "delete_file",
                "path": str(safe_path),
                "label": f"Удалить локальный файл: {safe_path.name}",
            }
        )
        n += 1

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for action in actions:
        if pending_only and odoo and not _rollback_action_pending(odoo, action):
            continue
        ident = _action_identity(action)
        if ident in seen:
            continue
        seen.add(ident)
        deduped.append(action)
    return deduped


def _rollback_action_count(path: Path, odoo: Optional[odoo_rpa.OdooClient] = None) -> int:
    try:
        return len(_context_rollback_actions(path, odoo=odoo, pending_only=bool(odoo)))
    except Exception:
        return 0


def _allowed_action_identities(path: Path) -> set[tuple[str, str, int, str]]:
    return {_action_identity(action) for action in _context_rollback_actions(path, odoo=None)}


def _rollback_sort_key(action: Dict[str, Any]) -> tuple[int, int, str]:
    kind = str(action.get("type") or "")
    if kind == "delete":
        return (0, ROLLBACK_DELETE_ORDER.get(str(action.get("model") or ""), 90), str(action.get("id") or ""))
    if kind == "write_restore":
        try:
            order = -int(action.get("order") or 0)
        except Exception:
            order = 0
        return (1, order, str(action.get("id") or ""))
    if kind == "delete_file":
        return (2, 0, str(action.get("id") or ""))
    return (9, 0, str(action.get("id") or ""))


def _option(label: str, action: str, **extra: Any) -> Dict[str, Any]:
    seed = f"{action}:{label}:{extra.get('record_id','')}:{extra.get('value','')}"
    oid = re.sub(r"[^a-zA-Z0-9_]+", "_", seed).strip("_")[:80] or action
    return {"id": oid, "label": label, "action": action, **extra}


UI_CANDIDATE_MIN_SCORE = 0.70


OP_LABELS = {
    "deal.search": "Поиск сделки",
    "deal.create": "Создание сделки",
    "deal.update_stage": "Смена стадии",
    "deal.update": "Назначение менеджера",
    "deal.add_tags": "Добавление тегов",
    "activity.create": "Создание активности",
    "meeting.schedule": "Планирование встречи",
    "deal.create_quotation": "Создание КП",
    "deal.mark_lost": "Пометка проигрыша",
    "contact.find_or_create": "Поиск или создание контакта",
    "contact.create": "Создание контакта",
    "contact.update": "Обновление контакта",
    "report.sales_daily": "Отчет по продажам",
    "report.export": "Экспорт отчета",
    "notify.email": "Email-уведомление",
    "watchdog": "Проверка результата",
}

MODEL_LABELS = {
    "crm.lead": "сделку",
    "mail.activity": "активность",
    "calendar.event": "встречу",
    "sale.order": "коммерческое предложение",
    "res.partner": "контакт",
    "mail.mail": "письмо",
}

ROLLBACK_MODEL_MAP = {
    "mails": "mail.mail",
    "activities": "mail.activity",
    "events": "calendar.event",
    "sale_orders": "sale.order",
    "deals": "crm.lead",
    "contacts": "res.partner",
}
ROLLBACK_DELETE_ORDER = {
    "mail.mail": 5,
    "mail.activity": 10,
    "calendar.event": 20,
    "sale.order": 30,
    "crm.lead": 40,
    "res.partner": 50,
}


def _resolve_static_vars(value: Any, vars_obj: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        match = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", value.strip())
        if match:
            return vars_obj.get(match.group(1), value)

        def repl(m: re.Match[str]) -> str:
            key = m.group(1)
            replacement = vars_obj.get(key)
            return str(replacement) if replacement is not None else m.group(0)

        return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", repl, value)
    if isinstance(value, list):
        return [_resolve_static_vars(v, vars_obj) for v in value]
    if isinstance(value, dict):
        return {k: _resolve_static_vars(v, vars_obj) for k, v in value.items()}
    return value


def _preview_step(step: Dict[str, Any], vars_obj: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(step)
    raw_input = out.get("input") if isinstance(out.get("input"), dict) else {}
    out["input"] = _resolve_static_vars(raw_input, vars_obj)
    return out


def _deal_options(odoo: odoo_rpa.OdooClient, title: str) -> List[Dict[str, Any]]:
    title = str(title or "").strip()
    if not title:
        return []
    exact_ids = odoo.search("crm.lead", [("name", "=", title), ("type", "=", "opportunity")], limit=5)
    options: List[Dict[str, Any]] = []
    for row in _read_rows(odoo, "crm.lead", exact_ids, ["id", "name", "expected_revenue", "stage_id"]):
        label = f"Точное совпадение: {row.get('name')} | id={row.get('id')}"
        options.append(
            _option(
                label,
                "use_record",
                record_id=int(row["id"]),
                value=row.get("name"),
                confidence=1.0,
                auto_safe=True,
            )
        )

    recent_ids = odoo.search("crm.lead", [("type", "=", "opportunity")], limit=300)
    rows = _read_rows(odoo, "crm.lead", recent_ids, ["id", "name", "expected_revenue", "stage_id"])
    scored = []
    exact_set = {int(x) for x in exact_ids}
    for row in rows:
        rid = int(row.get("id") or 0)
        name = str(row.get("name") or "")
        if not rid or not name or rid in exact_set:
            continue
        score = _score(title, name)
        auto_safe = heal_policy.deal_title_candidate_allowed(title, name)
        scored.append((score, auto_safe, row))
    scored.sort(key=lambda x: x[0], reverse=True)
    for score, auto_safe, row in scored[:5]:
        if score < UI_CANDIDATE_MIN_SCORE:
            continue
        prefix = "Похожая сделка"
        if not auto_safe:
            prefix = "Похожая сделка, только ручной выбор"
        label = f"{prefix}: {row.get('name')} | id={row.get('id')} | уверенность={score:.2f}"
        options.append(
            _option(
                label,
                "use_record",
                record_id=int(row["id"]),
                value=row.get("name"),
                confidence=round(score, 4),
                auto_safe=auto_safe,
            )
        )

    options.append(_option(f"Создать новую сделку: {title}", "create_new_deal", value=title, confidence=1.0))
    return options


def _stage_options(odoo: odoo_rpa.OdooClient, stage: str) -> List[Dict[str, Any]]:
    stage = str(stage or "").strip()
    if not stage:
        return []
    exact_ids = odoo.search("crm.stage", [("name", "=", stage)], limit=5)
    options: List[Dict[str, Any]] = []
    for row in _read_rows(odoo, "crm.stage", exact_ids, ["id", "name"]):
        options.append(_option(f"Использовать стадию: {row.get('name')} | id={row.get('id')}", "use_value", value=row.get("name"), record_id=int(row["id"]), confidence=1.0))

    ids = odoo.search("crm.stage", [], limit=200)
    rows = _read_rows(odoo, "crm.stage", ids, ["id", "name"])
    scored = []
    exact_set = {int(x) for x in exact_ids}
    for row in rows:
        rid = int(row.get("id") or 0)
        name = str(row.get("name") or "")
        if rid and name and rid not in exact_set:
            scored.append((_score(stage, name), row))
    scored.sort(key=lambda x: x[0], reverse=True)
    for score, row in scored[:3]:
        if score < UI_CANDIDATE_MIN_SCORE:
            continue
        options.append(
            _option(
                f"Похожая стадия: {row.get('name')} | id={row.get('id')} | уверенность={score:.2f}",
                "use_value",
                value=row.get("name"),
                record_id=int(row["id"]),
                confidence=round(score, 4),
                auto_safe=True,
            )
        )
    return options


def _user_options(odoo: odoo_rpa.OdooClient, user_text: str) -> List[Dict[str, Any]]:
    user_text = str(user_text or "").strip()
    if not user_text:
        return []
    domain = [("login", "=", user_text)] if "@" in user_text else [("name", "=", user_text)]
    exact_ids = odoo.search("res.users", domain, limit=5)
    options: List[Dict[str, Any]] = []
    for row in _read_rows(odoo, "res.users", exact_ids, ["id", "name", "login"]):
        value = row.get("login") or row.get("name")
        options.append(_option(f"Назначить пользователя: {value} | id={row.get('id')}", "use_value", value=value, record_id=int(row["id"]), confidence=1.0))

    ids = odoo.search("res.users", [], limit=200)
    rows = _read_rows(odoo, "res.users", ids, ["id", "name", "login"])
    scored = []
    exact_set = {int(x) for x in exact_ids}
    for row in rows:
        rid = int(row.get("id") or 0)
        if not rid or rid in exact_set:
            continue
        for key in ("login", "name"):
            value = str(row.get(key) or "")
            if "@" in user_text and key == "name":
                continue
            if "@" in user_text and key == "login" and not odoo_rpa._email_domain_compatible(user_text, value):
                continue
            if value:
                scored.append((_score(user_text, value), row, value))
    scored.sort(key=lambda x: x[0], reverse=True)
    seen = set()
    for score, row, value in scored:
        if score < UI_CANDIDATE_MIN_SCORE:
            continue
        if value in seen:
            continue
        seen.add(value)
        options.append(
            _option(
                f"Похожий пользователь: {value} | id={row.get('id')} | уверенность={score:.2f}",
                "use_value",
                value=value,
                record_id=int(row["id"]),
                confidence=round(score, 4),
                auto_safe=True,
            )
        )
        if len(options) >= 4:
            break
    return options


def _contact_options(odoo: odoo_rpa.OdooClient, email: str, phone: str) -> List[Dict[str, Any]]:
    query = email or phone
    if not query:
        return []
    ids = odoo.search("res.partner", [], limit=500)
    rows = _read_rows(odoo, "res.partner", ids, ["id", "name", "email", "phone", "mobile"])
    scored = []
    for row in rows:
        for key in ("email", "phone", "mobile", "name"):
            value = str(row.get(key) or "")
            if value:
                scored.append((_score(query, value), row, value))
    scored.sort(key=lambda x: x[0], reverse=True)
    options = []
    seen = set()
    for score, row, value in scored:
        if score < UI_CANDIDATE_MIN_SCORE:
            continue
        rid = int(row.get("id") or 0)
        if not rid or rid in seen:
            continue
        seen.add(rid)
        label = f"Похожий контакт: {row.get('name') or value} | id={rid} | уверенность={score:.2f}"
        options.append(_option(label, "use_record", record_id=rid, value=value, confidence=round(score, 4), auto_safe=True))
        if len(options) >= 3:
            break
    options.append(_option("Создать новый контакт", "create_new_contact", value=query, confidence=1.0))
    return options


def _deal_title_from_step(step: Dict[str, Any]) -> Optional[str]:
    inp = step.get("input") if isinstance(step.get("input"), dict) else {}
    op = step.get("op")
    if op == "deal.search" and inp.get("title"):
        return str(inp.get("title"))
    deal = inp.get("deal")
    if isinstance(deal, dict):
        for key in ("by_title", "title", "name"):
            if deal.get(key):
                return str(deal.get(key))
    elif isinstance(deal, str) and not deal.strip().isdigit() and "${" not in deal:
        return str(deal)
    return None


def _summarize_step(step: Dict[str, Any]) -> str:
    op = str(step.get("op") or "")
    inp = step.get("input") if isinstance(step.get("input"), dict) else {}
    if op == "deal.search":
        return f"Найти сделку: {inp.get('title') or 'по фильтру'}"
    if op == "deal.update_stage":
        return f"Перевести сделку в стадию: {inp.get('stage')}"
    if op == "deal.update":
        return f"Назначить менеджера: {inp.get('salesperson')}"
    if op == "deal.add_tags":
        return f"Добавить теги: {inp.get('tags')}"
    if op == "activity.create":
        return f"Создать активность: {inp.get('summary') or inp.get('type')}"
    if op == "meeting.schedule":
        return f"Запланировать встречу: {inp.get('when')}"
    if op == "deal.create":
        return f"Создать или переиспользовать сделку: {inp.get('title')}"
    if op == "contact.find_or_create":
        return f"Найти или создать контакт: {inp.get('email') or inp.get('phone')}"
    if op == "contact.create":
        return f"Создать контакт: {inp.get('name')}"
    if op == "report.sales_daily":
        return f"Собрать отчет по продажам: {inp.get('period', 'today')}"
    if op == "report.export":
        return f"Экспортировать отчет: {inp.get('format', 'pdf')}"
    if op == "notify.email":
        return f"Подготовить email: {inp.get('to')}"
    if op == "deal.create_quotation":
        return "Создать коммерческое предложение"
    if op == "deal.mark_lost":
        return f"Пометить сделку проигранной: {inp.get('reason', '')}"
    if op == "watchdog":
        return f"Проверить условие: {inp.get('condition')}"
    return OP_LABELS.get(op, op)


def _default_option_id(options: List[Dict[str, Any]], min_confidence: float, fallback_action: str = "") -> str:
    if not options:
        return ""
    high_actions = {"use_record", "use_value", "use_existing_duplicate"}
    for option in options:
        if (
            option.get("action") in high_actions
            and option.get("auto_safe", True) is not False
            and float(option.get("confidence") or 0.0) >= min_confidence
        ):
            return str(option.get("id") or "")
    if fallback_action:
        for option in options:
            if option.get("action") == fallback_action:
                return str(option.get("id") or "")
    return ""


def _preflight(scenario: Dict[str, Any]) -> Dict[str, Any]:
    steps = scenario.get("steps") if isinstance(scenario.get("steps"), list) else []
    vars_obj = scenario.get("vars") if isinstance(scenario.get("vars"), dict) else {}
    plan = []
    confirmations = []
    try:
        odoo = _odoo()
    except Exception as exc:
        return {
            "plan": [
                {
                    "step_id": str(s.get("id") or i),
                    "op": s.get("op"),
                    "op_label": OP_LABELS.get(str(s.get("op") or ""), str(s.get("op") or "")),
                    "summary": _summarize_step(_preview_step(s, vars_obj)),
                }
                for i, s in enumerate(steps, 1)
                if isinstance(s, dict)
            ],
            "confirmations": [],
            "odoo_available": False,
            "odoo_error": str(exc),
        }

    counter = 1
    for idx, step in enumerate(steps, 1):
        if not isinstance(step, dict):
            continue
        step_preview = _preview_step(step, vars_obj)
        sid = str(step.get("id") or f"step_{idx}")
        op = str(step_preview.get("op") or "")
        inp = step_preview.get("input") if isinstance(step_preview.get("input"), dict) else {}
        plan.append({"step_id": sid, "op": op, "op_label": OP_LABELS.get(op, op), "summary": _summarize_step(step_preview)})

        title = _deal_title_from_step(step_preview)
        if title:
            opts = _deal_options(odoo, title)
            has_exact_deal = any(o.get("action") == "use_record" and float(o.get("confidence") or 0.0) >= 1.0 for o in opts)
            has_safe_high_deal = any(
                o.get("action") == "use_record" and float(o.get("confidence") or 0.0) >= odoo_rpa.DEAL_TITLE_SEARCH_MIN_SCORE
                and o.get("auto_safe", True) is not False
                for o in opts
            )
            if not has_exact_deal or op != "deal.search":
                message = (
                    f"Сделка с таким названием не найдена: {title}. Выбери похожую сделку или создай новую."
                    if not has_exact_deal
                    else f"Проверь целевую сделку: {title}"
                )
                confirmations.append(
                    {
                        "id": f"c{counter}",
                        "kind": "deal_reference",
                        "step_id": sid,
                        "op": op,
                        "field": "title" if op == "deal.search" else "deal",
                        "original": title,
                        "risk": "risky",
                        "message": message,
                        "options": opts,
                        "default_option_id": _default_option_id(
                            opts,
                            odoo_rpa.DEAL_TITLE_SEARCH_MIN_SCORE,
                            fallback_action="create_new_deal" if not has_safe_high_deal else "",
                        ),
                    }
                )
                counter += 1

        if op == "deal.update_stage" and inp.get("stage"):
            stage = str(inp.get("stage"))
            opts = _stage_options(odoo, stage)
            if opts and opts[0].get("confidence", 0.0) < 1.0:
                confirmations.append(
                    {
                        "id": f"c{counter}",
                        "kind": "stage_name",
                        "step_id": sid,
                        "op": op,
                        "field": "stage",
                        "original": stage,
                        "risk": "risky",
                        "message": f"Стадия с таким названием не найдена: {stage}. Выбери ближайшую стадию.",
                        "options": opts,
                        "default_option_id": _default_option_id(opts, odoo_rpa.SELF_HEAL_MIN_SCORE),
                    }
                )
                counter += 1

        if op == "deal.update" and inp.get("salesperson"):
            user_text = str(inp.get("salesperson"))
            opts = _user_options(odoo, user_text)
            if opts and opts[0].get("confidence", 0.0) < 1.0:
                confirmations.append(
                    {
                        "id": f"c{counter}",
                        "kind": "salesperson",
                        "step_id": sid,
                        "op": op,
                        "field": "salesperson",
                        "original": user_text,
                        "risk": "risky",
                        "message": f"Пользователь с таким логином не найден: {user_text}. Выбери похожего пользователя.",
                        "options": opts,
                        "default_option_id": _default_option_id(opts, odoo_rpa.SELF_HEAL_MIN_SCORE),
                    }
                )
                counter += 1

        if op == "contact.find_or_create":
            email = str(inp.get("email") or "")
            phone = str(inp.get("phone") or "")
            exact = []
            if email:
                exact = odoo.search("res.partner", [("email", "=", email)], limit=1)
            if not exact and phone:
                exact = odoo.search("res.partner", [("phone", "=", phone)], limit=1)
            if not exact and (email or phone):
                opts = _contact_options(odoo, email, phone)
                confirmations.append(
                    {
                        "id": f"c{counter}",
                        "kind": "contact_lookup",
                        "step_id": sid,
                        "op": op,
                        "field": "contact",
                        "original": email or phone,
                        "risk": "risky",
                        "message": f"Контакт с такими данными не найден: {email or phone}. Выбери похожий контакт или создай новый.",
                        "options": opts,
                        "default_option_id": _default_option_id(opts, 0.96, fallback_action="create_new_contact"),
                    }
                )
                counter += 1

        if op == "deal.create" and inp.get("title"):
            title = str(inp.get("title"))
            exact = odoo.search("crm.lead", [("type", "=", "opportunity"), ("name", "=", title)], limit=1)
            if exact and not inp.get("use_existing") and not inp.get("force_create"):
                opts = [
                    _option("Использовать существующую сделку", "use_existing_duplicate", record_id=int(exact[0]), value=title, confidence=1.0),
                    _option("Создать новую сделку-дубликат", "force_create_duplicate", value=title, confidence=1.0),
                ]
                confirmations.append(
                    {
                        "id": f"c{counter}",
                        "kind": "deal_create_duplicate",
                        "step_id": sid,
                        "op": op,
                        "field": "title",
                        "original": title,
                        "risk": "policy",
                        "message": f"Сделка уже существует: {title}",
                        "options": opts,
                        "default_option_id": opts[0]["id"],
                    }
                )
                counter += 1
    return {"plan": plan, "confirmations": confirmations, "odoo_available": True, "odoo_error": ""}


def _generate_from_llm(req: PreviewRequest) -> Dict[str, Any]:
    provider = req.provider.strip().lower() or "groq"
    api_env = "GROQ_API_KEY" if provider == "groq" else "OPENAI_API_KEY"
    api_key = os.environ.get("GROQ_API_KEYS" if provider == "groq" else api_env, "").strip()
    if not api_key:
        api_key = os.environ.get(api_env, "").strip()
    if not api_key:
        raise HTTPException(status_code=400, detail=f"{api_env} is not set. Put it in .env.")

    retrieval_df = llm.load_dataset(Path(req.retrieval_data))
    vector_store = None
    try:
        if req.retriever in {"vector", "hybrid"} and req.fewshot_k > 0:
            vector_store = llm.SQLiteVectorStore(Path(".cache/ui_vector_store.sqlite"), dims=env_int("LLM_VECTOR_DIMS", 1536))
            vector_store.ensure_index(retrieval_df, "nl_plain")
        fewshot, fewshot_meta, hinted_ops = llm.select_fewshot(
            df=retrieval_df,
            current_id="_ui",
            k=req.fewshot_k,
            nl_col="nl_plain",
            retriever=req.retriever,
            fewshot_pool=env_int("LLM_FEWSHOT_POOL", 12),
            vector_store=vector_store,
            current_nl_override=req.nl_text,
        )
        relevant_ops = llm.select_relevant_ops(req.nl_text, fewshot_meta, hinted_ops, op_doc_k=env_int("LLM_OP_DOC_K", 10))
        text, obj, validation, attempts = llm.generate_with_repair(
            provider=provider,
            api_key=api_key,
            model=req.model,
            nl_instruction=req.nl_text,
            fewshot=fewshot,
            relevant_ops=relevant_ops,
            repair_attempts=req.repair_attempts,
            temperature=req.temperature,
            max_tokens=req.max_tokens,
        )
    except Exception as exc:
        fb_provider = req.fallback_provider.strip().lower()
        if not fb_provider or not ("429" in str(exc) or "rate limit" in str(exc).lower()):
            raise
        fb_env = "GROQ_API_KEY" if fb_provider == "groq" else "OPENAI_API_KEY"
        fb_key = os.environ.get("GROQ_API_KEYS" if fb_provider == "groq" else fb_env, "").strip()
        if not fb_key:
            fb_key = os.environ.get(fb_env, "").strip()
        if not fb_key:
            raise HTTPException(status_code=400, detail=f"{api_env} failed and {fb_env} is not set for fallback.")
        text, obj, validation, attempts = llm.generate_with_repair(
            provider=fb_provider,
            api_key=fb_key,
            model=req.fallback_model or req.model,
            nl_instruction=req.nl_text,
            fewshot=fewshot,
            relevant_ops=relevant_ops,
            repair_attempts=req.repair_attempts,
            temperature=req.temperature,
            max_tokens=req.max_tokens,
        )
        provider = fb_provider
    finally:
        if vector_store is not None:
            vector_store.close()

    return {
        "yaml_text": text,
        "scenario": _scenario_from_obj(obj, scenario_id="UI"),
        "validation": validation,
        "attempts": attempts,
        "provider": provider,
        "model": req.model,
    }


def _scenario_from_obj(obj: Optional[Dict[str, Any]], scenario_id: str) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        return {"id": scenario_id, "flow": "ui_flow", "vars": {}, "steps": []}
    return {
        "id": scenario_id,
        "flow": str(obj.get("flow") or "ui_flow"),
        "vars": obj.get("vars") if isinstance(obj.get("vars"), dict) else {},
        "steps": obj.get("steps") if isinstance(obj.get("steps"), list) else [],
    }


def _unique_step_id(base: str, steps: List[Dict[str, Any]]) -> str:
    existing = {str(s.get("id") or "") for s in steps if isinstance(s, dict)}
    seed = re.sub(r"[^a-z0-9_]+", "_", base.casefold()).strip("_") or "step"
    if seed not in existing:
        return seed
    n = 2
    while f"{seed}_{n}" in existing:
        n += 1
    return f"{seed}_{n}"


def _apply_decisions(scenario: Dict[str, Any], confirmations: List[Dict[str, Any]], decisions: Dict[str, str]) -> Dict[str, Any]:
    sc = copy.deepcopy(scenario)
    steps = sc.get("steps") if isinstance(sc.get("steps"), list) else []
    by_id = {str(s.get("id") or ""): s for s in steps if isinstance(s, dict)}
    inserts: List[tuple[int, Dict[str, Any]]] = []

    for conf in confirmations:
        cid = str(conf.get("id") or "")
        selected = decisions.get(cid) or str(conf.get("default_option_id") or "")
        options = {str(o.get("id") or ""): o for o in conf.get("options") or []}
        opt = options.get(selected)
        if not opt:
            continue
        step_id = str(conf.get("step_id") or "")
        step = by_id.get(step_id)
        if not step:
            continue
        inp = step.setdefault("input", {})
        action = str(opt.get("action") or "")
        kind = str(conf.get("kind") or "")

        if kind == "deal_reference":
            if action == "use_record":
                if step.get("op") == "deal.search":
                    inp["title"] = opt.get("value") or conf.get("original")
                else:
                    inp["deal"] = {"id": int(opt["record_id"])}
            elif action == "create_new_deal":
                new_id = _unique_step_id(f"create_{step_id}_deal", steps)
                title_value = opt.get("value") or conf.get("original")
                new_step = {
                    "id": new_id,
                    "op": "deal.create",
                    "input": {"title": title_value, "force_create": True},
                }
                idx = steps.index(step)
                inserts.append((idx, new_step))
                if step.get("op") == "deal.search":
                    inp["title"] = title_value
                else:
                    inp["deal"] = {"id": "${" + new_id + ".id}"}
        elif kind == "stage_name" and action == "use_value":
            inp["stage"] = opt.get("value") or conf.get("original")
        elif kind == "salesperson" and action == "use_value":
            inp["salesperson"] = opt.get("value") or conf.get("original")
        elif kind == "deal_create_duplicate":
            if action == "use_existing_duplicate":
                inp["use_existing"] = True
                inp.pop("force_create", None)
            elif action == "force_create_duplicate":
                inp["force_create"] = True
                inp.pop("use_existing", None)
        elif kind == "contact_lookup":
            if action == "create_new_contact":
                continue
            if action == "use_record":
                value = str(opt.get("value") or "")
                if "@" in value:
                    inp["email"] = value
                elif re.sub(r"\D", "", value):
                    inp["phone"] = value

    for idx, new_step in sorted(inserts, key=lambda x: x[0], reverse=True):
        steps.insert(idx, new_step)
    sc["steps"] = steps
    return sc


def _selected_confirmation_summaries(confirmations: List[Dict[str, Any]], decisions: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for conf in confirmations or []:
        cid = str(conf.get("id") or "")
        selected = decisions.get(cid) or str(conf.get("default_option_id") or "")
        options = {str(o.get("id") or ""): o for o in conf.get("options") or []}
        opt = options.get(selected)
        if not opt:
            continue
        out.append(
            {
                "id": cid,
                "kind": str(conf.get("kind") or ""),
                "step_id": str(conf.get("step_id") or ""),
                "original": conf.get("original"),
                "selected_label": opt.get("label") or opt.get("action"),
                "selected_action": opt.get("action"),
                "selected_value": opt.get("value"),
                "record_id": opt.get("record_id"),
                "confidence": opt.get("confidence"),
            }
        )
    return out


def _format_context_time(path: Path) -> str:
    return dt.datetime.fromtimestamp(path.stat().st_mtime).strftime("%d.%m.%Y %H:%M:%S")


def _scenario_id_from_filename(path: Path) -> str:
    name = path.stem
    match = re.match(r"run_context_(.+)_\d{8}_\d{6}$", name)
    return match.group(1) if match else name.replace("run_context_", "")


def _shorten(value: Any, limit: int = 42) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _ru_plural(n: int, one: str, few: str, many: str) -> str:
    n_abs = abs(int(n))
    if 11 <= n_abs % 100 <= 14:
        word = many
    elif n_abs % 10 == 1:
        word = one
    elif 2 <= n_abs % 10 <= 4:
        word = few
    else:
        word = many
    return f"{n} {word}"


def _rollback_candidate_text(n: int) -> str:
    n_abs = abs(int(n))
    word = "действия" if n_abs % 10 == 1 and n_abs % 100 != 11 else "действий"
    return f"до {n} {word} отката"


def _context_display_title(path: Path, obj: Optional[Dict[str, Any]] = None) -> str:
    obj = obj if isinstance(obj, dict) else _read_context_obj(path)
    scenario_id = str(obj.get("scenario_id") or _scenario_id_from_filename(path) or "scenario")
    if scenario_id == "UI":
        scenario_label = "Ручной запуск из сервиса"
    elif scenario_id.startswith("RBSHOT"):
        scenario_label = f"Проверка отката {_shorten(scenario_id, 24)}"
    else:
        scenario_label = f"Сценарий {_shorten(scenario_id, 28)}"
    return f"{scenario_label} - {_format_context_time(path)}"


def _context_step_details(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    traces = obj.get("step_traces") or []
    scenario = obj.get("scenario") if isinstance(obj.get("scenario"), dict) else {}
    scenario_steps = scenario.get("steps") if isinstance(scenario.get("steps"), list) else []
    by_id = {str(s.get("id") or ""): s for s in scenario_steps if isinstance(s, dict)}
    details: List[Dict[str, Any]] = []
    for trace in traces:
        if not isinstance(trace, dict):
            continue
        sid = str(trace.get("step_id") or "")
        op = str(trace.get("op") or "")
        step = by_id.get(sid)
        details.append(
            {
                "step_id": sid,
                "op": op,
                "op_label": OP_LABELS.get(op, op),
                "status": str(trace.get("status") or ""),
                "summary": _summarize_step(step) if step else OP_LABELS.get(op, op),
            }
        )
    return details


def _context_created_counts(path: Path) -> Dict[str, int]:
    labels = {
        "deals": "сделки",
        "activities": "активности",
        "events": "встречи",
        "sale_orders": "КП",
        "contacts": "контакты",
        "mails": "письма",
    }
    try:
        created = odoo_rollback.collect_created_ids_from_context(path)
    except Exception:
        created = {}
    return {labels.get(key, key): len(value or []) for key, value in created.items() if value}


def _uniq_int_values(values: List[Any]) -> List[int]:
    out: List[int] = []
    seen = set()
    for value in values:
        try:
            n = int(value)
        except Exception:
            continue
        if n <= 0 or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _created_ids_from_context_obj(obj: Dict[str, Any]) -> Dict[str, List[int]]:
    steps = obj.get("steps", {}) if isinstance(obj, dict) else {}
    traces = obj.get("step_traces", []) if isinstance(obj, dict) else []
    out: Dict[str, List[int]] = {
        "deals": [],
        "activities": [],
        "events": [],
        "sale_orders": [],
        "contacts": [],
        "mails": [],
    }
    for tr in traces:
        if not isinstance(tr, dict) or str(tr.get("status")) != "success":
            continue
        sid = str(tr.get("step_id") or "")
        op = str(tr.get("op") or "")
        step_out = steps.get(sid)
        if not isinstance(step_out, dict):
            continue
        if op == "deal.create":
            if bool(step_out.get("created", True)):
                out["deals"].append(step_out.get("id"))
        elif op == "activity.create":
            out["activities"].extend(step_out.get("activity_ids") or [])
        elif op == "meeting.schedule":
            if bool(step_out.get("created", False)):
                out["events"].append(step_out.get("event_id"))
        elif op == "deal.create_quotation":
            out["sale_orders"].append(step_out.get("sale_order_id"))
        elif op == "contact.create":
            if bool(step_out.get("created", True)):
                out["contacts"].append(step_out.get("id"))
        elif op == "contact.find_or_create":
            if bool(step_out.get("created", False)):
                out["contacts"].append(step_out.get("id"))
        elif op == "notify.email":
            out["mails"].append(step_out.get("odoo_mail_id"))
    return {key: _uniq_int_values(values) for key, values in out.items()}


def _context_created_counts_from_obj(obj: Dict[str, Any]) -> Dict[str, int]:
    labels = {
        "deals": "сделки",
        "activities": "активности",
        "events": "встречи",
        "sale_orders": "КП",
        "contacts": "контакты",
        "mails": "письма",
    }
    created = _created_ids_from_context_obj(obj)
    return {labels.get(key, key): len(value or []) for key, value in created.items() if value}


def _iter_context_file_values(obj: Any) -> List[Any]:
    values: List[Any] = []
    file_keys = {"csv_path", "pdf_path", "path", "saved_eml"}

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key in file_keys:
                    values.append(child)
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    if isinstance(obj, dict):
        walk(obj.get("steps"))
        walk(obj.get("aliases"))
    return values


def _context_file_names_from_obj(obj: Dict[str, Any]) -> List[str]:
    roots = []
    for key in ("artifact_dir", "log_dir"):
        value = obj.get(key)
        if value:
            roots.append(value)
    names: List[str] = []
    seen = set()
    for value in _iter_context_file_values(obj):
        path = odoo_rollback.safe_log_file_path(value, extra_roots=roots)
        if not path:
            continue
        key = str(path).casefold()
        if key in seen:
            continue
        seen.add(key)
        names.append(path.name)
        if len(names) >= 8:
            break
    return names


def _fast_rollback_action_count_from_obj(obj: Dict[str, Any]) -> int:
    created = _created_ids_from_context_obj(obj)
    identities = set()
    created_records = set()
    for group, ids in created.items():
        model = ROLLBACK_MODEL_MAP.get(group)
        if not model:
            continue
        for rid in ids:
            created_records.add((model, int(rid)))
            identities.add(("delete", model, int(rid), ""))

    for action in obj.get("rollback_actions") or []:
        if not isinstance(action, dict) or str(action.get("type") or "") != "write_restore":
            continue
        model = str(action.get("model") or "")
        try:
            rid = int(action.get("record_id") or 0)
        except Exception:
            continue
        vals = action.get("vals") if isinstance(action.get("vals"), dict) else {}
        if not model or rid <= 0 or (model, rid) in created_records or not vals:
            continue
        identities.add(("write_restore", model, rid, _json_key(vals)))

    roots = [obj.get("artifact_dir"), obj.get("log_dir")]
    for value in _iter_context_file_values(obj):
        path = odoo_rollback.safe_log_file_path(value, extra_roots=roots)
        if path:
            identities.add(("delete_file", "", 0, str(path)))
    return len(identities)


def _context_file_names(path: Path) -> List[str]:
    try:
        return [Path(x).name for x in odoo_rollback.collect_created_files_from_context(path)[:8]]
    except Exception:
        return []


@app.get("/api/health")
def health() -> Dict[str, Any]:
    try:
        odoo = _odoo()
        version = odoo.common.version()
        return {"ok": True, "odoo": version}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.post("/api/preview")
def preview(req: PreviewRequest) -> Dict[str, Any]:
    if req.yaml_text.strip():
        obj, validation = llm.validate_prediction(req.yaml_text)
        generated = {
            "yaml_text": req.yaml_text,
            "scenario": _scenario_from_obj(obj, scenario_id="UI"),
            "validation": validation,
            "attempts": [],
            "provider": "manual_yaml",
            "model": "",
        }
    else:
        if not req.nl_text.strip():
            raise HTTPException(status_code=400, detail="nl_text or yaml_text is required")
        generated = _generate_from_llm(req)
    preflight = _preflight(generated["scenario"])
    return {**generated, **preflight}


@app.post("/api/execute")
def execute(req: ExecuteRequest) -> Dict[str, Any]:
    manual_confirmations = _selected_confirmation_summaries(req.confirmations, req.decisions)
    scenario = _apply_decisions(req.scenario, req.confirmations, req.decisions)
    if manual_confirmations:
        scenario["ui_manual_confirmations"] = manual_confirmations
    odoo = _odoo()
    started = time.time()
    old_mode = odoo_rpa.SELF_HEAL_MODE
    if req.confirmations:
        # UI-исполнение должно учитывать выбор человека: точные замены идут штатно,
        # а неподтвержденные рискованные подстановки пропускаются вместо угадывания.
        odoo_rpa.SELF_HEAL_MODE = "confirm"
    try:
        ctx = odoo_rpa.run_scenario(scenario, odoo)
    finally:
        odoo_rpa.SELF_HEAL_MODE = old_mode
    summary = odoo_rpa.summarize_execution(ctx)
    return {
        "scenario": scenario,
        "summary": summary,
        "step_traces": ctx.step_traces,
        "self_heal_events": ctx.self_heal_events,
        "rollback_actions": ctx.rollback_actions,
        "alerts": ctx.alerts,
        "manual_confirmations": manual_confirmations,
        "duration_ms": int((time.time() - started) * 1000),
    }


@app.get("/api/run-contexts")
def run_contexts(limit: int = 30, active_only: bool = False) -> Dict[str, Any]:
    paths = sorted(Path(odoo_rpa.LOG_DIR).glob("run_context_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    rows = []
    hidden_sources = _load_hidden_rollback_sources() if active_only else set()
    max_rows = max(1, min(limit, 200))
    for path in paths:
        if active_only and _hidden_key(path) in hidden_sources:
            continue
        obj = _read_context_obj(path)
        step_count = len(obj.get("step_traces") or []) if isinstance(obj, dict) else 0
        rollback_count = _fast_rollback_action_count_from_obj(obj)
        if active_only and rollback_count <= 0:
            continue
        self_heal_count = len(obj.get("self_heal_events") or []) if isinstance(obj, dict) else 0
        alert_count = len(obj.get("alerts") or []) if isinstance(obj, dict) else 0
        display_name = _context_display_title(path, obj)
        rows.append(
            {
                "path": str(path),
                "name": path.name,
                "display_name": display_name,
                "mtime": _format_context_time(path),
                "steps": step_count,
                "rollback_actions": rollback_count,
                "self_heal_events": self_heal_count,
                "alerts": alert_count,
                "rollback_count_exact": False,
                "summary": (
                    f"{_ru_plural(step_count, 'шаг', 'шага', 'шагов')}, "
                    f"{_rollback_candidate_text(rollback_count)}, "
                    f"self-healing: {self_heal_count}, предупреждений: {alert_count}"
                ),
                "details": {
                    "scenario_id": str(obj.get("scenario_id") or _scenario_id_from_filename(path)),
                    "flow": str(obj.get("scenario_flow") or ""),
                    "steps": _context_step_details(obj),
                    "created": _context_created_counts_from_obj(obj),
                    "files": _context_file_names_from_obj(obj),
                    "artifact_dir": str(obj.get("artifact_dir") or ""),
                },
            }
        )
        if len(rows) >= max_rows:
            break
    return {"contexts": rows}


@app.post("/api/rollback/preview")
def rollback_preview(req: RollbackPreviewRequest) -> Dict[str, Any]:
    odoo = _odoo()
    actions: List[Dict[str, Any]] = []
    inactive_sources: List[Path] = []
    source_counts: Dict[str, int] = {}
    seen = set()
    for raw in req.paths:
        path = _safe_context_path(raw)
        if path is None:
            continue
        source_actions = _context_rollback_actions(path, odoo=odoo, pending_only=True)
        if not source_actions:
            source_counts[str(path)] = 0
            inactive_sources.append(path)
            continue
        added_for_source = 0
        for action in source_actions:
            ident = _action_identity(action)
            if ident in seen:
                continue
            seen.add(ident)
            action["id"] = f"a{len(actions) + 1}"
            action["source_title"] = _context_display_title(path)
            actions.append(action)
            added_for_source += 1
        source_counts[str(path)] = added_for_source
    actions.sort(key=_rollback_sort_key)
    hidden = _mark_rollback_sources_hidden(inactive_sources)
    return {"actions": actions, "count": len(actions), "source_counts": source_counts, "inactive_sources": hidden}


@app.post("/api/rollback/apply")
def rollback_apply(req: RollbackApplyRequest) -> Dict[str, Any]:
    odoo = _odoo()
    applied = 0
    failed = 0
    errors: List[str] = []
    seen = set()
    source_paths: Dict[str, Path] = {}
    for action in sorted(req.actions, key=_rollback_sort_key):
        try:
            kind = str(action.get("type") or "")
            source_path = _safe_context_path(action.get("source_path"))
            if source_path is None:
                failed += 1
                errors.append("rollback action rejected: missing or unsafe run_context source")
                continue
            source_paths[_hidden_key(source_path)] = source_path
            ident = _action_identity(action)
            if ident in seen:
                continue
            if ident not in _allowed_action_identities(source_path):
                failed += 1
                errors.append(f"rollback action rejected: action is not present in {source_path.name}")
                continue
            seen.add(ident)
            if not _rollback_action_pending(odoo, action):
                continue
            if kind == "delete":
                model = str(action.get("model") or "")
                rid = int(action.get("record_id"))
                applied += odoo_rollback.delete_records(odoo, model, [rid], apply=True)
            elif kind == "write_restore":
                model = str(action.get("model") or "")
                rid = int(action.get("record_id"))
                odoo.write(model, [rid], dict(action.get("vals") or {}))
                applied += 1
            elif kind == "delete_file":
                safe_path = _safe_generated_file_path(action.get("path"), source_path)
                if not safe_path:
                    failed += 1
                    errors.append("rollback action rejected: unsafe generated file path")
                    continue
                if safe_path.exists() and safe_path.is_file():
                    safe_path.unlink()
                    applied += 1
        except Exception as exc:
            failed += 1
            errors.append(str(exc))
    completed_sources: List[Path] = []
    for source_path in source_paths.values():
        try:
            if not _context_rollback_actions(source_path, odoo=odoo, pending_only=True):
                completed_sources.append(source_path)
        except Exception:
            pass
    hidden = _mark_rollback_sources_hidden(completed_sources)
    return {"applied": applied, "failed": failed, "errors": errors[:10], "completed_sources": hidden}


if UI_DIR.exists():
    app.mount("/", StaticFiles(directory=str(UI_DIR), html=True), name="ui")
