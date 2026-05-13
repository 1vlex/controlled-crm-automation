"""
Утилита отката и очистки данных Odoo, затронутых RPA-запусками.

Примеры использования:
1) Dry-run: удалить все opportunities, созданные сегодня в UTC-окне
   python odoo_rollback.py --delete_deals_created_today

2) Apply: удалить все opportunities, созданные сегодня
   python odoo_rollback.py --delete_deals_created_today --apply

3) Apply: удалить opportunities, созданные за последние 2 UTC-дня (сегодня + вчера)
   python odoo_rollback.py --delete_deals_created_last_days 2 --apply

4) Откатить сущности, созданные конкретным run_context-файлом
   python odoo_rollback.py --run_context logs/run_context_U01_20260306_204934.json --apply

5) Откатить обновления полей (tags/stage/owner/lost fields), записанные в run_context
   python odoo_rollback.py --run_context logs/run_context_U01_20260306_204934.json --revert_updates --apply
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import odoo_rpa


def _chunks(items: List[int], size: int = 200) -> Iterable[List[int]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _uniq_ints(values: Iterable[Any]) -> List[int]:
    out: List[int] = []
    seen: Set[int] = set()
    for x in values:
        try:
            n = int(x)
        except Exception:
            continue
        if n > 0 and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _uniq_paths(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        path = safe_log_file_path(value)
        if path is None:
            continue
        key = str(path)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _safe_file_roots(extra_roots: List[Any] | None = None) -> List[Path]:
    raw_roots: List[Any] = [
        odoo_rpa.LOG_DIR,
        odoo_rpa.BASE_DIR / "logs",
        getattr(odoo_rpa, "ARTIFACT_DIR", ""),
        odoo_rpa.BASE_DIR / "artifacts",
    ]
    if extra_roots:
        raw_roots.extend(extra_roots)
    roots: List[Path] = []
    seen = set()
    for root in raw_roots:
        if not root:
            continue
        try:
            resolved = Path(root).resolve()
        except Exception:
            continue
        key = str(resolved).casefold()
        if key in seen:
            continue
        seen.add(key)
        roots.append(resolved)
    return roots


def safe_log_file_path(value: Any, extra_roots: List[Any] | None = None) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        candidate = Path(text)
        if not candidate.is_absolute():
            candidate = odoo_rpa.BASE_DIR / candidate
        resolved = candidate.resolve()
        allowed_roots = _safe_file_roots(extra_roots)
        if not any(resolved == root or root in resolved.parents for root in allowed_roots):
            return None
        if resolved.suffix.casefold() not in {".csv", ".txt", ".pdf", ".eml", ".json"}:
            return None
        return resolved
    except Exception:
        return None


def _iter_file_values(obj: Any) -> Iterable[Any]:
    file_keys = {"csv_path", "pdf_path", "path", "saved_eml"}
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in file_keys:
                yield value
            yield from _iter_file_values(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _iter_file_values(value)


def collect_created_ids_from_context(path: Path) -> Dict[str, List[int]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
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
        if not isinstance(tr, dict):
            continue
        if str(tr.get("status")) != "success":
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

    for k in list(out.keys()):
        out[k] = _uniq_ints(out[k])
    return out


def collect_rollback_actions_from_context(path: Path) -> List[Dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    actions = obj.get("rollback_actions", []) if isinstance(obj, dict) else []
    out: List[Dict[str, Any]] = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        if str(a.get("type") or "") != "write_restore":
            continue
        model = str(a.get("model") or "")
        rid = a.get("record_id")
        vals = a.get("vals")
        if not model or vals is None:
            continue
        try:
            rid_n = int(rid)
        except Exception:
            continue
        if rid_n <= 0:
            continue
        out.append(
            {
                "type": "write_restore",
                "model": model,
                "record_id": rid_n,
                "vals": vals if isinstance(vals, dict) else {},
                "step_id": str(a.get("step_id") or ""),
                "op": str(a.get("op") or ""),
            }
        )
    return out


def collect_created_files_from_context(path: Path) -> List[str]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        return []
    values: List[Any] = []
    for key in ("steps", "aliases"):
        values.extend(list(_iter_file_values(obj.get(key))))
    return _uniq_paths(values)


def apply_rollback_actions(odoo: odoo_rpa.OdooClient, actions: List[Dict[str, Any]], apply: bool) -> Dict[str, int]:
    requested = len(actions)
    if requested == 0:
        return {"requested": 0, "applied": 0, "failed": 0}
    if not apply:
        return {"requested": requested, "applied": requested, "failed": 0}

    applied = 0
    failed = 0
    # Обратный порядок нужен, чтобы сначала откатить самые поздние изменения.
    for a in reversed(actions):
        try:
            odoo.write(str(a["model"]), [int(a["record_id"])], dict(a.get("vals") or {}))
            applied += 1
        except Exception:
            failed += 1
    return {"requested": requested, "applied": applied, "failed": failed}


def delete_files(paths: List[str], apply: bool, extra_roots: List[Any] | None = None) -> int:
    safe_paths = [safe_log_file_path(p, extra_roots=extra_roots) for p in paths]
    safe_paths = [p for p in safe_paths if p is not None]
    if not safe_paths:
        return 0
    if not apply:
        return len(safe_paths)
    deleted = 0
    for path in safe_paths:
        try:
            if path.exists() and path.is_file():
                path.unlink()
            deleted += 1
        except Exception:
            pass
    return deleted


def delete_records(odoo: odoo_rpa.OdooClient, model: str, ids: List[int], apply: bool) -> int:
    ids = _uniq_ints(ids)
    if not ids:
        return 0
    if not apply:
        return len(ids)
    deleted = 0
    for part in _chunks(ids):
        try:
            odoo.execute(model, "unlink", part)
            deleted += len(part)
        except Exception:
            for rid in part:
                try:
                    odoo.execute(model, "unlink", [rid])
                    deleted += 1
                except Exception:
                    if model == "res.partner":
                        try:
                            odoo.write(model, [rid], {"active": False})
                            deleted += 1
                        except Exception:
                            pass
    return deleted


def collect_deals_created_today(odoo: odoo_rpa.OdooClient) -> List[int]:
    today_utc = dt.datetime.utcnow().date()
    start_utc = dt.datetime.combine(today_utc, dt.time.min).strftime("%Y-%m-%d %H:%M:%S")
    return _uniq_ints(
        odoo.search(
            "crm.lead",
            [("type", "=", "opportunity"), ("create_date", ">=", start_utc)],
            limit=5000,
        )
    )


def collect_deals_created_last_days(odoo: odoo_rpa.OdooClient, days: int) -> List[int]:
    if days <= 0:
        return []
    today_utc = dt.datetime.utcnow().date()
    start_date = today_utc - dt.timedelta(days=days - 1)
    start_utc = dt.datetime.combine(start_date, dt.time.min).strftime("%Y-%m-%d %H:%M:%S")
    end_utc = dt.datetime.combine(today_utc + dt.timedelta(days=1), dt.time.min).strftime("%Y-%m-%d %H:%M:%S")
    return _uniq_ints(
        odoo.search(
            "crm.lead",
            [("type", "=", "opportunity"), ("create_date", ">=", start_utc), ("create_date", "<", end_utc)],
            limit=5000,
        )
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_context", action="append", default=[], help="Путь к run_context_*.json. Можно указать несколько раз")
    ap.add_argument("--delete_deals_created_today", action="store_true", help="Удалить все opportunities, созданные сегодня по UTC")
    ap.add_argument(
        "--delete_deals_created_last_days",
        type=int,
        default=0,
        help="Удалить opportunities, созданные за последние N UTC-дней. N=2 означает сегодня+вчера.",
    )
    ap.add_argument(
        "--revert_updates",
        action="store_true",
        help="Применить write_restore rollback actions из run_context-файлов (tags/stage/owner/lost fields).",
    )
    ap.add_argument("--apply", action="store_true", help="Реально удалить записи. По умолчанию работает dry-run")
    args = ap.parse_args()

    odoo = odoo_rpa.OdooClient(
        odoo_rpa.ODOO_URL,
        odoo_rpa.ODOO_DB,
        odoo_rpa.ODOO_EMAIL,
        odoo_rpa.ODOO_PASSWORD,
    )

    all_deals: Set[int] = set()
    all_activities: Set[int] = set()
    all_events: Set[int] = set()
    all_sale_orders: Set[int] = set()
    all_contacts: Set[int] = set()
    all_mails: Set[int] = set()
    all_files: Set[str] = set()
    rollback_actions: List[Dict[str, Any]] = []
    created_records: Set[tuple[str, int]] = set()

    for p in args.run_context:
        rp = Path(p)
        if not rp.exists():
            continue
        ids = collect_created_ids_from_context(rp)
        all_deals.update(ids["deals"])
        all_activities.update(ids["activities"])
        all_events.update(ids["events"])
        all_sale_orders.update(ids["sale_orders"])
        all_contacts.update(ids["contacts"])
        all_mails.update(ids["mails"])
        all_files.update(collect_created_files_from_context(rp))
        for model, values in {
            "crm.lead": ids["deals"],
            "mail.activity": ids["activities"],
            "calendar.event": ids["events"],
            "sale.order": ids["sale_orders"],
            "res.partner": ids["contacts"],
            "mail.mail": ids["mails"],
        }.items():
            for rid in values:
                created_records.add((model, int(rid)))
        if args.revert_updates:
            rollback_actions.extend(collect_rollback_actions_from_context(rp))

    if args.delete_deals_created_today:
        all_deals.update(collect_deals_created_today(odoo))
    if args.delete_deals_created_last_days > 0:
        all_deals.update(collect_deals_created_last_days(odoo, args.delete_deals_created_last_days))

    rollback_actions = [
        action
        for action in rollback_actions
        if (str(action.get("model") or ""), int(action.get("record_id") or 0)) not in created_records
    ]

    # Сначала удаляем зависимые объекты.
    deleted_mails = delete_records(odoo, "mail.mail", sorted(all_mails), args.apply)
    deleted_activities = delete_records(odoo, "mail.activity", sorted(all_activities), args.apply)
    deleted_events = delete_records(odoo, "calendar.event", sorted(all_events), args.apply)
    deleted_sale_orders = delete_records(odoo, "sale.order", sorted(all_sale_orders), args.apply)
    deleted_deals = delete_records(odoo, "crm.lead", sorted(all_deals), args.apply)
    deleted_contacts = delete_records(odoo, "res.partner", sorted(all_contacts), args.apply)
    deleted_files = delete_files(sorted(all_files), args.apply)
    rollback_report = apply_rollback_actions(odoo, rollback_actions, args.apply) if args.revert_updates else {
        "requested": 0,
        "applied": 0,
        "failed": 0,
    }

    report = {
        "dry_run": int(not args.apply),
        "requested": {
            "deals": len(all_deals),
            "activities": len(all_activities),
            "events": len(all_events),
            "sale_orders": len(all_sale_orders),
            "contacts": len(all_contacts),
            "mails": len(all_mails),
            "files": len(all_files),
        },
        "deleted": {
            "mails": deleted_mails,
            "deals": deleted_deals,
            "activities": deleted_activities,
            "events": deleted_events,
            "sale_orders": deleted_sale_orders,
            "contacts": deleted_contacts,
            "files": deleted_files,
        },
        "rollback_updates": rollback_report,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
