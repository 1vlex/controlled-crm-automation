"""
Execution-first оценка сценариев Odoo RPA.

Скрипт проверяет, выполняются ли сценарии фактически, а не только выглядит ли YAML корректным.
Режимы запуска:
1) Reference DSL из CSV-колонки (по умолчанию dsl_yaml)
2) Predicted DSL из preds_combined_baseline/*.yaml или другого --preds_dir
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

import odoo_rollback
import odoo_rpa

MANUAL_REVIEW_COLUMNS = [
    "manual_strict_task_success",
    "manual_entity_resolution_correct",
    "manual_wrong_object_success",
    "manual_postcondition_satisfied",
    "manual_reviewer",
    "manual_notes",
]


def parse_scenario_yaml(yaml_text: str, sid: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        obj = yaml.safe_load(yaml_text)
    except Exception as e:
        return None, f"yaml_parse_error: {e}"
    if not isinstance(obj, dict):
        return None, "yaml_parse_error: root is not a mapping"
    steps = obj.get("steps")
    if not isinstance(steps, list):
        return None, "schema_error: steps is not a list"
    vars_obj = obj.get("vars", {})
    if not isinstance(vars_obj, dict):
        vars_obj = {}
    flow = obj.get("flow") or str(sid).lower()
    scenario = {
        "id": sid,
        "flow": flow,
        "vars": vars_obj,
        "steps": steps,
    }
    return scenario, None


def load_rows_from_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def build_scenarios(
    csv_rows: List[Dict[str, str]],
    id_col: str,
    dsl_col: str,
    preds_dir: Optional[Path],
    limit: int,
) -> List[Dict[str, Any]]:
    rows = csv_rows[:limit] if limit > 0 else csv_rows
    out: List[Dict[str, Any]] = []

    for row in rows:
        sid = str(row.get(id_col, "")).strip()
        if not sid:
            continue

        if preds_dir:
            pred_path = preds_dir / f"{sid}.yaml"
            if not pred_path.exists():
                out.append({"id": sid, "scenario": None, "parse_error": f"missing prediction: {pred_path}"})
                continue
            yaml_text = pred_path.read_text(encoding="utf-8", errors="replace")
        else:
            yaml_text = str(row.get(dsl_col, ""))

        scenario, err = parse_scenario_yaml(yaml_text, sid)
        out.append({"id": sid, "scenario": scenario, "parse_error": err})

    return out


def safe_div(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return a / b


def _created_from_ctx(ctx: odoo_rpa.ExecutionContext) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {
        "deals": [],
        "activities": [],
        "events": [],
        "sale_orders": [],
        "contacts": [],
    }
    for trace in ctx.step_traces:
        if trace.get("status") != "success":
            continue
        sid = str(trace.get("step_id") or "")
        op = str(trace.get("op") or "")
        step_out = ctx.steps.get(sid)
        if not isinstance(step_out, dict):
            continue
        if op == "deal.create" and bool(step_out.get("created", True)):
            out["deals"].append(step_out.get("id"))
        elif op == "activity.create":
            out["activities"].extend(step_out.get("activity_ids") or [])
        elif op == "meeting.schedule" and bool(step_out.get("created", False)):
            out["events"].append(step_out.get("event_id"))
        elif op == "deal.create_quotation":
            out["sale_orders"].append(step_out.get("sale_order_id"))
        elif op == "contact.create" and bool(step_out.get("created", True)):
            out["contacts"].append(step_out.get("id"))
        elif op == "contact.find_or_create" and bool(step_out.get("created", False)):
            out["contacts"].append(step_out.get("id"))
    cleaned: Dict[str, List[int]] = {}
    for key, values in out.items():
        seen: List[int] = []
        for value in values:
            try:
                n = int(value)
            except Exception:
                continue
            if n > 0 and n not in seen:
                seen.append(n)
        cleaned[key] = seen
    return cleaned


def _rollback_ctx(odoo: odoo_rpa.OdooClient, ctx: odoo_rpa.ExecutionContext, enabled: bool) -> Dict[str, int]:
    if not enabled:
        return {"requested": 0, "applied": 0, "failed": 0}
    created = _created_from_ctx(ctx)
    applied = 0
    failed = 0
    delete_map = {
        "activities": "mail.activity",
        "events": "calendar.event",
        "sale_orders": "sale.order",
        "deals": "crm.lead",
        "contacts": "res.partner",
    }
    created_records = {
        (model, int(rid))
        for group, model in delete_map.items()
        for rid in created.get(group, [])
    }
    for group, model in delete_map.items():
        ids = created.get(group, [])
        requested = len(ids)
        deleted = odoo_rollback.delete_records(odoo, model, ids, apply=True)
        applied += deleted
        failed += max(0, requested - deleted)
    restore_actions = [
        action
        for action in ctx.rollback_actions
        if (str(action.get("model") or ""), int(action.get("record_id") or 0)) not in created_records
    ]
    rollback_report = odoo_rollback.apply_rollback_actions(odoo, restore_actions, apply=True)
    applied += int(rollback_report.get("applied", 0))
    failed += int(rollback_report.get("failed", 0))
    requested = sum(len(v) for v in created.values()) + len(restore_actions)
    return {"requested": requested, "applied": applied, "failed": failed}


def run_execution_eval(
    data_path: Path,
    outdir: Path,
    id_col: str,
    dsl_col: str,
    limit: int,
    preds_dir: Optional[Path],
    odoo_url: str,
    odoo_db: str,
    odoo_email: str,
    odoo_password: str,
    rollback_each: bool,
) -> Path:
    csv_rows = load_rows_from_csv(data_path)
    scenarios = build_scenarios(csv_rows, id_col=id_col, dsl_col=dsl_col, preds_dir=preds_dir, limit=limit)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "preds" if preds_dir else "reference"
    run_dir = outdir / f"exec_eval_{mode}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "config.json").write_text(
        json.dumps(
            {
                "data_path": str(data_path),
                "id_col": id_col,
                "dsl_col": dsl_col,
                "limit": limit,
                "preds_dir": str(preds_dir) if preds_dir else "",
                "odoo_url": odoo_url,
                "odoo_db": odoo_db,
                "odoo_email": odoo_email,
                "rollback_each": rollback_each,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    try:
        odoo = odoo_rpa.OdooClient(odoo_url, odoo_db, odoo_email, odoo_password)
    except Exception as e:
        raise RuntimeError(
            f"Odoo connection failed (url={odoo_url}, db={odoo_db}, email={odoo_email}): {e}"
        ) from e

    results: List[Dict[str, Any]] = []

    total_steps = 0
    total_success = 0
    total_skipped = 0
    total_error = 0
    total_heal_steps = 0
    total_heal_success_steps = 0
    total_heal_events = 0
    total_heal_failed_steps = 0
    total_alerts = 0
    total_rollback_requested = 0
    total_rollback_applied = 0
    total_rollback_failed = 0
    scenarios_success = 0
    scenarios_parse_ok = 0

    for item in scenarios:
        sid = item["id"]
        sc = item["scenario"]
        parse_error = item["parse_error"]

        if parse_error or not sc:
            results.append(
                {
                    "id": sid,
                    "flow": "",
                    "parse_ok": 0,
                    "scenario_success": 0,
                    "steps_total": 0,
                    "steps_success": 0,
                    "steps_skipped": 0,
                    "steps_error": 0,
                    "step_success_rate": 0.0,
                    "skip_rate": 0.0,
                    "self_heal_events": 0,
                    "self_heal_steps": 0,
                    "self_heal_success_steps": 0,
                    "self_heal_failed_steps": 0,
                    "alerts_total": 0,
                    "rollback_requested": 0,
                    "rollback_applied": 0,
                    "rollback_failed": 0,
                    "post_heal_success_rate": 0.0,
                    "error": parse_error or "scenario missing",
                }
            )
            continue

        scenarios_parse_ok += 1
        try:
            ctx = odoo_rpa.run_scenario(sc, odoo)
            s = odoo_rpa.summarize_execution(ctx)
            rollback_report = _rollback_ctx(odoo, ctx, rollback_each)
            scenario_success = 1 if s["scenario_success"] else 0

            results.append(
                {
                    "id": sid,
                    "flow": sc.get("flow", ""),
                    "parse_ok": 1,
                    "scenario_success": scenario_success,
                    "steps_total": s["steps_total"],
                    "steps_success": s["steps_success"],
                    "steps_skipped": s["steps_skipped"],
                    "steps_error": s["steps_error"],
                    "step_success_rate": round(safe_div(s["steps_success"], max(1, s["steps_total"])), 4),
                    "skip_rate": round(safe_div(s["steps_skipped"], max(1, s["steps_total"])), 4),
                    "self_heal_events": s["self_heal_events"],
                    "self_heal_steps": s["self_heal_steps"],
                    "self_heal_success_steps": s["self_heal_success_steps"],
                    "self_heal_failed_steps": s.get(
                        "self_heal_failed_steps",
                        max(0, int(s["self_heal_steps"]) - int(s["self_heal_success_steps"])),
                    ),
                    "alerts_total": s.get("alerts_total", 0),
                    "rollback_requested": rollback_report["requested"],
                    "rollback_applied": rollback_report["applied"],
                    "rollback_failed": rollback_report["failed"],
                    "post_heal_success_rate": round(
                        safe_div(s["self_heal_success_steps"], max(1, s["self_heal_steps"])), 4
                    ),
                    "error": "",
                }
            )

            total_steps += s["steps_total"]
            total_success += s["steps_success"]
            total_skipped += s["steps_skipped"]
            total_error += s["steps_error"]
            total_heal_steps += s["self_heal_steps"]
            total_heal_success_steps += s["self_heal_success_steps"]
            total_heal_events += s["self_heal_events"]
            total_heal_failed_steps += s.get(
                "self_heal_failed_steps",
                max(0, int(s["self_heal_steps"]) - int(s["self_heal_success_steps"])),
            )
            total_alerts += s.get("alerts_total", 0)
            total_rollback_requested += rollback_report["requested"]
            total_rollback_applied += rollback_report["applied"]
            total_rollback_failed += rollback_report["failed"]
            scenarios_success += scenario_success
        except Exception as e:
            results.append(
                {
                    "id": sid,
                    "flow": sc.get("flow", ""),
                    "parse_ok": 1,
                    "scenario_success": 0,
                    "steps_total": 0,
                    "steps_success": 0,
                    "steps_skipped": 0,
                    "steps_error": 1,
                    "step_success_rate": 0.0,
                    "skip_rate": 0.0,
                    "self_heal_events": 0,
                    "self_heal_steps": 0,
                    "self_heal_success_steps": 0,
                    "self_heal_failed_steps": 0,
                    "alerts_total": 1,
                    "rollback_requested": 0,
                    "rollback_applied": 0,
                    "rollback_failed": 0,
                    "post_heal_success_rate": 0.0,
                    "error": f"execution_exception: {e}",
                }
            )
            total_error += 1
            total_alerts += 1

    for r in results:
        for col in MANUAL_REVIEW_COLUMNS:
            r.setdefault(col, "")

    out_csv = run_dir / "results.csv"
    if results:
        with out_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)

    scenario_n = max(1, len(results))
    summary = {
        "n_scenarios": len(results),
        "parse_ok_rate": round(safe_div(scenarios_parse_ok, scenario_n), 4),
        "exec_success_rate": round(safe_div(scenarios_success, scenario_n), 4),
        "step_success_rate": round(safe_div(total_success, max(1, total_steps)), 4),
        "skip_rate": round(safe_div(total_skipped, max(1, total_steps)), 4),
        "error_rate": round(safe_div(total_error, max(1, total_steps)), 4),
        "self_heal_trigger_rate": round(safe_div(total_heal_steps, max(1, total_steps)), 4),
        "post_heal_success_rate": round(safe_div(total_heal_success_steps, max(1, total_heal_steps)), 4),
        "self_heal_failure_rate": round(safe_div(total_heal_failed_steps, max(1, total_heal_steps)), 4),
        "self_heal_events_total": total_heal_events,
        "self_heal_failed_steps_total": total_heal_failed_steps,
        "alerts_total": total_alerts,
        "rollback_requested": total_rollback_requested,
        "rollback_applied": total_rollback_applied,
        "rollback_failed": total_rollback_failed,
        "steps_total": total_steps,
        "steps_success": total_success,
        "steps_skipped": total_skipped,
        "steps_error": total_error,
    }

    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"run_dir": str(run_dir), **summary}, ensure_ascii=False, indent=2))
    return run_dir


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=str, default="combined_api_eval_odoo_API.csv")
    ap.add_argument("--outdir", type=str, default="execution_runs")
    ap.add_argument("--id_col", type=str, default="id")
    ap.add_argument("--dsl_col", type=str, default="dsl_yaml")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--preds_dir", type=str, default="", help="Путь к папке с prediction-файлами <id>.yaml")
    ap.add_argument("--rollback_each", action="store_true", help="Откатывать созданные записи и изменения полей после каждого сценария")

    ap.add_argument("--odoo_url", type=str, default=odoo_rpa.ODOO_URL)
    ap.add_argument("--odoo_db", type=str, default=odoo_rpa.ODOO_DB)
    ap.add_argument("--odoo_email", type=str, default=odoo_rpa.ODOO_EMAIL)
    ap.add_argument("--odoo_password", type=str, default=odoo_rpa.ODOO_PASSWORD)
    args = ap.parse_args()

    preds_dir = Path(args.preds_dir) if args.preds_dir.strip() else None
    try:
        run_execution_eval(
            data_path=Path(args.data),
            outdir=Path(args.outdir),
            id_col=args.id_col,
            dsl_col=args.dsl_col,
            limit=args.limit,
            preds_dir=preds_dir,
            odoo_url=args.odoo_url,
            odoo_db=args.odoo_db,
            odoo_email=args.odoo_email,
            odoo_password=args.odoo_password,
            rollback_each=args.rollback_each,
        )
    except Exception as e:
        print(f"execution_eval failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
