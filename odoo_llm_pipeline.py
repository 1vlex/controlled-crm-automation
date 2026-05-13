"""
Единый pipeline: NL -> LLM DSL -> исполнение в Odoo RPA -> сообщения для уточнения.

Это интеграционная точка входа для проверки реального качества исполнения в Odoo,
а не только метрик похожести YAML.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

import llm
import odoo_rollback
import odoo_rpa
from runtime_config import env_float, env_int, env_str

MANUAL_REVIEW_COLUMNS = [
    "manual_strict_task_success",
    "manual_entity_resolution_correct",
    "manual_wrong_object_success",
    "manual_postcondition_satisfied",
    "manual_reviewer",
    "manual_notes",
]


def _is_rate_limit_error(err: Exception) -> bool:
    text = str(err or "").lower()
    return ("429" in text) or ("rate limit" in text) or ("rate_limit" in text)


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
    scenario = {"id": sid, "flow": flow, "vars": vars_obj, "steps": steps}
    return scenario, None


def _extract_suggestions(err_text: str) -> List[str]:
    m = re.search(r"suggestions=\[(.*?)\]", err_text or "")
    if not m:
        return []
    raw = m.group(1)
    out: List[str] = []
    for x in raw.split(","):
        item = x.strip().strip("'").strip('"')
        if item:
            out.append(item)
    return out


def build_user_clarification(
    sid: str,
    nl_text: str,
    scenario_error: str,
    ctx: Optional[odoo_rpa.ExecutionContext],
    exec_summary: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    issues: List[str] = []
    questions: List[str] = []

    if scenario_error:
        issues.append(f"Сценарий завершился ошибкой: {scenario_error}")

    if ctx is not None:
        for t in ctx.step_traces:
            if t.get("status") == "success":
                continue
            step_id = str(t.get("step_id") or "")
            op = str(t.get("op") or "")
            err = str(t.get("error") or "unknown error")
            issues.append(f"Шаг {step_id} ({op}): {err}")

            low = err.lower()
            if "salesperson not found" in low:
                sugg = _extract_suggestions(err)
                if sugg:
                    questions.append(f"Не найден salesperson. Вы имели в виду: {', '.join(sugg)}?")
                else:
                    questions.append("Не найден salesperson. Уточните логин/email пользователя в Odoo.")
            elif "stage not found" in low:
                questions.append("Не найдена стадия сделки. Уточните точное имя стадии в вашей Odoo.")
            elif "deal not found" in low:
                sugg = _extract_suggestions(err)
                if sugg:
                    questions.append(f"Сделка не найдена. Вы имели в виду: {', '.join(sugg)}?")
                else:
                    questions.append("Сделка не найдена. Уточните title сделки или пришлите точный ID.")
            elif "duplicate title exists" in low:
                questions.append(
                    "Найдена сделка с таким же названием. Подтвердите действие: использовать существующую (use_existing=true) или создать дубль (force_create=true)."
                )
            elif "mail.activity.type not found" in low:
                questions.append(
                    "В Odoo отсутствуют типы активностей. Создать базовый тип 'To Do' и повторить сценарий?"
                )
            elif "deals is empty" in low or "no deals" in low:
                questions.append(
                    "В шаг не переданы сделки. Уточните, какой список сделок нужно использовать."
                )

    if exec_summary is not None and exec_summary.get("self_heal_failed_steps", 0) > 0:
        questions.append(
            "Self-healing пытался исправить проблему, но не завершил шаг. Подтвердите корректные значения для проблемных шагов."
        )

    # Оставляем только уникальные вопросы, сохраняя порядок.
    questions = list(dict.fromkeys(questions))

    if not questions and (issues or scenario_error):
        questions.append("Подтвердите желаемый результат и критичные параметры (stage/salesperson/deal title).")

    needs_input = 1 if (scenario_error or issues) else 0
    text_lines: List[str] = []
    if needs_input:
        text_lines.append(f"Сценарий {sid} выполнен с проблемами.")
        for item in issues[:4]:
            text_lines.append(f"- {item}")
        if questions:
            text_lines.append("Уточнение для пользователя:")
            for i, q in enumerate(questions, start=1):
                text_lines.append(f"{i}. {q}")
    else:
        text_lines.append(f"Сценарий {sid} выполнен успешно, уточнения не требуются.")

    return {
        "id": sid,
        "nl_text": nl_text,
        "needs_user_input": needs_input,
        "message": "\n".join(text_lines),
        "questions": questions,
    }


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
        step_id = str(trace.get("step_id") or "")
        op = str(trace.get("op") or "")
        step_out = ctx.steps.get(step_id)
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
                item_id = int(value)
            except Exception:
                continue
            if item_id > 0 and item_id not in seen:
                seen.append(item_id)
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
        (model, int(record_id))
        for group, model in delete_map.items()
        for record_id in created.get(group, [])
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
    requested = sum(len(values) for values in created.values()) + len(restore_actions)
    return {"requested": requested, "applied": applied, "failed": failed}


def run_pipeline(
    data_path: Path,
    retrieval_data_path: Optional[Path],
    outdir: Path,
    nl_col: str,
    limit: int,
    id_include: Optional[Set[str]],
    provider: str,
    model: str,
    fallback_provider: str,
    fallback_model: str,
    fewshot_k: int,
    retriever: str,
    vector_db: Path,
    vector_dims: int,
    fewshot_pool: int,
    op_doc_k: int,
    repair_attempts: int,
    temperature: float,
    max_tokens: int,
    sleep_s: float,
    preds_dir: Optional[Path],
    odoo_url: str,
    odoo_db: str,
    odoo_email: str,
    odoo_password: str,
    rpa_console_log_level: str,
    rollback_each: bool,
) -> Path:
    df = llm.load_dataset(data_path)
    if nl_col not in df.columns:
        raise ValueError(f"Column '{nl_col}' not found in dataset. Available: {list(df.columns)}")
    retrieval_df = df
    if retrieval_data_path is not None:
        retrieval_df = llm.load_dataset(retrieval_data_path)
        if nl_col not in retrieval_df.columns:
            raise ValueError(
                f"Column '{nl_col}' not found in retrieval dataset. Available: {list(retrieval_df.columns)}"
            )

    provider_norm = str(provider or "groq").strip().lower()
    api_env = "GROQ_API_KEY" if provider_norm == "groq" else "OPENAI_API_KEY"
    api_key = os.environ.get("GROQ_API_KEYS" if provider_norm == "groq" else api_env, "").strip()
    if not api_key:
        api_key = os.environ.get(api_env, "").strip()
    fallback_provider_norm = str(fallback_provider or "").strip().lower()
    fallback_model_text = str(fallback_model or "").strip()
    fallback_api_key = ""
    fallback_api_env = ""
    if fallback_provider_norm:
        fallback_api_env = "GROQ_API_KEY" if fallback_provider_norm == "groq" else "OPENAI_API_KEY"
        fallback_api_key = os.environ.get("GROQ_API_KEYS" if fallback_provider_norm == "groq" else fallback_api_env, "").strip()
        if not fallback_api_key:
            fallback_api_key = os.environ.get(fallback_api_env, "").strip()
    if not preds_dir and not api_key:
        raise RuntimeError(f"{api_env} is required when --preds_dir is not provided.")
    if fallback_provider_norm and not fallback_api_key:
        raise RuntimeError(f"{fallback_api_env} is required when fallback provider is enabled.")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    run_dir = outdir / f"odoo_llm_pipeline_{ts}"
    pred_out = run_dir / "preds"
    run_dir.mkdir(parents=True, exist_ok=True)
    pred_out.mkdir(parents=True, exist_ok=True)

    (run_dir / "config.json").write_text(
        json.dumps(
            {
                "data_path": str(data_path),
                "retrieval_data_path": str(retrieval_data_path) if retrieval_data_path else "",
                "nl_col": nl_col,
                "limit": limit,
                "id_include": sorted(id_include) if id_include else [],
                "provider": provider_norm,
                "model": model,
                "fallback_provider": fallback_provider_norm,
                "fallback_model": fallback_model_text,
                "fewshot_k": fewshot_k,
                "retriever": retriever,
                "vector_db": str(vector_db),
                "vector_dims": vector_dims,
                "fewshot_pool": fewshot_pool,
                "op_doc_k": op_doc_k,
                "repair_attempts": repair_attempts,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "sleep_s": sleep_s,
                "preds_dir": str(preds_dir) if preds_dir else "",
                "odoo_url": odoo_url,
                "odoo_db": odoo_db,
                "odoo_email": odoo_email,
                "rpa_console_log_level": rpa_console_log_level,
                "rollback_each": rollback_each,
                "retrieval_docs": int(len(retrieval_df)),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    vector_store: Optional[llm.SQLiteVectorStore] = None
    if (not preds_dir) and retriever in {"vector", "hybrid"} and fewshot_k > 0:
        vector_store = llm.SQLiteVectorStore(vector_db, dims=vector_dims)
        vector_store.ensure_index(retrieval_df, nl_col)

    # В pipeline-режиме консоль остается тихой, подробные трассы пишутся в logs/run.log.
    odoo_rpa.set_console_log_level(rpa_console_log_level)
    odoo = odoo_rpa.OdooClient(odoo_url, odoo_db, odoo_email, odoo_password)
    work_df = df.head(limit) if limit > 0 else df
    if id_include:
        work_df = work_df[work_df["id"].astype(str).isin(set(id_include))]

    rows: List[Dict[str, Any]] = []
    clarifications: List[Dict[str, Any]] = []

    total_steps = 0
    total_success = 0
    total_skipped = 0
    total_errors = 0
    total_heal_steps = 0
    total_heal_success = 0
    total_alerts = 0
    total_rollback_requested = 0
    total_rollback_applied = 0
    total_rollback_failed = 0
    scenario_success_count = 0
    llm_generation_error_count = 0

    try:
        for _, row in work_df.iterrows():
            sid = str(row["id"])
            nl_text = str(row[nl_col])
            pred_text = ""
            pred_obj: Optional[Dict[str, Any]] = None
            val = {"parse_ok": 0, "schema_ok": 0, "contract_ok": 0, "flow_snake_ok": 0, "error": ""}
            attempts_used = 0
            scenario_error = ""
            exec_summary: Optional[Dict[str, Any]] = None
            ctx: Optional[odoo_rpa.ExecutionContext] = None
            used_provider = provider_norm
            used_model = model
            rollback_report = {"requested": 0, "applied": 0, "failed": 0}

            if preds_dir:
                p = preds_dir / f"{sid}.yaml"
                if not p.exists():
                    scenario_error = f"missing prediction file: {p}"
                else:
                    pred_text = p.read_text(encoding="utf-8", errors="replace")
                    pred_obj, val = llm.validate_prediction(pred_text)
            else:
                try:
                    fewshot, fewshot_meta, hinted_ops = llm.select_fewshot(
                        df=retrieval_df,
                        current_id=sid,
                        k=fewshot_k,
                        nl_col=nl_col,
                        retriever=retriever,
                        fewshot_pool=fewshot_pool,
                        vector_store=vector_store,
                        current_nl_override=nl_text,
                    )
                    relevant_ops = llm.select_relevant_ops(nl_text, fewshot_meta, hinted_ops, op_doc_k=op_doc_k)
                    pred_text, pred_obj, val, attempt_records = llm.generate_with_repair(
                        provider=provider_norm,
                        api_key=api_key,
                        model=model,
                        nl_instruction=nl_text,
                        fewshot=fewshot,
                        relevant_ops=relevant_ops,
                        repair_attempts=repair_attempts,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    )
                    attempts_used = max(0, len(attempt_records) - 1)
                except Exception as e:
                    if (
                        fallback_provider_norm
                        and (fallback_provider_norm != provider_norm or (fallback_model_text and fallback_model_text != model))
                        and _is_rate_limit_error(e)
                    ):
                        try:
                            used_provider = fallback_provider_norm
                            used_model = fallback_model_text or model
                            pred_text, pred_obj, val, attempt_records = llm.generate_with_repair(
                                provider=used_provider,
                                api_key=fallback_api_key,
                                model=used_model,
                                nl_instruction=nl_text,
                                fewshot=fewshot,
                                relevant_ops=relevant_ops,
                                repair_attempts=repair_attempts,
                                temperature=temperature,
                                max_tokens=max_tokens,
                            )
                            attempts_used = max(0, len(attempt_records) - 1)
                        except Exception as e2:
                            scenario_error = f"llm_generation_error: primary={e}; fallback={e2}"
                    else:
                        scenario_error = f"llm_generation_error: {e}"

            (pred_out / f"{sid}.yaml").write_text(pred_text or "", encoding="utf-8")

            if not scenario_error and (val["parse_ok"] and val["schema_ok"]):
                scenario: Optional[Dict[str, Any]] = None
                if isinstance(pred_obj, dict):
                    steps_obj = pred_obj.get("steps")
                    vars_obj = pred_obj.get("vars", {})
                    if isinstance(steps_obj, list):
                        if not isinstance(vars_obj, dict):
                            vars_obj = {}
                        scenario = {
                            "id": sid,
                            "flow": str(pred_obj.get("flow") or sid.lower()),
                            "vars": vars_obj,
                            "steps": steps_obj,
                        }

                if scenario is None:
                    scenario, parse_err = parse_scenario_yaml(pred_text, sid)
                    if parse_err:
                        scenario_error = parse_err

                if scenario is not None:
                    try:
                        ctx = odoo_rpa.run_scenario(scenario, odoo)
                        exec_summary = odoo_rpa.summarize_execution(ctx)
                        rollback_report = _rollback_ctx(odoo, ctx, rollback_each)
                    except Exception as e:
                        scenario_error = f"execution_exception: {e}"

            if exec_summary is None:
                exec_summary = {
                    "steps_total": 0,
                    "steps_success": 0,
                    "steps_skipped": 0,
                    "steps_error": 1 if scenario_error else 0,
                    "self_heal_events": 0,
                    "self_heal_steps": 0,
                    "self_heal_success_steps": 0,
                    "self_heal_failed_steps": 0,
                    "alerts_total": 1 if scenario_error else 0,
                    "scenario_success": False,
                }

            if scenario_error.startswith("llm_generation_error:"):
                llm_generation_error_count += 1

            clarification = build_user_clarification(
                sid=sid,
                nl_text=nl_text,
                scenario_error=scenario_error or str(val.get("error") or ""),
                ctx=ctx,
                exec_summary=exec_summary,
            )
            clarifications.append(clarification)

            scenario_success = 1 if exec_summary.get("scenario_success") else 0
            rows.append(
                {
                    "id": sid,
                    "parse_ok": int(val["parse_ok"]),
                    "schema_ok": int(val["schema_ok"]),
                    "contract_ok": int(val["contract_ok"]),
                    "scenario_success": scenario_success,
                    "steps_total": int(exec_summary["steps_total"]),
                    "steps_success": int(exec_summary["steps_success"]),
                    "steps_skipped": int(exec_summary["steps_skipped"]),
                    "steps_error": int(exec_summary["steps_error"]),
                    "self_heal_events": int(exec_summary.get("self_heal_events", 0)),
                    "self_heal_steps": int(exec_summary.get("self_heal_steps", 0)),
                    "self_heal_success_steps": int(exec_summary.get("self_heal_success_steps", 0)),
                    "self_heal_failed_steps": int(exec_summary.get("self_heal_failed_steps", 0)),
                    "alerts_total": int(exec_summary.get("alerts_total", 0)),
                    "attempts_used": attempts_used,
                    "llm_provider": used_provider,
                    "llm_model": used_model,
                    "needs_user_input": int(clarification["needs_user_input"]),
                    "rollback_requested": int(rollback_report["requested"]),
                    "rollback_applied": int(rollback_report["applied"]),
                    "rollback_failed": int(rollback_report["failed"]),
                    "error": (scenario_error or str(val.get("error") or ""))[:500],
                }
            )

            total_steps += int(exec_summary["steps_total"])
            total_success += int(exec_summary["steps_success"])
            total_skipped += int(exec_summary["steps_skipped"])
            total_errors += int(exec_summary["steps_error"])
            total_heal_steps += int(exec_summary.get("self_heal_steps", 0))
            total_heal_success += int(exec_summary.get("self_heal_success_steps", 0))
            total_alerts += int(exec_summary.get("alerts_total", 0))
            total_rollback_requested += int(rollback_report["requested"])
            total_rollback_applied += int(rollback_report["applied"])
            total_rollback_failed += int(rollback_report["failed"])
            scenario_success_count += scenario_success

            if sleep_s > 0:
                time.sleep(sleep_s)
    finally:
        if vector_store is not None:
            vector_store.close()

    for r in rows:
        for col in MANUAL_REVIEW_COLUMNS:
            r.setdefault(col, "")

    results_path = run_dir / "results.csv"
    if rows:
        with results_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    clar_path = run_dir / "clarifications.json"
    clar_path.write_text(json.dumps(clarifications, ensure_ascii=False, indent=2), encoding="utf-8")

    n = max(1, len(rows))
    total_accounted_steps = max(1, total_success + total_skipped + total_errors)
    summary = {
        "n_scenarios": len(rows),
        "exec_success_rate": round(safe_div(scenario_success_count, n), 4),
        "step_success_rate": round(safe_div(total_success, total_accounted_steps), 4),
        "skip_rate": round(safe_div(total_skipped, total_accounted_steps), 4),
        "error_rate": round(safe_div(total_errors, total_accounted_steps), 4),
        "self_heal_trigger_rate": round(safe_div(total_heal_steps, max(1, total_steps)), 4),
        "post_heal_success_rate": round(safe_div(total_heal_success, max(1, total_heal_steps)), 4),
        "alerts_total": total_alerts,
        "needs_user_input_count": sum(int(x["needs_user_input"]) for x in clarifications),
        "llm_generation_error_count": llm_generation_error_count,
        "rollback_requested": total_rollback_requested,
        "rollback_applied": total_rollback_applied,
        "rollback_failed": total_rollback_failed,
        "results_csv": str(results_path),
        "clarifications_json": str(clar_path),
    }

    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"run_dir": str(run_dir), **summary}, ensure_ascii=False, indent=2))
    return run_dir


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=str, default="nl_dsl_scenarios_odoo_API.csv")
    ap.add_argument(
        "--retrieval_data",
        type=str,
        default="",
        help="Опциональный датасет только для retriever index/few-shot pool, чтобы не было утечки из eval.",
    )
    ap.add_argument("--outdir", type=str, default="pipeline_runs")
    ap.add_argument("--nl_col", type=str, default="nl_plain")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--id_include", type=str, default="", help="ID сценариев через запятую, которые нужно выполнить")

    ap.add_argument("--provider", type=str, default=env_str("LLM_PROVIDER", "groq"), choices=["groq", "openai"])
    ap.add_argument("--model", type=str, default=env_str("LLM_MODEL", "llama-3.3-70b-versatile"))
    ap.add_argument(
        "--fallback_provider",
        type=str,
        default=env_str("LLM_FALLBACK_PROVIDER", ""),
        choices=["", "groq", "openai"],
        help="Опциональный fallback-провайдер, если основной провайдер уперся в rate limit.",
    )
    ap.add_argument("--fallback_model", type=str, default=env_str("LLM_FALLBACK_MODEL", ""), help="ID fallback-модели. Если пусто, используется --model")
    ap.add_argument("--fewshot_k", type=int, default=env_int("LLM_FEWSHOT_K", 3))
    ap.add_argument("--retriever", type=str, default=env_str("LLM_RETRIEVER", "hybrid"), choices=["none", "lexical", "vector", "hybrid"])
    ap.add_argument("--vector_db", type=str, default=".cache/llm_vector_store.sqlite")
    ap.add_argument("--vector_dims", type=int, default=env_int("LLM_VECTOR_DIMS", 1536))
    ap.add_argument("--fewshot_pool", type=int, default=env_int("LLM_FEWSHOT_POOL", 12))
    ap.add_argument("--op_doc_k", type=int, default=env_int("LLM_OP_DOC_K", 10))
    ap.add_argument("--repair_attempts", type=int, default=env_int("LLM_REPAIR_ATTEMPTS", 2))
    ap.add_argument("--temperature", type=float, default=env_float("LLM_TEMPERATURE", 0.0))
    ap.add_argument("--max_tokens", type=int, default=env_int("LLM_MAX_TOKENS", 2200))
    ap.add_argument("--sleep", type=float, default=env_float("LLM_SLEEP", 1.0))
    ap.add_argument("--preds_dir", type=str, default="", help="Использовать готовые <id>.yaml файлы и пропустить LLM-генерацию")

    ap.add_argument("--odoo_url", type=str, default=odoo_rpa.ODOO_URL)
    ap.add_argument("--odoo_db", type=str, default=odoo_rpa.ODOO_DB)
    ap.add_argument("--odoo_email", type=str, default=odoo_rpa.ODOO_EMAIL)
    ap.add_argument("--odoo_password", type=str, default=odoo_rpa.ODOO_PASSWORD)
    ap.add_argument(
        "--rpa_console_log_level",
        type=str,
        default="WARNING",
        help="Уровень логирования RPA в консоли (DEBUG/INFO/WARNING/ERROR/CRITICAL).",
    )
    ap.add_argument("--rollback_each", action="store_true", help="Откатывать созданные записи и изменения полей после каждого сценария")
    args = ap.parse_args()

    preds_dir = Path(args.preds_dir) if args.preds_dir.strip() else None
    retrieval_data_path = Path(args.retrieval_data) if args.retrieval_data.strip() else None
    id_include = {x.strip() for x in args.id_include.split(",") if x.strip()}
    run_pipeline(
        data_path=Path(args.data),
        retrieval_data_path=retrieval_data_path,
        outdir=Path(args.outdir),
        nl_col=args.nl_col,
        limit=args.limit,
        id_include=id_include if id_include else None,
        provider=args.provider,
        model=args.model,
        fallback_provider=args.fallback_provider,
        fallback_model=args.fallback_model,
        fewshot_k=args.fewshot_k,
        retriever=args.retriever,
        vector_db=Path(args.vector_db),
        vector_dims=args.vector_dims,
        fewshot_pool=args.fewshot_pool,
        op_doc_k=args.op_doc_k,
        repair_attempts=args.repair_attempts,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        sleep_s=args.sleep,
        preds_dir=preds_dir,
        odoo_url=args.odoo_url,
        odoo_db=args.odoo_db,
        odoo_email=args.odoo_email,
        odoo_password=args.odoo_password,
        rpa_console_log_level=args.rpa_console_log_level,
        rollback_each=args.rollback_each,
    )


if __name__ == "__main__":
    main()
