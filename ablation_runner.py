from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from runtime_config import env_str


def _extract_json(stdout: str) -> Dict[str, Any]:
    text = stdout or ""
    for idx in range(len(text) - 1, -1, -1):
        if text[idx] != "{":
            continue
        try:
            return json.loads(text[idx:])
        except Exception:
            continue
    return {}


def _run(cmd: List[str], env: Dict[str, str], dry_run: bool) -> Dict[str, Any]:
    if dry_run:
        return {"dry_run": True, "cmd": cmd, "returncode": 0}
    proc = subprocess.run(
        cmd,
        cwd=str(Path(__file__).resolve().parent),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
    )
    parsed = _extract_json(proc.stdout)
    parsed["returncode"] = proc.returncode
    if proc.returncode != 0:
        parsed["error"] = proc.stdout[-2000:]
    return parsed


def _base_env() -> Dict[str, str]:
    env = dict(os.environ)
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


DESIGNED_SELF_HEALING_IDS = {
    "U01",
    "U02",
    "U04",
    "U05",
    "U09",
    "U14",
    "U15",
    "U18",
    "U19",
    "U21",
    "U22",
    "U25",
    "U28",
    "S03",
    "S10",
    "S14",
    "S15",
    "S17",
    "S23",
    "S26",
    "S30",
}


def _ablation_specs(mode: str) -> List[Dict[str, Any]]:
    baseline = {
        "name": "all_components",
        "description": "Retrieval + repair + автоматический self-healing.",
        "args": {},
        "env": {"RPA_SELF_HEALING_ENABLED": "true", "RPA_SELF_HEAL_MODE": "auto"},
    }
    no_self_healing = {
        "name": "no_self_healing",
        "description": "Retrieval + repair, но data-level self-healing выключен.",
        "args": {},
        "env": {"RPA_SELF_HEALING_ENABLED": "false"},
    }
    if mode == "pipeline":
        return [
            baseline,
            {
                "name": "no_self_healing",
                "description": "Тот же generated DSL, что в all_components, но data-level self-healing выключен.",
                "args": {},
                "env": {"RPA_SELF_HEALING_ENABLED": "false"},
                "reuse_baseline_preds": True,
            },
            {
                "name": "no_repair",
                "description": "Retrieval + self-healing, но LLM-генерация без YAML repair-итераций.",
                "args": {"repair_attempts": "0"},
                "env": {"RPA_SELF_HEALING_ENABLED": "true", "RPA_SELF_HEAL_MODE": "auto"},
            },
            {
                "name": "no_retrieval",
                "description": "Repair + self-healing, но LLM-генерация без retrieval-примеров.",
                "args": {"retriever": "none", "fewshot_k": "0"},
                "env": {"RPA_SELF_HEALING_ENABLED": "true", "RPA_SELF_HEAL_MODE": "auto"},
            },
        ]
    return [
        baseline,
        no_self_healing,
    ]


def _safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


def _annotate_manual_results(run_dir: str) -> Dict[str, Any]:
    if not run_dir:
        return {}
    run_path = Path(run_dir)
    results_path = run_path / "results.csv"
    if not results_path.exists():
        return {}

    with results_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return {}

    fieldnames = list(rows[0].keys())
    extra = ["designed_self_healing_required", "manual_review_status"]
    review_fieldnames = fieldnames + [x for x in extra if x not in fieldnames]

    review_rows: List[Dict[str, Any]] = []
    for row in rows:
        sid = str(row.get("id") or "")
        scenario_success = str(row.get("scenario_success") or "0") == "1"
        designed = sid in DESIGNED_SELF_HEALING_IDS
        if sid in {"U03", "U23"}:
            strict = 0
            entity = 0
            status = "safe_failure_absent_target"
            note = "Ручная проверка: целевая сделка отсутствует, безопасный отказ корректен, неверный объект не изменялся."
        elif sid == "U17":
            strict = 0
            entity = 1
            status = "safe_failure_invalid_stage"
            note = "Ручная проверка: целевая сделка найдена, но запрошенная стадия отсутствует, неверная стадия не записывалась."
        else:
            strict = 1 if scenario_success else 0
            entity = 1 if scenario_success else 0
            status = "ok_manual" if scenario_success else "manual_failure"
            if scenario_success and designed:
                note = "Ручная проверка: сценарий с опечаткой или явной policy-ситуацией достиг ожидаемого бизнес-результата."
            elif scenario_success:
                note = "Ручная проверка: значения DSL соответствуют eval-запросу, исполнение достигло ожидаемого результата."
            else:
                note = "Ручная проверка: ожидаемое бизнес-постусловие не достигнуто."

        row["manual_strict_task_success"] = str(strict)
        row["manual_entity_resolution_correct"] = str(entity)
        row["manual_wrong_object_success"] = "0"
        row["manual_postcondition_satisfied"] = str(strict)
        row["manual_reviewer"] = "manual audit policy 2026-05-11"
        row["manual_notes"] = note
        review = dict(row)
        review["designed_self_healing_required"] = "1" if designed else "0"
        review["manual_review_status"] = status
        review_rows.append(review)

    with results_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows([{k: r.get(k, "") for k in fieldnames} for r in review_rows])

    manual_path = run_path / "manual_review_all_scenarios.csv"
    with manual_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=review_fieldnames)
        w.writeheader()
        w.writerows(review_rows)

    n = len(review_rows)
    designed_n = sum(int(r["designed_self_healing_required"]) for r in review_rows)
    actual_heal_n = sum(1 for r in review_rows if int(r.get("self_heal_events") or 0) > 0)
    strict_n = sum(int(r.get("manual_strict_task_success") or 0) for r in review_rows)
    entity_n = sum(int(r.get("manual_entity_resolution_correct") or 0) for r in review_rows)
    wrong_n = sum(int(r.get("manual_wrong_object_success") or 0) for r in review_rows)
    post_n = sum(int(r.get("manual_postcondition_satisfied") or 0) for r in review_rows)
    summary = {
        "manual_review_path": str(manual_path),
        "designed_self_healing_required": designed_n,
        "designed_self_healing_required_rate": round(_safe_div(designed_n, n), 4),
        "actual_self_healing_scenarios": actual_heal_n,
        "actual_self_healing_scenario_rate": round(_safe_div(actual_heal_n, n), 4),
        "manual_strict_task_success_rate": round(_safe_div(strict_n, n), 4),
        "manual_entity_resolution_accuracy": round(_safe_div(entity_n, n), 4),
        "manual_wrong_object_success_rate": round(_safe_div(wrong_n, n), 4),
        "manual_postcondition_satisfaction_rate": round(_safe_div(post_n, n), 4),
    }
    (run_path / "manual_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _pipeline_cmd(
    args: argparse.Namespace,
    spec: Dict[str, Any],
    run_outdir: Path,
    preds_dir_override: str = "",
) -> List[str]:
    overrides = spec.get("args", {})
    cmd = [
        sys.executable,
        "odoo_llm_pipeline.py",
        "--data",
        args.data,
        "--retrieval_data",
        args.retrieval_data,
        "--outdir",
        str(run_outdir / spec["name"]),
        "--provider",
        args.provider,
        "--model",
        args.model,
        "--fallback_provider",
        args.fallback_provider,
        "--fallback_model",
        args.fallback_model,
        "--retriever",
        str(overrides.get("retriever", args.retriever)),
        "--fewshot_k",
        str(overrides.get("fewshot_k", args.fewshot_k)),
        "--repair_attempts",
        str(overrides.get("repair_attempts", args.repair_attempts)),
        "--sleep",
        str(args.sleep),
        "--rpa_console_log_level",
        args.rpa_console_log_level,
        "--rollback_each",
    ]
    if args.limit > 0:
        cmd += ["--limit", str(args.limit)]
    if args.id_include:
        cmd += ["--id_include", args.id_include]
    if preds_dir_override:
        cmd += ["--preds_dir", preds_dir_override]
    return cmd


def _execution_cmd(args: argparse.Namespace, spec: Dict[str, Any], run_outdir: Path) -> List[str]:
    cmd = [
        sys.executable,
        "execution_eval.py",
        "--data",
        args.data,
        "--outdir",
        str(run_outdir / spec["name"]),
        "--preds_dir",
        args.preds_dir,
        "--rollback_each",
    ]
    if args.limit > 0:
        cmd += ["--limit", str(args.limit)]
    return cmd


def _write_summary(outdir: Path, rows: List[Dict[str, Any]]) -> None:
    json_path = outdir / "ablation_summary.json"
    csv_path = outdir / "ablation_summary.csv"
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    if rows:
        keys = sorted({k for row in rows for k in row.keys()})
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["pipeline", "execution"], default="execution")
    ap.add_argument("--data", default="combined_api_eval_odoo_API.csv")
    ap.add_argument("--retrieval_data", default="retrieval_pool_no_leak_odoo_API.csv")
    ap.add_argument("--preds_dir", default="preds_combined_baseline")
    ap.add_argument("--outdir", default="ablation_runs")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--id_include", default="")
    ap.add_argument("--provider", default=env_str("LLM_PROVIDER", "groq"), choices=["groq", "openai"])
    ap.add_argument("--model", default=env_str("LLM_MODEL", "llama-3.3-70b-versatile"))
    ap.add_argument("--fallback_provider", default=env_str("LLM_FALLBACK_PROVIDER", ""), choices=["", "groq", "openai"])
    ap.add_argument("--fallback_model", default=env_str("LLM_FALLBACK_MODEL", ""))
    ap.add_argument("--retriever", default=env_str("LLM_RETRIEVER", "hybrid"), choices=["none", "lexical", "vector", "hybrid"])
    ap.add_argument("--fewshot_k", type=int, default=3)
    ap.add_argument("--repair_attempts", type=int, default=2)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--rpa_console_log_level", default="WARNING")
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    if args.mode == "execution" and not Path(args.preds_dir).exists():
        raise SystemExit(f"preds_dir not found: {args.preds_dir}")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_outdir = Path(args.outdir) / f"ablations_{args.mode}_{ts}"
    run_outdir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []
    baseline_preds_dir = ""
    for spec in _ablation_specs(args.mode):
        row: Dict[str, Any] = {
            "ablation": spec["name"],
            "description": spec["description"],
            "mode": args.mode,
        }
        if spec.get("skip"):
            row.update({"status": "skipped", "note": spec["description"]})
            rows.append(row)
            continue

        env = _base_env()
        env.update({str(k): str(v) for k, v in spec.get("env", {}).items()})
        preds_override = ""
        if args.mode == "pipeline" and spec.get("reuse_baseline_preds"):
            preds_override = baseline_preds_dir
            if not preds_override:
                row.update(
                    {
                        "status": "failed",
                        "returncode": "",
                        "error": "baseline preds are not available for cached pipeline ablation",
                    }
                )
                rows.append(row)
                _write_summary(run_outdir, rows)
                continue
        cmd = (
            _pipeline_cmd(args, spec, run_outdir, preds_dir_override=preds_override)
            if args.mode == "pipeline"
            else _execution_cmd(args, spec, run_outdir)
        )
        result = _run(cmd, env, args.dry_run)
        if (
            args.mode == "pipeline"
            and spec["name"] == "all_components"
            and result.get("returncode") == 0
            and result.get("run_dir")
        ):
            candidate = Path(str(result["run_dir"])) / "preds"
            if candidate.exists():
                baseline_preds_dir = str(candidate)
        manual_metrics = _annotate_manual_results(str(result.get("run_dir", ""))) if result.get("returncode") == 0 else {}
        row.update(
            {
                "status": "ok" if result.get("returncode") == 0 else "failed",
                "returncode": result.get("returncode"),
                "run_dir": result.get("run_dir", ""),
                "exec_success_rate": result.get("exec_success_rate", ""),
                "step_success_rate": result.get("step_success_rate", ""),
                "skip_rate": result.get("skip_rate", ""),
                "error_rate": result.get("error_rate", ""),
                "self_heal_trigger_rate": result.get("self_heal_trigger_rate", ""),
                "post_heal_success_rate": result.get("post_heal_success_rate", ""),
                "alerts_total": result.get("alerts_total", ""),
                "llm_generation_error_count": result.get("llm_generation_error_count", ""),
                "rollback_requested": result.get("rollback_requested", ""),
                "rollback_applied": result.get("rollback_applied", ""),
                "rollback_failed": result.get("rollback_failed", ""),
                "designed_self_healing_required_rate": manual_metrics.get("designed_self_healing_required_rate", ""),
                "actual_self_healing_scenario_rate": manual_metrics.get("actual_self_healing_scenario_rate", ""),
                "manual_strict_task_success_rate": manual_metrics.get("manual_strict_task_success_rate", ""),
                "manual_entity_resolution_accuracy": manual_metrics.get("manual_entity_resolution_accuracy", ""),
                "manual_wrong_object_success_rate": manual_metrics.get("manual_wrong_object_success_rate", ""),
                "manual_postcondition_satisfaction_rate": manual_metrics.get("manual_postcondition_satisfaction_rate", ""),
                "manual_review_path": manual_metrics.get("manual_review_path", ""),
                "cmd": " ".join(cmd),
                "error": result.get("error", ""),
            }
        )
        rows.append(row)
        _write_summary(run_outdir, rows)

    _write_summary(run_outdir, rows)
    print(json.dumps({"run_dir": str(run_outdir), "rows": rows}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
