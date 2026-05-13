const state = {
  preview: null,
  contexts: [],
  rollbackActions: [],
  rollbackPreviewLoaded: false,
};

const $ = (id) => document.getElementById(id);

const KIND_LABELS = {
  deal_reference: "сделка",
  stage_name: "стадия",
  salesperson: "менеджер",
  contact_lookup: "контакт",
  deal_create_duplicate: "дубликат",
};

const TYPE_LABELS = {
  delete: "удаление",
  write_restore: "восстановление",
  delete_file: "удаление файла",
};

function toast(message) {
  const el = $("toast");
  el.textContent = message;
  el.classList.remove("hidden");
  setTimeout(() => el.classList.add("hidden"), 4200);
}

function badge(el, text, cls = "") {
  el.textContent = text;
  el.className = `badge ${cls}`.trim();
}

function setRefreshBusy(isBusy) {
  ["refreshContexts", "refreshRollbackContexts"].forEach((id) => {
    const el = $(id);
    if (el) el.disabled = isBusy;
  });
}

async function api(path, body = null) {
  const init = body
    ? {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }
    : {};
  const res = await fetch(path, init);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || data.error || `${res.status} ${res.statusText}`);
  }
  return data;
}

function confidenceText(value) {
  if (value === undefined || value === null || value === "") return "";
  const num = Number(value);
  if (Number.isNaN(num)) return "";
  return `, уверенность ${num.toFixed(2)}`;
}

function renderPlan(plan) {
  const box = $("planList");
  if (!plan || !plan.length) {
    box.className = "list empty";
    box.textContent = "План еще не построен";
    return;
  }
  box.className = "list";
  box.innerHTML = plan
    .map(
      (step, index) => `
        <div class="item">
          <div class="item-title">
            <span>${index + 1}. ${escapeHtml(step.summary || step.op_label || step.op)}</span>
            <span class="badge">${escapeHtml(step.op_label || step.op || "")}</span>
          </div>
          <div class="item-sub">Шаг: ${escapeHtml(step.step_id || "")}</div>
        </div>
      `,
    )
    .join("");
}

function renderConfirmations(confirmations) {
  const box = $("confirmList");
  badge($("confirmBadge"), String(confirmations?.length || 0), confirmations?.length ? "warn" : "");
  if (!confirmations || !confirmations.length) {
    box.className = "list empty";
    box.textContent = "Нет рискованных исправлений. Можно выполнять без ручного выбора.";
    return;
  }
  box.className = "list";
  box.innerHTML = confirmations
    .map((conf) => {
      const options = (conf.options || [])
        .map((opt) => {
          const checked = opt.id === conf.default_option_id ? "checked" : "";
          return `
            <label class="choice">
              <input type="radio" name="decision_${escapeAttr(conf.id)}" value="${escapeAttr(opt.id)}" ${checked} />
              <span>
                <strong>${escapeHtml(opt.label || opt.action)}</strong>
                <small>${escapeHtml(confidenceText(opt.confidence))}</small>
              </span>
            </label>
          `;
        })
        .join("");
      return `
        <div class="item">
          <div class="item-title">
            <span>${escapeHtml(conf.message || "Нужно уточнение")}</span>
            <span class="badge warn">${escapeHtml(KIND_LABELS[conf.kind] || conf.kind || "")}</span>
          </div>
          <div class="item-sub">Шаг: ${escapeHtml(conf.step_id || "")}. Исходное значение: ${escapeHtml(conf.original || "")}</div>
          <div class="confirm-options">${options}</div>
        </div>
      `;
    })
    .join("");
}

function renderResult(data) {
  const box = $("resultBox");
  box.className = "result";
  const summary = data?.summary || {};
  const traces = data?.step_traces || [];
  const heals = data?.self_heal_events || [];
  const alerts = data?.alerts || [];
  const manualConfirmations = data?.manual_confirmations || [];
  const autoSelfHealing = Number(summary.self_heal_events || 0);
  const manualSelfHealing = manualConfirmations.length;
  const totalSelfHealing = autoSelfHealing + manualSelfHealing;
  badge($("resultBadge"), summary.scenario_success ? "успешно" : "нужна проверка", summary.scenario_success ? "" : "warn");
  box.innerHTML = `
    <div class="item">
      <div class="item-title"><span>Итог выполнения</span><span class="badge">${escapeHtml(String(data.duration_ms || 0))} мс</span></div>
      <div class="item-sub">
        шагов: ${summary.steps_total || 0}, успешно: ${summary.steps_success || 0}, пропущено: ${summary.steps_skipped || 0},
        ошибок: ${summary.steps_error || 0}, self-healing всего: ${totalSelfHealing}
        (авто: ${autoSelfHealing}, ручные уточнения: ${manualSelfHealing}), предупреждений: ${summary.alerts_total || 0}
      </div>
    </div>
    ${manualConfirmations
      .map(
        (m) => `
        <div class="item">
          <div class="item-title"><span>Self-healing уточнение: ${escapeHtml(KIND_LABELS[m.kind] || m.kind || "")}</span><span class="badge warn">выбрано вручную</span></div>
          <div class="item-sub">${escapeHtml(m.original || "")} -> ${escapeHtml(m.selected_label || "")}</div>
        </div>`,
      )
      .join("")}
    ${heals
      .map(
        (h) => `
        <div class="item">
          <div class="item-title"><span>${escapeHtml(KIND_LABELS[h.role] || h.role || "self-healing")}</span><span class="badge ${h.risk === "risky" ? "warn" : ""}">${escapeHtml(h.risk || "safe")}</span></div>
          <div class="item-sub">${escapeHtml(h.original)} -> ${escapeHtml(h.healed)}. ${escapeHtml(h.details || "")}</div>
        </div>`,
      )
      .join("")}
    ${alerts
      .map(
        (a) => `
        <div class="item">
          <div class="item-title"><span>${escapeHtml(a.message || "предупреждение")}</span><span class="badge err">${escapeHtml(a.severity || "")}</span></div>
          <div class="item-sub">${escapeHtml(a.details || "")}</div>
        </div>`,
      )
      .join("")}
    <pre>${escapeHtml(JSON.stringify(traces, null, 2))}</pre>
  `;
}

function invalidatePreview() {
  if (!state.preview) return;
  state.preview = null;
  $("dslBox").textContent = "";
  renderPlan([]);
  renderConfirmations([]);
  badge($("validationBadge"), "нужно обновить", "warn");
}

async function preview(options = {}) {
  badge($("validationBadge"), "строится", "warn");
  if (!options.keepResult) {
    badge($("resultBadge"), "ожидает");
    $("resultBox").className = "result empty";
    $("resultBox").textContent = "План строится без выполнения.";
  }
  const mode = $("inputMode").value;
  const body = {
    nl_text: mode === "nl" ? $("nlText").value : "",
    yaml_text: mode === "yaml" ? $("yamlText").value : "",
    provider: $("provider").value.trim(),
    model: $("model").value.trim(),
    fallback_provider: $("fallbackProvider").value.trim(),
    fallback_model: $("fallbackModel").value.trim(),
  };
  const data = await api("/api/preview", body);
  state.preview = data;
  $("dslBox").textContent = data.yaml_text || "";
  renderPlan(data.plan || []);
  renderConfirmations(data.confirmations || []);
  const ok = data.validation?.contract_ok === 1 || data.validation?.schema_ok === 1;
  badge($("validationBadge"), ok ? "DSL валиден" : "проверь DSL", ok ? "" : "warn");
  if (!options.keepResult) {
    $("resultBox").textContent = "План построен без выполнения. Выбери уточнения и нажми «Выполнить».";
  }
  if (!data.odoo_available) toast(data.odoo_error || "Odoo недоступен");
  if (!options.silent) {
    const n = data.confirmations?.length || 0;
    toast(n ? `План готов, найдено уточнений self-healing: ${n}` : "План готов, рискованных уточнений нет");
  }
}

function currentDecisions() {
  const out = {};
  for (const conf of state.preview?.confirmations || []) {
    const checked = document.querySelector(`input[name="decision_${CSS.escape(conf.id)}"]:checked`);
    if (checked) out[conf.id] = checked.value;
  }
  return out;
}

async function execute() {
  if (!state.preview) {
    await preview({ silent: true, keepResult: true });
    const n = state.preview?.confirmations?.length || 0;
    if (n) {
      const ok = window.confirm(`Найдено уточнений self-healing: ${n}. Выполнить сразу с выбранными вариантами?`);
      if (!ok) {
        badge($("resultBadge"), "ожидает");
        $("resultBox").className = "result empty";
        $("resultBox").textContent = "План построен без выполнения. Проверь уточнения и нажми «Выполнить».";
        return;
      }
    }
  }
  badge($("resultBadge"), "выполняется", "warn");
  const data = await api("/api/execute", {
    scenario: state.preview.scenario,
    confirmations: state.preview.confirmations || [],
    decisions: currentDecisions(),
  });
  renderResult(data);
  await loadContexts();
}

async function loadHealth() {
  const data = await api("/api/health");
  $("health").textContent = data.ok ? `Odoo подключен, версия ${data.odoo.server_version || ""}` : `Ошибка Odoo: ${data.error}`;
}

async function loadContexts() {
  setRefreshBusy(true);
  const box = $("contextList");
  box.className = "list compact empty";
  box.textContent = "Загружаю журналы...";
  $("rollbackList").className = "list compact empty";
  $("rollbackList").textContent = "Выберите журнал и покажите действия отката";

  try {
    const data = await api("/api/run-contexts?limit=40&active_only=1");
    const contexts = data.contexts || [];
    state.contexts = contexts;
    state.rollbackPreviewLoaded = false;
    state.rollbackActions = [];
    if (!contexts.length) {
      box.className = "list compact empty";
      box.textContent = "Нет журналов с активными действиями отката. Нажми «Обновить список» после нового запуска.";
      return;
    }
    box.className = "list compact";
    box.innerHTML = contexts
      .map((ctx) => renderContextCard(ctx))
      .join("");
  } finally {
    setRefreshBusy(false);
  }
}

function renderContextCard(ctx) {
  const details = ctx.details || {};
  const steps = (details.steps || [])
    .slice(0, 8)
    .map(
      (s) => `
        <li>
          <span>${escapeHtml(s.summary || s.op_label || s.op || "")}</span>
          <small>${escapeHtml(s.status || "")}</small>
        </li>
      `,
    )
    .join("");
  const created = Object.entries(details.created || {})
    .map(([key, value]) => `${escapeHtml(key)}: ${escapeHtml(value)}`)
    .join(", ");
  const files = (details.files || []).map((x) => escapeHtml(x)).join(", ");
  return `
    <div class="item context-card">
      <div class="context-head">
        <input type="checkbox" class="ctxCheck" value="${escapeAttr(ctx.path)}" />
        <div>
          <strong>${escapeHtml(ctx.display_name || ctx.name)}</strong>
          <div class="item-sub">${escapeHtml(ctx.summary || "")}</div>
        </div>
      </div>
      <details class="context-details">
        <summary>Раскрыть журнал</summary>
        <div class="context-meta">
          <span>Файл: ${escapeHtml(ctx.name || "")}</span>
          <span>ID: ${escapeHtml(details.scenario_id || "")}</span>
          ${details.flow ? `<span>Flow: ${escapeHtml(details.flow)}</span>` : ""}
          ${created ? `<span>Создано: ${created}</span>` : ""}
          ${files ? `<span>Файлы: ${files}</span>` : ""}
        </div>
        ${steps ? `<ol class="step-mini">${steps}</ol>` : `<div class="item-sub">Шаги не сохранены в журнале</div>`}
        <div class="item-sub">В списке сначала показано предварительное число "до N": созданные записи, файлы и изменения полей из журнала. После кнопки "Показать действия" сервис проверяет Odoo и оставляет справа только то, что реально еще можно откатить.</div>
      </details>
    </div>
  `;
}

function pluralRu(n, one, few, many) {
  const value = Math.abs(Number(n) || 0);
  const mod100 = value % 100;
  const mod10 = value % 10;
  if (mod100 >= 11 && mod100 <= 14) return `${n} ${many}`;
  if (mod10 === 1) return `${n} ${one}`;
  if (mod10 >= 2 && mod10 <= 4) return `${n} ${few}`;
  return `${n} ${many}`;
}

function exactSummary(ctx, count) {
  return `${pluralRu(ctx.steps || 0, "шаг", "шага", "шагов")}, ${pluralRu(count, "активное действие отката", "активных действия отката", "активных действий отката")}, self-healing: ${ctx.self_heal_events || 0}, предупреждений: ${ctx.alerts || 0}`;
}

function syncContextsAfterRollbackPreview(data, selectedPaths) {
  const counts = data?.source_counts || {};
  const selected = new Set(selectedPaths);
  const inactive = new Set(data?.inactive_sources || []);
  let changed = false;
  state.contexts = state.contexts
    .map((ctx) => {
      if (!(ctx.path in counts)) return ctx;
      const count = Number(counts[ctx.path] || 0);
      changed = true;
      return {
        ...ctx,
        rollback_actions: count,
        rollback_count_exact: true,
        summary: exactSummary(ctx, count),
      };
    })
    .filter((ctx) => !inactive.has(ctx.path) && Number(ctx.rollback_actions || 0) > 0);

  if (!changed) return;
  const box = $("contextList");
  if (!state.contexts.length) {
    box.className = "list compact empty";
    box.textContent = "Нет журналов с активными действиями отката. Нажми «Обновить список» после нового запуска.";
    return;
  }
  box.className = "list compact";
  box.innerHTML = state.contexts.map((ctx) => renderContextCard(ctx)).join("");
  document.querySelectorAll(".ctxCheck").forEach((input) => {
    input.checked = selected.has(input.value);
  });
}

async function previewRollback() {
  const paths = [...document.querySelectorAll(".ctxCheck:checked")].map((x) => x.value);
  if (!paths.length) {
    toast("Выбери хотя бы один журнал");
    return;
  }
  const data = await api("/api/rollback/preview", { paths });
  state.rollbackActions = data.actions || [];
  state.rollbackPreviewLoaded = true;
  syncContextsAfterRollbackPreview(data, paths);
  renderRollbackActions(state.rollbackActions);
}

function renderRollbackActions(actions) {
  const box = $("rollbackList");
  if (!actions.length) {
    box.className = "list compact empty";
    box.textContent = "Для выбранных журналов нет действий отката";
    return;
  }
  box.className = "list compact";
  box.innerHTML = actions
    .map(
      (a) => `
        <label class="item check-row">
          <input type="checkbox" class="rollbackCheck" value="${escapeAttr(a.id)}" checked />
          <span>
            <strong>${escapeHtml(a.label)}</strong><br />
            <span class="item-sub">${escapeHtml(TYPE_LABELS[a.type] || a.type)}. Журнал: ${escapeHtml(a.source_title || a.source || "")}</span>
          </span>
        </label>
      `,
    )
    .join("");
}

async function applyRollback() {
  let actions = [];
  const paths = [...document.querySelectorAll(".ctxCheck:checked")].map((x) => x.value);

  if (paths.length) {
    const data = await api("/api/rollback/preview", { paths });
    actions = data.actions || [];
    state.rollbackActions = actions;
    state.rollbackPreviewLoaded = true;
    syncContextsAfterRollbackPreview(data, paths);
    renderRollbackActions(actions);
  } else if (state.rollbackPreviewLoaded) {
    const selected = new Set([...document.querySelectorAll(".rollbackCheck:checked")].map((x) => x.value));
    actions = state.rollbackActions.filter((a) => selected.has(a.id));
  } else {
    toast("Выбери хотя бы один журнал");
    return;
  }
  if (!actions.length) {
    toast("Не выбраны действия отката");
    return;
  }
  const ok = window.confirm(`Откатить все активные действия: ${actions.length}?`);
  if (!ok) return;
  const data = await api("/api/rollback/apply", { actions });
  toast(`Откат выполнен: ${data.applied}, ошибок: ${data.failed}`);
  state.rollbackActions = [];
  state.rollbackPreviewLoaded = false;
  $("rollbackList").className = "list compact empty";
  $("rollbackList").textContent = "Откат выполнен. Журналы без активных действий скрыты из списка.";
  await loadContexts();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function escapeAttr(value) {
  return escapeHtml(value).replaceAll("'", "&#39;");
}

$("inputMode").addEventListener("change", () => {
  const yaml = $("inputMode").value === "yaml";
  $("yamlText").classList.toggle("hidden", !yaml);
  $("nlText").classList.toggle("hidden", yaml);
  invalidatePreview();
});

["nlText", "yamlText", "provider", "model", "fallbackProvider", "fallbackModel"].forEach((id) => {
  $(id).addEventListener("input", invalidatePreview);
});

$("inlinePreviewBtn").addEventListener("click", (event) => {
  event.preventDefault();
  preview().catch((e) => toast(e.message));
});
$("executeBtn").addEventListener("click", (event) => {
  event.preventDefault();
  execute().catch((e) => {
    badge($("resultBadge"), "ошибка", "err");
    toast(e.message);
  });
});
$("inlineExecuteBtn").addEventListener("click", (event) => {
  event.preventDefault();
  execute().catch((e) => {
    badge($("resultBadge"), "ошибка", "err");
    toast(e.message);
  });
});
$("refreshContexts").addEventListener("click", (event) => {
  event.preventDefault();
  loadContexts().catch((e) => toast(e.message));
});
$("refreshRollbackContexts").addEventListener("click", (event) => {
  event.preventDefault();
  loadContexts().catch((e) => {
    $("contextList").className = "list compact empty";
    $("contextList").textContent = `Не удалось загрузить журналы: ${e.message}`;
    toast(e.message);
  });
});
$("previewRollback").addEventListener("click", (event) => {
  event.preventDefault();
  previewRollback().catch((e) => toast(e.message));
});
$("applyRollback").addEventListener("click", (event) => {
  event.preventDefault();
  applyRollback().catch((e) => toast(e.message));
});

loadHealth().catch((e) => {
  $("health").textContent = e.message;
});
loadContexts().catch((e) => {
  $("contextList").className = "list compact empty";
  $("contextList").textContent = `Не удалось загрузить журналы: ${e.message}`;
});
