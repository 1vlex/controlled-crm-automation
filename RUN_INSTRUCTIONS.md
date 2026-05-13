# Инструкция запуска

Все команды выполняются из корня проекта, то есть из папки, где лежат `README.md`, `llm.py`, `odoo_rpa.py`, `rpa_service.py`.

## 1. Требования

Нужно установить:

- Docker Desktop.
- Python 3.10 или новее.
- Python-библиотеки из `requirements.txt`.
- Локальный Odoo через Docker Compose или уже запущенный Odoo, доступный по XML-RPC.

Установка Python-библиотек:

```powershell
python -m pip install -r requirements.txt
```

## 2. Настройка .env

Создать локальный `.env`:

```powershell
Copy-Item .env.example .env
```

Заполнить значения:

```text
GROQ_API_KEY=...
GROQ_API_KEYS=...
OPENAI_API_KEY=...
ODOO_URL=http://localhost:8069
ODOO_DB=...
ODOO_EMAIL=...
ODOO_PASSWORD=...
```

Если Groq ловит rate limit, pipeline может использовать fallback:

```text
LLM_FALLBACK_PROVIDER=openai
LLM_FALLBACK_MODEL=gpt-4o-mini
```

Пользовательские выгрузки по умолчанию пишутся в `artifacts`. При необходимости путь можно поменять:

```text
RPA_ARTIFACT_DIR=artifacts
```

Технические журналы `run_context_*.json` остаются в `logs`.

## 3. Запуск Odoo через Docker

Из корня проекта:

```powershell
Set-Location .\odoo_ocr_docker
docker compose up -d --build
Set-Location ..
```

Проверка:

```text
http://localhost:8069
```

Если Docker пишет, что не найден `dockerDesktopLinuxEngine`, сначала открой Docker Desktop и дождись полного запуска.

## 4. Запуск сервиса

Из корня проекта:

```powershell
python -m uvicorn rpa_service:app --host 127.0.0.1 --port 8077
```

Открыть:

```text
http://127.0.0.1:8077
```

В сервисе можно:

- ввести NL-запрос или готовый YAML DSL;
- нажать `Построить план`, чтобы получить DSL, план выполнения и self-healing уточнения без записи в Odoo;
- выбрать кандидатов self-healing или вариант создания новой сделки;
- нажать `Выполнить`, чтобы применить выбранный план;
- обновить журналы, раскрыть нужный запуск и откатить активные действия.

## 5. Файлы данных

В корне проекта должны быть:

- `combined_api_eval_odoo_API.csv` - основной eval-набор.
- `retrieval_pool_no_leak_odoo_API.csv` - отдельный retrieval-пул без утечки.
- `preds_combined_baseline/` - готовые YAML DSL для execution-абляций без повторной генерации LLM.
- `manual_review_all_scenarios.csv` - ручная semantic-разметка всех 58 сценариев.
- `manual_summary.json` - итоговые ручные метрики.

### Eval-набор

`combined_api_eval_odoo_API.csv` содержит:

- `id` - идентификатор сценария, например `U02`.
- `nl_plain` - пользовательский запрос.
- `expected_ru` - ожидаемый бизнес-результат на русском.

Пример одной строки eval-набора:

```csv
id,nl_plain,expected_ru
T01,"Найди сделку ""Global Solutons: Furnitures""; переведи в стадию Proposotion; назначь менеджера galiy.ivan2003@mail.r; только эта сделка","Найти целевую сделку, исправить опечатки в названии сделки, стадии и логине менеджера, затем применить изменения только к этой сделке."
```

Если для сценария нужен эталонный DSL, он хранится отдельно как `preds_combined_baseline/<id>.yaml`. Так eval CSV остается читаемым, а execution-прогоны могут переиспользовать готовый DSL.

### Retrieval-пул

`retrieval_pool_no_leak_odoo_API.csv` содержит:

- `id` - идентификатор retrieval-примера, например `RE101`.
- `nl_plain` - текст независимого примера.
- `dsl_yaml` - эталонный YAML DSL.
- `source_id` - независимый id, не совпадающий с eval-id.
- `lang` - язык примера.
- `variant` - вариант формулировки.

Пример retrieval-примера для ручного добавления:

```text
id: RE101
nl_plain: Use this independent CRM example: find opportunity '<deal_title>'; move it to stage '<stage_name>'; create activity '<activity_title>' due tomorrow [retrieval_example RE101]
source_id: independent_RE101
lang: en
variant: manual_independent
dsl_yaml:
  dsl: v0.3
  flow: retrieval_example_re101
  steps:
    - id: s1_find
      op: deal.search
      input:
        title: <deal_title>
    - id: s2_stage
      op: deal.update_stage
      input:
        deal:
          id: ${s1_find.deal_id}
        stage: <stage_name>
    - id: s3_activity
      op: activity.create
      input:
        deal:
          id: ${s1_find.deal_id}
        summary: <activity_title>
        date_deadline: tomorrow
```

Правила для сбора retrieval-пула:

- не копировать eval-запросы дословно;
- не использовать `Uxx` или `Sxx` из eval как `id` или `source_id`;
- менять имена сделок, email, телефоны, числа и quoted values;
- сохранять тот же DSL-контракт операций, который поддерживает исполнитель;
- проверять, что YAML из `dsl_yaml` парсится;
- вручную просматривать похожие строки, чтобы не было скрытой утечки через почти одинаковый текст.

Так как DSL и операции специфичны для этой работы, eval-набор и retrieval-пул собираются вручную. Готового публичного датасета под такой набор CRM API-сценариев нет.

## 6. Полный execution-прогон

Этот режим не тратит LLM-токены, потому что использует готовые YAML из `preds_combined_baseline`.

```powershell
python execution_eval.py `
  --data combined_api_eval_odoo_API.csv `
  --preds_dir preds_combined_baseline `
  --outdir full_eval_runs `
  --rollback_each
```

Результаты появятся в `full_eval_runs/<run_id>/`:

- `results.csv`
- `summary.json`
- `manual_review_all_scenarios.csv`
- `manual_summary.json`

`--rollback_each` нужен, чтобы сценарии не влияли друг на друга через состояние Odoo.

## 7. Execution-абляции

Этот режим сравнивает execution-уровень на одном и том же DSL:

- `all_components` - готовый DSL + self-healing.
- `no_self_healing` - тот же DSL, но self-healing выключен.

```powershell
python ablation_runner.py `
  --mode execution `
  --data combined_api_eval_odoo_API.csv `
  --preds_dir preds_combined_baseline `
  --outdir ablation_runs
```

В `ablation_summary.csv` записываются автоматические и ручные метрики:

- `exec_success_rate`
- `step_success_rate`
- `manual_strict_task_success_rate`
- `manual_entity_resolution_accuracy`
- `manual_wrong_object_success_rate`
- `manual_postcondition_satisfaction_rate`

`manual_entity_resolution_accuracy` равна 1, когда существующая целевая сущность найдена и выбрана корректно. Ошибки параметров действия, например отсутствующая стадия, учитываются в strict/postcondition, но не обязательно снижают entity-resolution. Safe-failure для отсутствующей сущности не считается entity success, но отдельно контролируется через `manual_wrong_object_success_rate`: если похожий объект не изменен, wrong-object success остается 0. U17 может иметь `entity=1` при `strict/postcondition=0`: целевая сделка выбрана корректно, но нужная стадия отсутствует.

## 8. Pipeline-абляции

Этот режим заново генерирует YAML DSL и поэтому тратит LLM-токены. Он сравнивает:

- `all_components` - retrieval + repair + self-healing.
- `no_self_healing` - тот же generated YAML, но без self-healing на execution-уровне.
- `no_repair` - без YAML repair.
- `no_retrieval` - без retrieval.

Запуск на всех 58 сценариях:

```powershell
python ablation_runner.py `
  --mode pipeline `
  --data combined_api_eval_odoo_API.csv `
  --retrieval_data retrieval_pool_no_leak_odoo_API.csv `
  --outdir ablation_runs `
  --sleep 1
```

Малый smoke-test:

```powershell
python ablation_runner.py `
  --mode pipeline `
  --data combined_api_eval_odoo_API.csv `
  --retrieval_data retrieval_pool_no_leak_odoo_API.csv `
  --outdir ablation_runs `
  --limit 10 `
  --sleep 1
```

Для `no_self_healing` runner переиспользует YAML из `all_components`, чтобы изолировать вклад self-healing и не тратить лишние LLM-токены.

## 9. Откат

Через UI:

- открыть `http://127.0.0.1:8077`;
- нажать `Обновить журналы` или `Обновить список`;
- выбрать один или несколько журналов;
- раскрыть журнал, чтобы проверить сценарий и шаги;
- нажать `Показать действия`;
- нажать `Откатить все`, если нужно откатить все активные действия выбранного запуска.

Что попадает в откат:

- созданные сделки `crm.lead`;
- созданные активности `mail.activity`;
- созданные встречи `calendar.event`;
- созданные коммерческие предложения `sale.order`;
- созданные контакты `res.partner`;
- созданные письма `mail.mail`, если включена реальная отправка через Odoo;
- изменения существующих записей через `write_restore`: стадия, вероятность, ответственный, теги, lost-поля;
- локальные файлы отчетов и уведомлений из `artifacts`.

Защита от лишнего отката:

- UI отправляет действие с привязкой к конкретному `run_context`;
- сервер заново строит допустимые действия из этого `run_context`;
- если действие не найдено в журнале, оно отклоняется;
- после успешного отката повторный preview показывает только оставшиеся active-действия;
- файлы удаляются только из безопасных локальных папок.

Rollback является компенсирующим и покрывает действия, зафиксированные в `run_context`; полная транзакционная гарантия восстановления всей базы Odoo не заявляется.

CLI-вариант:

```powershell
python odoo_rollback.py `
  --run_context logs\run_context_<ID>_<TIME>.json `
  --revert_updates `
  --apply
```
