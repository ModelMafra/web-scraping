from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from typing import Any
from urllib.parse import urlparse
import webbrowser

from idealista_ericeira_scraper.core import (
    filter_output_record,
    list_output_field_specs,
    load_config,
    load_output_selection,
    read_last_jsonl_record,
    save_output_selection,
)
from idealista_ericeira_scraper.scraper import IdealistaCrawler

UI_HTML = """<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>idealista-ericeira UI</title>
  <style>
    :root {
      --sand: #efe5d2;
      --paper: #f8f4eb;
      --ink: #1f2a30;
      --muted: #5e6f73;
      --ocean: #0e6776;
      --ocean-soft: #d8eef0;
      --line: rgba(17, 37, 44, 0.14);
      --card: rgba(255, 255, 255, 0.75);
      --shadow: 0 18px 45px rgba(17, 37, 44, 0.12);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(255,255,255,0.65), transparent 30%),
        linear-gradient(180deg, #dceff0 0%, var(--sand) 52%, #f6efe1 100%);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
    }

    .shell {
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 18px 48px;
    }

    .hero {
      padding: 24px 24px 18px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: linear-gradient(135deg, rgba(255,255,255,0.82), rgba(216,238,240,0.68));
      box-shadow: var(--shadow);
      margin-bottom: 18px;
    }

    .eyebrow {
      margin: 0 0 8px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
      color: var(--ocean);
      font-weight: 700;
    }

    h1, h2, h3 {
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      letter-spacing: -0.02em;
      margin: 0 0 10px;
    }

    h1 { font-size: clamp(32px, 5vw, 52px); line-height: 1.02; max-width: 11ch; }
    h2 { font-size: 24px; }
    h3 { font-size: 17px; }

    p { margin: 0; line-height: 1.55; }
    .muted { color: var(--muted); }

    .panel {
      border: 1px solid var(--line);
      border-radius: 22px;
      background: var(--card);
      backdrop-filter: blur(10px);
      box-shadow: var(--shadow);
      padding: 18px;
      margin-bottom: 18px;
    }

    .split {
      display: grid;
      grid-template-columns: 1.05fr 0.95fr;
      gap: 18px;
    }

    .status-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }

    .metric {
      padding: 14px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.72);
      border: 1px solid var(--line);
    }

    .metric strong {
      display: block;
      font-size: 24px;
      margin-top: 4px;
    }

    .panel-header,
    .toolbar,
    .stack {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }

    .stack { justify-content: flex-start; }

    .source-list,
    .target-list,
    .field-grid {
      display: grid;
      gap: 12px;
      margin-top: 14px;
    }

    .source-list { grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); }
    .target-list { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
    .field-grid { grid-template-columns: repeat(auto-fit, minmax(245px, 1fr)); }

    .tile,
    .field-card,
    .target-card {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.82);
      padding: 14px;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 5px 10px;
      background: var(--ocean-soft);
      color: var(--ocean);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.07em;
    }

    .field-card label,
    .target-card label {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      cursor: pointer;
    }

    .field-meta {
      display: grid;
      gap: 8px;
    }

    .field-source {
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      padding: 8px 10px;
      border-radius: 12px;
      background: rgba(14, 103, 118, 0.08);
      color: #124550;
      overflow-wrap: anywhere;
    }

    .sample {
      font-size: 12px;
      color: var(--muted);
      background: rgba(17, 37, 44, 0.05);
      padding: 8px 10px;
      border-radius: 12px;
      overflow-wrap: anywhere;
    }

    input[type="checkbox"] {
      inline-size: 18px;
      block-size: 18px;
      accent-color: var(--ocean);
      margin-top: 2px;
      flex: none;
    }

    .controls {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }

    .control {
      display: grid;
      gap: 6px;
    }

    .control label {
      font-size: 13px;
      font-weight: 700;
      color: var(--muted);
    }

    input[type="number"],
    select {
      width: 100%;
      border-radius: 12px;
      border: 1px solid var(--line);
      padding: 11px 12px;
      font: inherit;
      color: var(--ink);
      background: rgba(255, 255, 255, 0.92);
    }

    button {
      border: 0;
      border-radius: 999px;
      padding: 10px 14px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      color: white;
      background: var(--ocean);
      transition: transform 120ms ease, opacity 120ms ease;
    }

    button.secondary {
      background: rgba(17, 37, 44, 0.14);
      color: var(--ink);
    }

    button:hover { transform: translateY(-1px); }
    button:active { transform: translateY(0); }

    pre {
      margin: 12px 0 0;
      border-radius: 18px;
      padding: 14px;
      background: #11252c;
      color: #f3f6f6;
      overflow: auto;
      min-height: 120px;
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      line-height: 1.55;
    }

    .footer-note {
      margin-top: 12px;
      color: var(--muted);
      font-size: 13px;
    }

    @media (max-width: 900px) {
      .split {
        grid-template-columns: 1fr;
      }

      .shell {
        padding-inline: 14px;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <p class="eyebrow">Idealista / Ericeira</p>
      <h1>Escolher o que guardar do scraping</h1>
      <p class="muted">Os campos abaixo foram definidos a partir dos snapshots HTML reais guardados da pagina de detalhe. A UI serve para escolher o output do teu JSONL sem andares a editar o parser.</p>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2>Estado atual</h2>
          <p class="muted">Resumo do projeto e da selecao ativa.</p>
        </div>
        <button id="refreshBtn" class="secondary">Atualizar</button>
      </div>
      <div id="statusGrid" class="status-grid"></div>
    </section>

    <section class="panel split">
      <div>
        <h2>Campos confirmados na pagina</h2>
        <p class="muted">Cada cartao diz de onde o scraper esta a ler o valor no HTML real.</p>
        <div id="sourceFields" class="source-list"></div>
      </div>
      <div>
        <h2>Comando sugerido</h2>
        <p class="muted">A UI guarda a selecao em ficheiro. O scraping continua a ser corrido pelo CLI.</p>
        <div class="controls">
          <div class="control">
            <label for="actionSelect">Acao</label>
            <select id="actionSelect">
              <option value="crawl">crawl</option>
              <option value="discover">discover</option>
              <option value="extract">extract</option>
            </select>
          </div>
          <div class="control">
            <label for="pagesInput">Max pages</label>
            <input id="pagesInput" type="number" min="1" value="2">
          </div>
          <div class="control">
            <label for="limitInput">Limit</label>
            <input id="limitInput" type="number" min="1" value="10">
          </div>
          <div class="control">
            <label for="modeSelect">Modo</label>
            <select id="modeSelect">
              <option value="">default</option>
              <option value="stealth">stealth</option>
              <option value="dynamic">dynamic</option>
              <option value="http">http</option>
            </select>
          </div>
        </div>
        <div id="targetList" class="target-list"></div>
        <div class="stack" style="margin-top: 14px;">
          <button id="runSelectedBtn">Correr agora</button>
          <button id="extractNowBtn" class="secondary">Extrair 10</button>
          <button id="discoverNowBtn" class="secondary">Descobrir 1 pagina</button>
        </div>
        <pre id="commandPreview"></pre>
        <p class="footer-note">A selecao de campos vai para <span id="selectionPath"></span>.</p>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2>Campos a guardar no JSONL</h2>
          <p class="muted">Os campos bloqueados ficam sempre presentes para manter rastreabilidade e retoma.</p>
        </div>
        <div class="stack">
          <button id="recommendedBtn" class="secondary">Recomendado</button>
          <button id="minimalBtn" class="secondary">Minimo</button>
          <button id="saveBtn">Guardar selecao</button>
        </div>
      </div>
      <div id="fieldGrid" class="field-grid"></div>
      <p id="saveState" class="footer-note"></p>
    </section>

    <section class="panel split">
      <div>
        <h2>Preview do ultimo registo</h2>
        <p class="muted">Vista rapida do output depois do filtro atual.</p>
        <pre id="recordPreview"></pre>
      </div>
      <div>
        <h2>Selecao atual</h2>
        <p class="muted">Lista ordenada de campos que vao sair no detalhe.</p>
        <pre id="selectionPreview"></pre>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2>Resultado da ultima acao</h2>
          <p class="muted">Se correres o scraping pela UI, o resultado aparece aqui.</p>
        </div>
      </div>
      <pre id="runOutput">Ainda nao foi corrido nenhum comando pela UI.</pre>
    </section>
  </main>

  <script>
    let state = null;
    let staticBound = false;

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function formatValue(value) {
      if (value === null || value === undefined) {
        return "sem exemplo guardado";
      }
      if (typeof value === "string") {
        return value;
      }
      return JSON.stringify(value);
    }

    function selectedFields() {
      return state.fields
        .filter((field) => field.required || document.getElementById(`field-${field.name}`)?.checked)
        .map((field) => field.name);
    }

    function selectedTargets() {
      return state.targets
        .filter((target) => document.getElementById(`target-${target.name}`)?.checked)
        .map((target) => target.name);
    }

    function previewRecord(record, fields) {
      if (!record) {
        return { note: "Ainda nao ha detalhe extraido para preview." };
      }
      const output = {};
      for (const field of fields) {
        output[field] = Object.prototype.hasOwnProperty.call(record, field) ? record[field] : null;
      }
      return output;
    }

    function renderStatus() {
      const status = state.status;
      const items = [
        ["Indexados", status.indexed_listings],
        ["Completos", status.completed_listings],
        ["Pendentes", status.pending_listings],
        ["Paginas", status.discovered_pages],
        ["Modo", status.fetch_mode],
        ["Campos", state.selection.selected_fields.length],
      ];
      document.getElementById("statusGrid").innerHTML = items.map(([label, value]) => `
        <article class="metric">
          <span class="muted">${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </article>
      `).join("");
      document.getElementById("selectionPath").textContent = state.selection_file;
    }

    function renderTargets() {
      document.getElementById("targetList").innerHTML = state.targets.map((target) => `
        <article class="target-card">
          <label>
            <input id="target-${target.name}" type="checkbox" checked>
            <span class="field-meta">
              <strong>${escapeHtml(target.name)}</strong>
              <span class="sample">${escapeHtml(target.listing_type)} / ${escapeHtml(target.property_scope)}</span>
              <span class="field-source">${escapeHtml(target.search_url)}</span>
            </span>
          </label>
        </article>
      `).join("");
    }

    function renderSources() {
      document.getElementById("sourceFields").innerHTML = state.fields
        .filter((field) => !["identificacao", "origem"].includes(field.group))
        .map((field) => `
          <article class="tile">
            <span class="pill">${escapeHtml(field.group)}</span>
            <h3>${escapeHtml(field.label)}</h3>
            <p class="muted">${escapeHtml(field.description)}</p>
            <div class="field-source">${escapeHtml(field.source_hint)}</div>
            <div class="sample">${escapeHtml(formatValue(field.sample_preview))}</div>
          </article>
        `).join("");
    }

    function renderFieldGrid() {
      const selected = new Set(state.selection.selected_fields);
      document.getElementById("fieldGrid").innerHTML = state.fields.map((field) => `
        <article class="field-card">
          <label>
            <input
              id="field-${field.name}"
              type="checkbox"
              ${selected.has(field.name) ? "checked" : ""}
              ${field.required ? "disabled" : ""}
            >
            <span class="field-meta">
              <span class="stack">
                <strong>${escapeHtml(field.label)}</strong>
                <span class="pill">${escapeHtml(field.group)}</span>
                ${field.required ? '<span class="pill">fixo</span>' : ""}
              </span>
              <span class="muted">${escapeHtml(field.description)}</span>
              <span class="field-source">${escapeHtml(field.source_hint)}</span>
              <span class="sample">${escapeHtml(formatValue(field.sample_preview))}</span>
            </span>
          </label>
        </article>
      `).join("");
    }

    function buildCommand() {
      const action = document.getElementById("actionSelect").value;
      const maxPages = document.getElementById("pagesInput").value;
      const limit = document.getElementById("limitInput").value;
      const mode = document.getElementById("modeSelect").value;
      const targets = selectedTargets();
      const parts = ["idealista-ericeira", action];
      for (const target of targets) {
        parts.push("--target", target);
      }
      if (mode) {
        parts.push("--mode", mode);
      }
      if (action !== "extract" && maxPages) {
        parts.push("--max-pages", maxPages);
      }
      if (action !== "discover" && limit) {
        parts.push("--limit", limit);
      }
      return parts.join(" ");
    }

    function refreshPreviews() {
      const fields = selectedFields();
      document.getElementById("commandPreview").textContent = buildCommand();
      document.getElementById("recordPreview").textContent = JSON.stringify(previewRecord(state.latest_record, fields), null, 2);
      document.getElementById("selectionPreview").textContent = JSON.stringify({
        selected_fields: fields,
        detail_output: state.detail_output,
      }, null, 2);
    }

    async function runAction(actionOverride) {
      const action = actionOverride || document.getElementById("actionSelect").value;
      const payload = {
        action,
        target_names: selectedTargets(),
        mode: document.getElementById("modeSelect").value || null,
        max_pages: document.getElementById("pagesInput").value || null,
        limit: document.getElementById("limitInput").value || null,
      };
      document.getElementById("runOutput").textContent = "A correr... isto pode demorar alguns segundos.";
      const response = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const result = await response.json();
      document.getElementById("runOutput").textContent = JSON.stringify(result, null, 2);
      state = await (await fetch("/api/ui")).json();
      render();
      document.getElementById("runOutput").textContent = JSON.stringify(result, null, 2);
    }

    function bindEvents() {
      if (!staticBound) {
        document.getElementById("refreshBtn").addEventListener("click", loadUi);
        document.getElementById("recommendedBtn").addEventListener("click", () => {
          for (const field of state.fields) {
            const input = document.getElementById(`field-${field.name}`);
            if (!input || field.required) {
              continue;
            }
            input.checked = Boolean(field.default);
          }
          refreshPreviews();
        });
        document.getElementById("minimalBtn").addEventListener("click", () => {
          for (const field of state.fields) {
            const input = document.getElementById(`field-${field.name}`);
            if (!input || field.required) {
              continue;
            }
            input.checked = false;
          }
          refreshPreviews();
        });
        document.getElementById("saveBtn").addEventListener("click", saveSelection);
        document.getElementById("runSelectedBtn").addEventListener("click", () => runAction());
        document.getElementById("extractNowBtn").addEventListener("click", () => {
          document.getElementById("actionSelect").value = "extract";
          document.getElementById("limitInput").value = "10";
          refreshPreviews();
          runAction("extract");
        });
        document.getElementById("discoverNowBtn").addEventListener("click", () => {
          document.getElementById("actionSelect").value = "discover";
          document.getElementById("pagesInput").value = "1";
          refreshPreviews();
          runAction("discover");
        });
        for (const element of document.querySelectorAll("#actionSelect, #pagesInput, #limitInput, #modeSelect")) {
          element.addEventListener("input", refreshPreviews);
          element.addEventListener("change", refreshPreviews);
        }
        staticBound = true;
      }
      for (const element of document.querySelectorAll("#fieldGrid input, #targetList input")) {
        element.addEventListener("input", refreshPreviews);
        element.addEventListener("change", refreshPreviews);
      }
    }

    async function saveSelection() {
      document.getElementById("saveState").textContent = "A guardar...";
      const response = await fetch("/api/selection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ selected_fields: selectedFields() }),
      });
      state = await response.json();
      render();
      document.getElementById("saveState").textContent = "Selecao guardada.";
    }

    function render() {
      renderStatus();
      renderTargets();
      renderSources();
      renderFieldGrid();
      bindEvents();
      refreshPreviews();
    }

    async function loadUi() {
      const response = await fetch("/api/ui");
      state = await response.json();
      render();
    }

    loadUi();
  </script>
</body>
</html>
"""


def _preview_value(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 180:
        return value[:177] + "..."
    if isinstance(value, list):
        preview = value[:3]
        if len(value) > 3:
            preview = [*preview, f"... ({len(value)} total)"]
        return preview
    if isinstance(value, dict):
        preview = dict(list(value.items())[:4])
        if len(value) > 4:
            preview["_extra"] = f"... ({len(value)} keys)"
        return preview
    return value


def _ui_payload(config_path: str | None = None) -> dict[str, Any]:
    crawler = IdealistaCrawler(config_path=config_path)
    selection = load_output_selection(crawler.paths.selection_file)
    latest_record = read_last_jsonl_record(crawler.paths.details_output)
    fields = []
    for spec in list_output_field_specs():
        sample_value = None if latest_record is None else latest_record.get(spec["name"])
        fields.append({**spec, "sample_preview": _preview_value(sample_value)})

    return {
        "config_file": str(crawler.paths.config_file),
        "detail_output": str(crawler.paths.details_output),
        "fields": fields,
        "latest_record": filter_output_record(latest_record, selection["selected_fields"]) if latest_record else None,
        "project_root": str(crawler.paths.root),
        "selection": selection,
        "selection_file": str(crawler.paths.selection_file),
        "status": crawler.status(),
        "targets": [
            {
                "listing_type": target.listing_type,
                "name": target.name,
                "property_scope": target.property_scope,
                "search_url": target.search_url,
            }
            for target in crawler.config.targets
        ],
    }


def _run_action(payload: dict[str, Any], config_path: str | None = None) -> dict[str, Any]:
    action = str(payload.get("action") or "extract")
    target_names = payload.get("target_names") or None
    mode_override = payload.get("mode") or None
    max_pages = payload.get("max_pages")
    limit = payload.get("limit")

    def _as_int(value: Any) -> int | None:
        if value in (None, "", 0, "0"):
            return None
        return int(value)

    crawler = IdealistaCrawler(config_path=config_path, mode_override=mode_override)
    if action == "discover":
        result = crawler.discover(target_names=target_names, max_pages=_as_int(max_pages))
    elif action == "crawl":
        result = crawler.crawl(
            target_names=target_names,
            max_pages=_as_int(max_pages),
            limit=_as_int(limit),
        )
    elif action == "extract":
        result = crawler.extract(target_names=target_names, limit=_as_int(limit))
    else:
        raise ValueError(f"Acao invalida: {action}")

    return {
        "action": action,
        "mode": crawler.config.fetch.mode,
        "result": result,
        "status_after": crawler.status(),
        "target_names": target_names or [target.name for target in crawler.config.targets],
    }


def _handler_factory(config_path: str | None = None):
    class UiHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            route = urlparse(self.path).path
            if route == "/":
                self._send_html(UI_HTML)
                return
            if route == "/api/ui":
                self._send_json(_ui_payload(config_path))
                return
            self.send_error(404, "Rota nao encontrada.")

        def do_POST(self) -> None:
            route = urlparse(self.path).path
            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                self.send_error(400, "JSON invalido.")
                return

            if route == "/api/selection":
                _, paths = load_config(config_path)
                save_output_selection(paths.selection_file, payload.get("selected_fields"))
                self._send_json(_ui_payload(config_path))
                return

            if route == "/api/run":
                try:
                    self._send_json(_run_action(payload, config_path))
                except Exception as exc:
                    self._send_json({"error": str(exc), "ok": False}, status=500)
                return

            self.send_error(404, "Rota nao encontrada.")

        def log_message(self, format: str, *args) -> None:
            return

        def _send_html(self, body: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _send_json(self, body: dict[str, Any], status: int = 200) -> None:
            payload = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return UiHandler


def serve_ui(
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    config_path: str | None = None,
    open_browser: bool = True,
) -> None:
    server = ThreadingHTTPServer((host, port), _handler_factory(config_path))
    url = f"http://{host}:{port}/"
    print(f"[ui] Frontend disponivel em {url}", flush=True)
    print("[ui] Usa Ctrl+C para fechar.", flush=True)
    if open_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
