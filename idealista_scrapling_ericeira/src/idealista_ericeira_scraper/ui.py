from __future__ import annotations

from html import escape as html_escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
from threading import Lock, Thread
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4
import webbrowser

from idealista_ericeira_scraper.core import (
    list_output_field_specs,
    load_config,
    load_output_selection,
    read_last_jsonl_record,
    save_output_selection,
    utc_now_iso,
)
from idealista_ericeira_scraper.scraper import IdealistaCrawler

RUN_JOBS: dict[str, dict[str, Any]] = {}
RUN_JOBS_LOCK = Lock()
MAX_JOB_LOG_LINES = 500

TARGET_LABELS = {
    "ericeira_buy_homes": "Comprar casas",
    "ericeira_rent_homes": "Arrendar casas",
}

TARGET_HELP = {
    "ericeira_buy_homes": "Pesquisa anuncios de compra em Ericeira.",
    "ericeira_rent_homes": "Pesquisa anuncios de arrendamento em Ericeira.",
}

UI_HTML = """<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Idealista Ericeira</title>
  <style>
    :root {
      --bg-a: #d8e7e6;
      --bg-b: #efe7d7;
      --ink: #1d262b;
      --muted: #66747a;
      --line: rgba(22, 39, 46, 0.12);
      --panel: rgba(255, 255, 255, 0.74);
      --panel-strong: rgba(255, 255, 255, 0.9);
      --accent: #0f6b72;
      --accent-soft: #d5eff0;
      --accent-deep: #0d3038;
      --warm: #ffefe1;
      --shadow: 0 28px 70px rgba(20, 34, 40, 0.16);
      --radius-xl: 30px;
      --radius-lg: 22px;
      --radius-md: 16px;
      --mono: "SFMono-Regular", Consolas, monospace;
      --body: "Avenir Next", "Segoe UI", sans-serif;
      --display: "Iowan Old Style", "Palatino Linotype", serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at 12% 18%, rgba(255, 255, 255, 0.72), transparent 20%),
        radial-gradient(circle at 85% 15%, rgba(255, 255, 255, 0.38), transparent 18%),
        linear-gradient(140deg, var(--bg-a) 0%, var(--bg-b) 55%, #f4efe2 100%);
      color: var(--ink);
      font-family: var(--body);
      padding: 12px;
      overflow: auto;
    }

    .stage {
      width: min(1760px, calc(100vw - 24px));
      min-height: calc(100vh - 24px);
      height: auto;
      aspect-ratio: auto;
      margin: 0 auto;
      border-radius: 34px;
      background: linear-gradient(160deg, rgba(255,255,255,0.5), rgba(255,255,255,0.18));
      backdrop-filter: blur(18px);
      box-shadow: var(--shadow);
      border: 1px solid rgba(255,255,255,0.38);
      padding: 18px;
      display: grid;
      grid-template-columns: 360px minmax(0, 1fr);
      grid-template-rows: auto auto auto;
      grid-template-areas:
        "hero hero"
        "sidebar activity"
        "sidebar details";
      gap: 18px;
      overflow: visible;
    }

    .panel {
      border-radius: var(--radius-xl);
      background: var(--panel);
      border: 1px solid var(--line);
      backdrop-filter: blur(10px);
      padding: 20px;
      min-width: 0;
    }

    .hero {
      grid-area: hero;
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(320px, 0.9fr);
      gap: 16px;
      align-items: center;
      padding: 18px 22px;
      background:
        linear-gradient(135deg, rgba(255,255,255,0.88), rgba(213,239,240,0.82)),
        linear-gradient(180deg, rgba(255,255,255,0.75), rgba(255,255,255,0.35));
    }

    .hero-copy {
      display: grid;
      align-content: center;
      gap: 8px;
      max-width: 700px;
      min-width: 0;
    }

    .hero-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 6px;
    }

    .nav-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 10px 14px;
      border: 1px solid rgba(15, 107, 114, 0.18);
      background: rgba(255,255,255,0.82);
      color: var(--accent-deep);
      font-weight: 800;
      text-decoration: none;
      transition: transform 120ms ease, background 120ms ease;
    }

    .nav-link:hover {
      transform: translateY(-1px);
      background: white;
    }

    .eyebrow {
      margin: 0 0 10px;
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--accent);
    }

    h1, h2, h3 {
      margin: 0;
      font-family: var(--display);
      letter-spacing: -0.03em;
    }

    h1 {
      font-size: clamp(24px, 2.5vw, 34px);
      line-height: 0.95;
      max-width: 10ch;
      margin-bottom: 4px;
    }

    h2 {
      font-size: 26px;
      line-height: 1.02;
    }

    h3 {
      font-size: 18px;
      line-height: 1.05;
    }

    p {
      margin: 0;
      line-height: 1.55;
    }

    .muted {
      color: var(--muted);
    }

    .hero-stats {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      align-self: stretch;
    }

    .stat {
      border-radius: 20px;
      padding: 12px 14px;
      background: rgba(255,255,255,0.8);
      border: 1px solid var(--line);
    }

    .stat-label {
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--muted);
    }

    .stat-value {
      font-size: 24px;
      font-weight: 800;
      letter-spacing: -0.03em;
      margin-top: 4px;
    }

    .sidebar {
      grid-area: sidebar;
      display: grid;
      grid-template-rows: auto auto auto;
      gap: 12px;
      align-content: start;
      overflow: visible;
    }

    .sidebar-block {
      display: grid;
      gap: 8px;
    }

    .target-grid,
    .action-grid,
    .field-grid {
      display: grid;
      gap: 12px;
    }

    .target-option,
    .action-card,
    .field-card,
    .mini-card {
      border-radius: var(--radius-lg);
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.86);
    }

    .target-option label,
    .field-card label {
      display: flex;
      gap: 10px;
      padding: 10px 12px;
      cursor: pointer;
    }

    .target-option input,
    .field-card input {
      margin-top: 2px;
      inline-size: 18px;
      block-size: 18px;
      accent-color: var(--accent);
      flex: none;
    }

    .target-name,
    .field-name {
      display: block;
      font-weight: 800;
      margin-bottom: 2px;
    }

    .target-help,
    .field-help,
    .button-help {
      font-size: 12px;
      color: var(--muted);
    }

    .target-option {
      border-radius: 18px;
      background: rgba(255,255,255,0.9);
    }

    .target-option label {
      align-items: center;
    }

    .target-option .target-help {
      display: none;
    }

    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }

    .select-wrap {
      display: grid;
      gap: 8px;
    }

    .select-wrap label {
      font-size: 13px;
      font-weight: 800;
      color: var(--muted);
    }

    select {
      width: 100%;
      border-radius: 16px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      background: rgba(255,255,255,0.92);
      color: var(--ink);
      font: inherit;
    }

    button {
      border: 0;
      border-radius: 999px;
      padding: 11px 16px;
      font: inherit;
      font-weight: 800;
      cursor: pointer;
      transition: transform 120ms ease, opacity 120ms ease;
    }

    button.primary {
      background: var(--accent);
      color: white;
    }

    button.secondary {
      background: rgba(15, 107, 114, 0.1);
      color: var(--accent);
    }

    button.ghost {
      background: rgba(21, 37, 44, 0.08);
      color: var(--ink);
    }

    button:hover {
      transform: translateY(-1px);
    }

    button:disabled {
      cursor: wait;
      opacity: 0.55;
      transform: none;
    }

    .action-card {
      padding: 12px 14px;
      display: grid;
      gap: 6px;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.94), rgba(255,255,255,0.78)),
        linear-gradient(120deg, rgba(255,239,225,0.8), rgba(213,239,240,0.6));
    }

    .sidebar-note {
      border-radius: 20px;
      border: 1px solid var(--line);
      background: rgba(15, 107, 114, 0.08);
      padding: 10px 12px;
      font-size: 12px;
      line-height: 1.45;
      color: var(--accent-deep);
    }

    .action-tag,
    .status-chip,
    .field-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .action-tag,
    .field-pill {
      color: var(--accent);
      background: var(--accent-soft);
    }

    .status-chip {
      background: rgba(21, 37, 44, 0.08);
      color: var(--ink);
    }

    .status-running {
      background: #fff2d9;
      color: #8b5b00;
    }

    .status-done {
      background: #d8f0e6;
      color: #1f7250;
    }

    .status-error {
      background: #ffe1e1;
      color: #993333;
    }

    .activity {
      grid-area: activity;
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      gap: 14px;
      min-height: 0;
      overflow: hidden;
      align-self: stretch;
    }

    .section-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }

    .section-copy {
      display: grid;
      gap: 6px;
    }

    .logbox {
      border-radius: 22px;
      background: linear-gradient(180deg, #0d2430, #102a35);
      color: #ecf4f5;
      padding: 16px 18px;
      font-family: var(--mono);
      font-size: 14px;
      line-height: 1.65;
      overflow: auto;
      scrollbar-gutter: stable;
      white-space: pre-wrap;
      word-break: break-word;
      min-height: 0;
      block-size: 320px;
      max-block-size: 320px;
    }

    .right-grid {
      grid-area: details;
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 18px;
      min-height: 0;
      align-content: stretch;
    }

    .right-grid > .panel {
      overflow: visible;
    }

    .latest-card,
    .state-grid {
      display: grid;
      gap: 12px;
    }

    .latest-card {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .latest-media,
    .latest-main {
      grid-column: 1 / -1;
    }

    .latest-media {
      border-radius: var(--radius-lg);
      overflow: hidden;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(13, 36, 48, 0.94), rgba(16, 42, 53, 0.98));
      block-size: 150px;
      min-block-size: 150px;
    }

    .latest-media img {
      inline-size: 100%;
      block-size: 100%;
      display: block;
      object-fit: contain;
      object-position: center;
      background: #102a35;
    }

    .latest-media-empty {
      display: grid;
      place-items: center;
      block-size: 100%;
      padding: 18px;
      color: rgba(236, 244, 245, 0.88);
      text-align: center;
      font-size: 14px;
      line-height: 1.5;
    }

    .latest-main {
      padding: 14px;
      border-radius: var(--radius-lg);
      background: rgba(255,255,255,0.92);
      border: 1px solid var(--line);
    }

    .latest-title {
      font-size: 18px;
      line-height: 1.16;
    }

    .mini-card {
      padding: 12px 14px;
    }

    .mini-label {
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 6px;
    }

    .mini-value {
      font-size: 16px;
      font-weight: 800;
      line-height: 1.22;
      word-break: break-word;
    }

    .state-grid {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }

    details {
      width: 100%;
    }

    summary {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      cursor: pointer;
      list-style: none;
      font-weight: 800;
    }

    summary::-webkit-details-marker {
      display: none;
    }

    .summary-note {
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .advanced-inline {
      min-height: 0;
      padding-top: 2px;
    }

    .advanced-grid {
      margin-top: 14px;
      display: grid;
      gap: 14px;
    }

    .field-grid {
      grid-template-columns: 1fr;
    }

    .command-box {
      border-radius: 20px;
      padding: 14px 16px;
      background: linear-gradient(180deg, #0d2430, #102a35);
      color: #ecf4f5;
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.55;
      white-space: pre-wrap;
      word-break: break-word;
    }

    .helper-stack {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }

    .empty-state {
      display: grid;
      place-items: center;
      min-height: 180px;
      color: var(--muted);
      text-align: center;
      padding: 18px;
    }

    @media (max-width: 1240px), (max-aspect-ratio: 16 / 11) {
      body {
        padding: 10px;
      }

      .stage {
        width: min(100%, 1120px);
        height: auto;
        aspect-ratio: auto;
        min-height: calc(100vh - 20px);
        grid-template-columns: 1fr;
        grid-template-rows: auto;
        grid-template-areas:
          "hero"
          "sidebar"
          "activity"
          "details";
        overflow: auto;
      }

      .hero,
      .right-grid,
      .advanced-grid,
      .latest-card,
      .state-grid {
        grid-template-columns: 1fr;
      }

      .sidebar {
        overflow: visible;
      }

      .logbox {
        block-size: 240px;
        max-block-size: 240px;
      }
    }
  </style>
</head>
<body>
  <main class="stage">
    <header class="hero panel">
      <div class="hero-copy">
        <p class="eyebrow">Idealista / Ericeira</p>
        <h1>Painel simples de scraping</h1>
        <p class="muted">Escolhe o alvo, clica num botao e acompanha tudo na caixa de atividade. <strong>Sacar tudo</strong> ja corre todas as paginas sem pedires numero.</p>
        <div class="hero-actions">
          <a class="nav-link" href="/guia">Abrir guia do projeto</a>
        </div>
      </div>
      <div id="heroStats" class="hero-stats"></div>
    </header>

    <aside class="sidebar panel">
      <section class="sidebar-block">
        <div class="section-copy">
          <h2>Escolher alvo</h2>
          <p class="muted">Escolhe compra, arrendamento ou ambos.</p>
        </div>
        <div id="targetGrid" class="target-grid"></div>
      </section>

      <section class="sidebar-block">
        <div class="section-copy">
          <h2>Acoes rapidas</h2>
          <p class="muted">Cada botao explica o que faz antes de correres.</p>
        </div>
        <div class="select-wrap">
          <label for="modeSelect">Modo do browser</label>
          <select id="modeSelect">
            <option value="">Default do projeto</option>
            <option value="stealth">Stealth</option>
            <option value="dynamic">Dynamic</option>
            <option value="http">HTTP</option>
          </select>
        </div>
        <div class="action-grid">
          <article class="action-card">
            <span class="action-tag">Mais simples</span>
            <h3>Sacar 1 pagina</h3>
            <p class="button-help">Abre a primeira pagina de resultados e grava cada anuncio logo no JSONL final, um a um.</p>
            <button id="runPageBtn" class="primary">Sacar 1 pagina</button>
          </article>
          <article class="action-card">
            <span class="action-tag">Sem meter numero</span>
            <h3>Sacar tudo</h3>
            <p class="button-help">Vai pagina a pagina ate nao haver mais resultados. Cada anuncio e guardado assim que termina.</p>
            <button id="runAllPagesBtn" class="secondary">Sacar tudo</button>
          </article>
          <article class="action-card">
            <span class="action-tag">Retomar</span>
            <h3>Continuar pendentes</h3>
            <p class="button-help">Usa o indice que ja existe e tenta sacar apenas os anuncios que ainda nao estao no output final.</p>
            <button id="runPendingBtn" class="ghost">Continuar pendentes</button>
          </article>
        </div>
        <div class="sidebar-note">
          <strong>Nota:</strong> para guardar todos os campos, o scraper abre cada anuncio individualmente. Os logs uteis aparecem na caixa da direita.
        </div>
      </section>

      <section class="sidebar-block advanced-inline">
        <details>
          <summary>
            <span>Campos guardados e opcoes avancadas</span>
            <span class="summary-note">Opcional</span>
          </summary>
          <div class="advanced-grid">
            <div class="section-copy">
              <h2>Campos do JSONL</h2>
              <p class="muted">Opcional. Ajusta aqui os campos guardados no ficheiro final.</p>
            </div>
            <div id="fieldGrid" class="field-grid"></div>
            <div class="toolbar">
              <button id="saveFieldsBtn" class="primary">Guardar campos</button>
              <button id="recommendedBtn" class="ghost">Recomendado</button>
              <button id="minimalBtn" class="ghost">Minimo</button>
            </div>
          </div>
        </details>
      </section>
    </aside>

    <section class="activity panel">
      <div class="section-head">
        <div class="section-copy">
          <h2>Atividade em direto</h2>
          <p class="muted">Os prints do scraping aparecem aqui. Ja nao tens de ir ao terminal para perceber o que esta a acontecer.</p>
        </div>
        <span id="jobStatus" class="status-chip">Pronto</span>
      </div>
      <div id="activityLog" class="logbox">A espera de uma acao. Clica em “Sacar 1 pagina” para comecar.</div>
    </section>

    <section class="right-grid">
      <article class="panel">
        <div class="section-head">
          <div class="section-copy">
            <h2>Ultimo anuncio guardado</h2>
            <p class="muted">Resumo do ultimo registo presente no JSONL final.</p>
          </div>
        </div>
        <div id="latestRecord" class="latest-card"></div>
      </article>

      <article class="panel">
        <div class="section-head">
          <div class="section-copy">
            <h2>Estado atual</h2>
            <p class="muted">Visao rapida do progresso e da configuracao ativa.</p>
          </div>
        </div>
        <div id="stateGrid" class="state-grid"></div>
      </article>
    </section>
  </main>

  <script>
    let state = null;
    let currentJobId = null;
    let currentJob = null;
    let pollTimer = null;
    let selectedTargetNames = new Set();

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function selectedTargets() {
      return Array.from(selectedTargetNames);
    }

    function selectedFields() {
      return state.fields
        .filter((field) => field.required || document.getElementById(`field-${field.name}`)?.checked)
        .map((field) => field.name);
    }

    function setActionButtonsDisabled(disabled) {
      for (const id of ["runPageBtn", "runAllPagesBtn", "runPendingBtn"]) {
        const node = document.getElementById(id);
        if (node) {
          node.disabled = disabled;
        }
      }
    }

    function renderHeroStats() {
      const status = state.status;
      const items = [
        ["Concluidos", status.completed_listings],
        ["Pendentes", status.pending_listings],
        ["Paginas", status.discovered_pages],
        ["Modo", status.fetch_mode],
      ];
      document.getElementById("heroStats").innerHTML = items.map(([label, value]) => `
        <article class="stat">
          <div class="stat-label">${escapeHtml(label)}</div>
          <div class="stat-value">${escapeHtml(value)}</div>
        </article>
      `).join("");
    }

    function renderTargets() {
      if (selectedTargetNames.size === 0) {
        state.targets.forEach((target) => selectedTargetNames.add(target.name));
      }
      document.getElementById("targetGrid").innerHTML = state.targets.map((target) => `
        <article class="target-option">
          <label>
            <input
              id="target-${target.name}"
              type="checkbox"
              ${selectedTargetNames.has(target.name) ? "checked" : ""}
            >
            <span>
              <span class="target-name">${escapeHtml(target.label)}</span>
              <span class="target-help">${escapeHtml(target.help)}</span>
            </span>
          </label>
        </article>
      `).join("");

      for (const target of state.targets) {
        const checkbox = document.getElementById(`target-${target.name}`);
        checkbox.addEventListener("change", () => {
          if (checkbox.checked) {
            selectedTargetNames.add(target.name);
          } else {
            selectedTargetNames.delete(target.name);
          }
          renderCommandBox();
        });
      }
    }

    function renderLatestRecord() {
      const record = state.latest_record;
      const root = document.getElementById("latestRecord");
      if (!record) {
        root.innerHTML = `<div class="empty-state">Ainda nao existe nenhum anuncio guardado no ficheiro final.</div>`;
        return;
      }

      const imageUrl = record.preview_image_url || "";
      const imageFrame = imageUrl
        ? `
          <article class="latest-media">
            <img
              loading="lazy"
              referrerpolicy="no-referrer"
              src="${escapeHtml(imageUrl)}"
              alt="Imagem do ultimo anuncio guardado"
            >
          </article>
        `
        : `
          <article class="latest-media">
            <div class="latest-media-empty">Este registo nao tem imagem guardada no JSONL.</div>
          </article>
        `;

      root.innerHTML = `
        ${imageFrame}
        <article class="latest-main">
          <div class="mini-label">Titulo</div>
          <div class="latest-title">${escapeHtml(record.title || "Sem titulo")}</div>
        </article>
        <article class="mini-card">
          <div class="mini-label">Preco</div>
          <div class="mini-value">${escapeHtml(record.price_text || "Sem preco")}</div>
        </article>
        <article class="mini-card">
          <div class="mini-label">Localizacao</div>
          <div class="mini-value">${escapeHtml(record.address || "Sem localizacao")}</div>
        </article>
        <article class="mini-card">
          <div class="mini-label">ID</div>
          <div class="mini-value">${escapeHtml(record.listing_id || "-")}</div>
        </article>
        <article class="mini-card">
          <div class="mini-label">Data</div>
          <div class="mini-value">${escapeHtml(record.fetched_at || "-")}</div>
        </article>
      `;
    }

    function renderStateGrid() {
      const status = state.status;
      const fields = [
        ["Indexados", status.indexed_listings],
        ["Concluidos", status.completed_listings],
        ["Pendentes", status.pending_listings],
        ["Campos ativos", state.selection.selected_fields.length],
        ["Delay", `${status.request_delay_seconds}s`],
        ["Espera browser", `${status.wait_ms} ms`],
      ];
      document.getElementById("stateGrid").innerHTML = fields.map(([label, value]) => `
        <article class="mini-card">
          <div class="mini-label">${escapeHtml(label)}</div>
          <div class="mini-value">${escapeHtml(value)}</div>
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
            <span>
              <span class="field-name">${escapeHtml(field.label)}</span>
              <span class="field-help">${escapeHtml(field.description)}</span>
            </span>
          </label>
        </article>
      `).join("");
    }

    function renderCommandBox() {
      const mode = document.getElementById("modeSelect")?.value || "";
      const targets = selectedTargets();
      const targetArgs = targets.flatMap((target) => ["--target", target]).join(" ");
      const modeArg = mode ? ` --mode ${mode}` : "";
      const commands = [
        `Sacar 1 pagina\\nidealista-ericeira page ${targetArgs}${modeArg} --max-pages 1`,
        `Sacar tudo\\nidealista-ericeira page ${targetArgs}${modeArg} --all-pages`,
        `Continuar pendentes\\nidealista-ericeira extract ${targetArgs}${modeArg}`,
      ];
      const commandBox = document.getElementById("commandBox");
      if (commandBox) {
        commandBox.textContent = commands.join("\\n\\n");
      }
    }

    function statusChipClass(job) {
      if (!job) return "status-chip";
      if (job.status === "running") return "status-chip status-running";
      if (job.status === "completed") return "status-chip status-done";
      if (job.status === "failed") return "status-chip status-error";
      return "status-chip";
    }

    function statusChipText(job) {
      if (!job) return "Pronto";
      if (job.status === "running") return "A correr";
      if (job.status === "completed") return "Concluido";
      if (job.status === "failed") return "Erro";
      return "Pronto";
    }

    function renderJob(job) {
      currentJob = job || null;
      const chip = document.getElementById("jobStatus");
      chip.className = statusChipClass(job);
      chip.textContent = statusChipText(job);

      const logBox = document.getElementById("activityLog");
      if (!job) {
        logBox.textContent = "A espera de uma acao. Clica em “Sacar 1 pagina” para comecar.";
        return;
      }

      const lines = [];
      if (job.logs && job.logs.length) {
        lines.push(...job.logs);
      }
      if (job.result) {
        lines.push("");
        lines.push("Resumo final:");
        lines.push(...jobSummaryLines(job.result));
      }
      if (job.error) {
        lines.push("");
        lines.push(`Erro: ${job.error}`);
      }
      logBox.textContent = lines.join("\\n");
      logBox.scrollTop = logBox.scrollHeight;
    }

    function jobSummaryLines(payload) {
      const lines = [];
      const result = payload.result || {};
      const statusAfter = payload.status_after || {};
      lines.push(`Acao: ${payload.action || "-"}`);
      if (payload.mode) {
        lines.push(`Modo: ${payload.mode}`);
      }
      if (typeof result.pages_done !== "undefined") {
        lines.push(`Paginas processadas: ${result.pages_done}`);
      }
      if (typeof result.saved !== "undefined") {
        lines.push(`Anuncios guardados agora: ${result.saved}`);
      }
      if (typeof result.indexed_now !== "undefined") {
        lines.push(`Anuncios indexados agora: ${result.indexed_now}`);
      }
      if (typeof statusAfter.completed_listings !== "undefined") {
        lines.push(`Total guardados no output: ${statusAfter.completed_listings}`);
      }
      if (typeof statusAfter.pending_listings !== "undefined") {
        lines.push(`Pendentes restantes: ${statusAfter.pending_listings}`);
      }
      return lines;
    }

    async function saveFields() {
      const response = await fetch("/api/selection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ selected_fields: selectedFields() }),
      });
      state = await response.json();
      renderUi(false);
    }

    async function startJob(payload) {
      if (currentJobId) {
        return;
      }
      setActionButtonsDisabled(true);
      renderJob({
        status: "running",
        label: payload.label,
        logs: ["[ui] Pedido enviado. A preparar o scraping..."],
      });
      const response = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const job = await response.json();
      currentJobId = job.job_id;
      renderJob(job);
      pollJob();
    }

    async function pollJob() {
      if (!currentJobId) {
        return;
      }
      const response = await fetch(`/api/run/${currentJobId}`);
      const job = await response.json();
      renderJob(job);
      if (job.status === "running") {
        pollTimer = setTimeout(pollJob, 900);
        return;
      }
      currentJobId = null;
      setActionButtonsDisabled(false);
      state = await (await fetch("/api/ui")).json();
      renderUi(false);
      renderJob(job);
    }

    function quickPayload(label, action, maxPages = null) {
      return {
        action,
        label,
        max_pages: maxPages,
        mode: document.getElementById("modeSelect").value || null,
        target_names: selectedTargets(),
      };
    }

    function bindStaticEvents() {
      document.getElementById("modeSelect").addEventListener("change", renderCommandBox);
      document.getElementById("runPageBtn").addEventListener("click", () => {
        startJob(quickPayload("Sacar 1 pagina", "page", 1));
      });
      document.getElementById("runAllPagesBtn").addEventListener("click", () => {
        startJob(quickPayload("Sacar tudo", "page", null));
      });
      document.getElementById("runPendingBtn").addEventListener("click", () => {
        startJob(quickPayload("Continuar pendentes", "extract", null));
      });
      document.getElementById("saveFieldsBtn").addEventListener("click", saveFields);
      document.getElementById("recommendedBtn").addEventListener("click", () => {
        for (const field of state.fields) {
          const input = document.getElementById(`field-${field.name}`);
          if (input && !field.required) {
            input.checked = Boolean(field.default);
          }
        }
      });
      document.getElementById("minimalBtn").addEventListener("click", () => {
        for (const field of state.fields) {
          const input = document.getElementById(`field-${field.name}`);
          if (input && !field.required) {
            input.checked = false;
          }
        }
      });
    }

    function renderUi(bindEvents = true) {
      renderHeroStats();
      renderTargets();
      renderLatestRecord();
      renderStateGrid();
      renderFieldGrid();
      renderCommandBox();
      if (bindEvents) {
        bindStaticEvents();
      }
    }

    async function loadUi() {
      state = await (await fetch("/api/ui")).json();
      renderUi(true);
      if (state.current_job) {
        currentJobId = state.current_job.job_id;
        renderJob(state.current_job);
        setActionButtonsDisabled(true);
        pollJob();
      } else {
        renderJob(null);
      }
    }

    loadUi();
  </script>
</body>
</html>
"""

GUIDE_HTML = """<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Guia do Projeto</title>
  <style>
    :root {
      --bg-a: #d8e7e6;
      --bg-b: #efe7d7;
      --ink: #1d262b;
      --muted: #66747a;
      --line: rgba(22, 39, 46, 0.12);
      --panel: rgba(255, 255, 255, 0.82);
      --accent: #0f6b72;
      --accent-soft: #d5eff0;
      --accent-deep: #0d3038;
      --shadow: 0 28px 70px rgba(20, 34, 40, 0.16);
      --radius-xl: 30px;
      --radius-lg: 22px;
      --mono: "SFMono-Regular", Consolas, monospace;
      --body: "Avenir Next", "Segoe UI", sans-serif;
      --display: "Iowan Old Style", "Palatino Linotype", serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at 12% 18%, rgba(255, 255, 255, 0.72), transparent 20%),
        radial-gradient(circle at 85% 15%, rgba(255, 255, 255, 0.38), transparent 18%),
        linear-gradient(140deg, var(--bg-a) 0%, var(--bg-b) 55%, #f4efe2 100%);
      color: var(--ink);
      font-family: var(--body);
      padding: 22px;
    }

    .shell {
      width: min(1120px, 100%);
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }

    .panel {
      border-radius: var(--radius-xl);
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
      padding: 24px 28px;
    }

    .hero {
      display: grid;
      gap: 12px;
      background:
        linear-gradient(135deg, rgba(255,255,255,0.9), rgba(213,239,240,0.82)),
        linear-gradient(180deg, rgba(255,255,255,0.75), rgba(255,255,255,0.35));
    }

    .eyebrow {
      margin: 0;
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--accent);
    }

    h1, h2, h3 {
      margin: 0;
      font-family: var(--display);
      letter-spacing: -0.03em;
    }

    h1 {
      font-size: clamp(34px, 5vw, 56px);
      line-height: 0.96;
    }

    h2 {
      font-size: 30px;
      line-height: 1.02;
      margin-top: 8px;
    }

    h3 {
      font-size: 22px;
      line-height: 1.06;
      margin-top: 4px;
    }

    p {
      margin: 0;
      line-height: 1.65;
    }

    .muted {
      color: var(--muted);
    }

    .topbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }

    .nav-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 10px 14px;
      border: 1px solid rgba(15, 107, 114, 0.18);
      background: rgba(255,255,255,0.86);
      color: var(--accent-deep);
      font-weight: 800;
      text-decoration: none;
    }

    .guide {
      display: grid;
      gap: 18px;
    }

    .guide p + p {
      margin-top: 10px;
    }

    .guide ul,
    .guide ol {
      margin: 0;
      padding-left: 22px;
      display: grid;
      gap: 8px;
      line-height: 1.6;
    }

    .guide code {
      font-family: var(--mono);
      background: rgba(15, 107, 114, 0.08);
      padding: 1px 6px;
      border-radius: 8px;
      font-size: 0.96em;
    }

    .guide pre {
      margin: 0;
      padding: 16px 18px;
      border-radius: var(--radius-lg);
      background: linear-gradient(180deg, #0d2430, #102a35);
      color: #ecf4f5;
      overflow: auto;
      font-family: var(--mono);
      font-size: 13px;
      line-height: 1.6;
    }

    .guide pre code {
      background: transparent;
      padding: 0;
      border-radius: 0;
      color: inherit;
    }

    .guide h2,
    .guide h3 {
      padding-top: 18px;
      border-top: 1px solid var(--line);
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="panel hero">
      <p class="eyebrow">Idealista / Ericeira</p>
      <div class="topbar">
        <a class="nav-link" href="/">Voltar ao painel</a>
      </div>
      <h1>Guia do projeto</h1>
      <p class="muted">Esta pagina mostra o conteudo do README dentro da app, mas com um layout mais limpo e mais facil de consultar.</p>
    </section>
    <section class="panel guide">
      __GUIDE_CONTENT__
    </section>
  </main>
</body>
</html>
"""


def _friendly_target_label(name: str) -> str:
    return TARGET_LABELS.get(name, name.replace("_", " ").title())


def _friendly_target_help(name: str) -> str:
    return TARGET_HELP.get(name, "Target configurado no projeto.")


def _short_text(value: str | None, limit: int = 180) -> str | None:
    if not value:
        return value
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _inline_markdown_html(text: str) -> str:
    pattern = re.compile(r"(`[^`]+`|\*\*[^*]+\*\*)")
    parts: list[str] = []
    cursor = 0
    for match in pattern.finditer(text):
        parts.append(html_escape(text[cursor:match.start()]))
        token = match.group(0)
        if token.startswith("`"):
            parts.append(f"<code>{html_escape(token[1:-1])}</code>")
        else:
            parts.append(f"<strong>{html_escape(token[2:-2])}</strong>")
        cursor = match.end()
    parts.append(html_escape(text[cursor:]))
    return "".join(parts)


def _markdown_to_html(markdown: str) -> str:
    html_parts: list[str] = []
    paragraph_lines: list[str] = []
    list_items: list[str] = []
    list_kind: str | None = None
    code_lines: list[str] = []
    in_code = False

    def flush_paragraph() -> None:
        nonlocal paragraph_lines
        if not paragraph_lines:
            return
        text = " ".join(part.strip() for part in paragraph_lines if part.strip())
        html_parts.append(f"<p>{_inline_markdown_html(text)}</p>")
        paragraph_lines = []

    def flush_list() -> None:
        nonlocal list_items, list_kind
        if not list_items or list_kind is None:
            list_items = []
            list_kind = None
            return
        tag = "ol" if list_kind == "ol" else "ul"
        items = "".join(f"<li>{item}</li>" for item in list_items)
        html_parts.append(f"<{tag}>{items}</{tag}>")
        list_items = []
        list_kind = None

    def flush_code() -> None:
        nonlocal code_lines
        if not code_lines:
            return
        html_parts.append(f"<pre><code>{html_escape(chr(10).join(code_lines))}</code></pre>")
        code_lines = []

    for raw_line in markdown.splitlines():
        stripped = raw_line.strip()

        if stripped.startswith("```"):
            flush_paragraph()
            flush_list()
            if in_code:
                flush_code()
                in_code = False
            else:
                in_code = True
            continue

        if in_code:
            code_lines.append(raw_line)
            continue

        if not stripped:
            flush_paragraph()
            flush_list()
            continue

        heading = re.match(r"^(#{1,3})\s+(.*)$", stripped)
        if heading:
            flush_paragraph()
            flush_list()
            level = len(heading.group(1))
            html_parts.append(f"<h{level}>{_inline_markdown_html(heading.group(2))}</h{level}>")
            continue

        ordered = re.match(r"^\d+\.\s+(.*)$", stripped)
        bullet = re.match(r"^-\s+(.*)$", stripped)
        if ordered or bullet:
            flush_paragraph()
            item_html = _inline_markdown_html((ordered or bullet).group(1))
            next_kind = "ol" if ordered else "ul"
            if list_kind not in (None, next_kind):
                flush_list()
            list_kind = next_kind
            list_items.append(item_html)
            continue

        paragraph_lines.append(stripped)

    flush_paragraph()
    flush_list()
    flush_code()
    return "\n".join(html_parts)


def _readme_path(config_path: str | None = None) -> Path:
    _, paths = load_config(config_path)
    return paths.root / "README.md"


def _guide_html(config_path: str | None = None) -> str:
    readme_path = _readme_path(config_path)
    if readme_path.exists():
        markdown = readme_path.read_text(encoding="utf-8")
    else:
        markdown = "# Guia\n\nREADME.md nao encontrado."
    return GUIDE_HTML.replace("__GUIDE_CONTENT__", _markdown_to_html(markdown))


def _latest_record_payload(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    images = record.get("images") or []
    return {
        "address": record.get("address"),
        "description": _short_text(record.get("description")),
        "fetched_at": record.get("fetched_at"),
        "images_count": len(images),
        "listing_id": record.get("listing_id"),
        "preview_image_url": images[0] if images else None,
        "price_text": record.get("price_text"),
        "title": record.get("title"),
        "url": record.get("url"),
    }


def _field_payload(latest_record: dict[str, Any] | None, selected_fields: list[str]) -> list[dict[str, Any]]:
    selected = set(selected_fields)
    fields: list[dict[str, Any]] = []
    for spec in list_output_field_specs():
        fields.append(
            {
                "default": spec["default"],
                "description": spec["description"],
                "label": spec["label"],
                "name": spec["name"],
                "required": spec["required"],
                "selected": spec["name"] in selected,
                "value_preview": _short_text(str(latest_record.get(spec["name"]))) if latest_record else None,
            }
        )
    return fields


def _running_job_snapshot() -> dict[str, Any] | None:
    with RUN_JOBS_LOCK:
        running_jobs = [job for job in RUN_JOBS.values() if job["status"] == "running"]
        if not running_jobs:
            return None
        latest = max(running_jobs, key=lambda job: job["started_at"])
        return dict(latest)


def _ui_payload(config_path: str | None = None) -> dict[str, Any]:
    crawler = IdealistaCrawler(config_path=config_path)
    selection = load_output_selection(crawler.paths.selection_file)
    latest_record = read_last_jsonl_record(crawler.paths.details_output)

    return {
        "current_job": _running_job_snapshot(),
        "detail_output": str(crawler.paths.details_output),
        "fields": _field_payload(latest_record, selection["selected_fields"]),
        "latest_record": _latest_record_payload(latest_record),
        "selection": selection,
        "selection_file": str(crawler.paths.selection_file),
        "status": crawler.status(),
        "targets": [
            {
                "help": _friendly_target_help(target.name),
                "label": _friendly_target_label(target.name),
                "name": target.name,
                "search_url": target.search_url,
            }
            for target in crawler.config.targets
        ],
    }


def _run_action(payload: dict[str, Any], config_path: str | None = None, logger=None) -> dict[str, Any]:
    action = str(payload.get("action") or "page")
    target_names = payload.get("target_names") or None
    mode_override = payload.get("mode") or None
    max_pages = payload.get("max_pages")

    def _as_int(value: Any) -> int | None:
        if value in (None, "", 0, "0"):
            return None
        return int(value)

    crawler = IdealistaCrawler(config_path=config_path, mode_override=mode_override, logger=logger)
    if logger is not None:
        logger(f"[ui] Targets ativos: {', '.join(target_names or [target.name for target in crawler.config.targets])}")
        logger(f"[ui] Modo: {crawler.config.fetch.mode}")

    if action == "page":
        result = crawler.page_extract(target_names=target_names, max_pages=_as_int(max_pages) or 0)
    elif action == "discover":
        result = crawler.discover(target_names=target_names, max_pages=_as_int(max_pages))
    elif action == "crawl":
        result = crawler.crawl(target_names=target_names, max_pages=_as_int(max_pages), limit=None)
    elif action == "extract":
        result = crawler.extract(target_names=target_names, limit=None)
    else:
        raise ValueError(f"Acao invalida: {action}")

    return {
        "action": action,
        "mode": crawler.config.fetch.mode,
        "result": result,
        "status_after": crawler.status(),
        "target_names": target_names or [target.name for target in crawler.config.targets],
    }


def _update_job(job_id: str, **changes) -> dict[str, Any] | None:
    with RUN_JOBS_LOCK:
        job = RUN_JOBS.get(job_id)
        if job is None:
            return None
        job.update(changes)
        return dict(job)


def _append_job_log(job_id: str, message: str) -> None:
    with RUN_JOBS_LOCK:
        job = RUN_JOBS.get(job_id)
        if job is None:
            return
        job["logs"].append(message)
        if len(job["logs"]) > MAX_JOB_LOG_LINES:
            job["logs"] = job["logs"][-MAX_JOB_LOG_LINES:]
        job["updated_at"] = utc_now_iso()


def _job_snapshot(job_id: str) -> dict[str, Any] | None:
    with RUN_JOBS_LOCK:
        job = RUN_JOBS.get(job_id)
        return dict(job) if job is not None else None


def _start_job(payload: dict[str, Any], config_path: str | None = None) -> dict[str, Any]:
    with RUN_JOBS_LOCK:
        for job in RUN_JOBS.values():
            if job["status"] == "running":
                return dict(job)

        job_id = uuid4().hex[:10]
        job = {
            "error": None,
            "finished_at": None,
            "job_id": job_id,
            "label": payload.get("label") or str(payload.get("action") or "Acao"),
            "logs": ["[ui] Pedido recebido. A preparar execucao..."],
            "result": None,
            "started_at": utc_now_iso(),
            "status": "running",
            "updated_at": utc_now_iso(),
        }
        RUN_JOBS[job_id] = job

    def worker() -> None:
        def logger(message: str) -> None:
            _append_job_log(job_id, message)

        try:
            logger(f"[ui] {payload.get('label') or payload.get('action')}")
            result = _run_action(payload, config_path, logger=logger)
            logger("[ui] Execucao terminada.")
            _update_job(
                job_id,
                finished_at=utc_now_iso(),
                result=result,
                status="completed",
                updated_at=utc_now_iso(),
            )
        except Exception as exc:
            logger(f"[ui] Erro: {exc}")
            _update_job(
                job_id,
                error=str(exc),
                finished_at=utc_now_iso(),
                status="failed",
                updated_at=utc_now_iso(),
            )

    Thread(target=worker, daemon=True).start()
    return _job_snapshot(job_id) or {}


def _handler_factory(config_path: str | None = None):
    class UiHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            route = urlparse(self.path).path
            if route == "/":
                self._send_html(UI_HTML)
                return
            if route == "/guia":
                self._send_html(_guide_html(config_path))
                return
            if route == "/api/ui":
                self._send_json(_ui_payload(config_path))
                return
            if route.startswith("/api/run/"):
                job_id = route.rsplit("/", 1)[-1]
                job = _job_snapshot(job_id)
                if job is None:
                    self.send_error(404, "Job nao encontrado.")
                    return
                self._send_json(job)
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
                job = _start_job(payload, config_path)
                self._send_json(job)
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
