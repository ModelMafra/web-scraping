from __future__ import annotations

from collections import Counter
from html import escape as html_escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
from statistics import mean, median
from threading import Lock, Thread
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4
import webbrowser

from idealista_ericeira_scraper.core import (
    list_output_field_specs,
    load_config,
    load_output_selection,
    read_jsonl,
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

ANALYSIS_METRIC_SPECS: tuple[dict[str, str], ...] = (
    {"label": "Total anuncios", "key": "total_listings", "format": "number"},
    {"label": "Com preco", "key": "priced_listings", "format": "number"},
    {"label": "Preco medio", "key": "average_price_eur", "format": "euro"},
    {"label": "Preco mediano", "key": "median_price_eur", "format": "euro"},
    {"label": "Preco medio / m²", "key": "average_price_per_m2_eur", "format": "euro"},
    {"label": "Zonas unicas", "key": "unique_locations", "format": "number"},
)

ANALYSIS_CHART_SPECS: tuple[dict[str, str], ...] = (
    {
        "chart_id": "locationsChart",
        "data_key": "top_locations",
        "description": "Onde existem mais anuncios no dataset atual.",
        "title": "Top localizacoes",
    },
    {
        "chart_id": "bedroomsChart",
        "data_key": "bedrooms",
        "description": "Distribuicao por numero de quartos.",
        "title": "Tipologias",
    },
    {
        "chart_id": "typesChart",
        "data_key": "property_types",
        "description": "Agrupamento rapido pelo tipo principal do anuncio.",
        "title": "Tipos de imovel",
    },
)

ANALYSIS_PREVIEW_IMAGE_LIMIT = 40

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
          <a class="nav-link" href="/analise">Abrir analise de dados</a>
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
        <a class="nav-link" href="/analise">Abrir analise de dados</a>
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

ANALYSIS_HTML = """<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Analise dos dados</title>
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
      padding: 22px;
    }

    .shell {
      width: min(1480px, 100%);
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
      padding: 22px 24px;
      min-width: 0;
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
      font-size: 28px;
      line-height: 1.04;
    }

    h3 {
      font-size: 18px;
      line-height: 1.08;
    }

    p {
      margin: 0;
      line-height: 1.6;
    }

    .muted {
      color: var(--muted);
    }

    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
    }

    .metric {
      border-radius: var(--radius-lg);
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.88);
      padding: 16px;
      display: grid;
      gap: 6px;
    }

    .metric-label {
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }

    .metric-value {
      font-size: 28px;
      font-weight: 800;
      line-height: 1;
    }

    .chart-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
    }

    .section-copy {
      display: grid;
      gap: 6px;
      margin-bottom: 14px;
    }

    .bar-list {
      display: grid;
      gap: 12px;
    }

    .bar-row {
      display: grid;
      gap: 6px;
    }

    .bar-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      font-size: 14px;
      font-weight: 700;
    }

    .bar-track {
      block-size: 10px;
      border-radius: 999px;
      background: rgba(15, 107, 114, 0.12);
      overflow: hidden;
    }

    .bar-fill {
      block-size: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, #0f6b72, #6aa6ae);
    }

    .explorer-grid {
      display: grid;
      grid-template-columns: 430px minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }

    .list-panel {
      display: grid;
      gap: 14px;
      align-content: start;
    }

    .toolbar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 180px;
      gap: 10px;
    }

    input,
    select {
      width: 100%;
      border-radius: 16px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      background: rgba(255,255,255,0.92);
      color: var(--ink);
      font: inherit;
    }

    .ads-count {
      font-size: 13px;
      font-weight: 700;
      color: var(--muted);
    }

    .ads-list {
      display: grid;
      gap: 10px;
      max-block-size: 920px;
      overflow: auto;
      padding-right: 4px;
    }

    .ad-row {
      display: grid;
      grid-template-columns: 72px minmax(0, 1fr);
      gap: 12px;
      padding: 10px;
      border-radius: 20px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.88);
      cursor: pointer;
      transition: transform 120ms ease, border-color 120ms ease, background 120ms ease;
    }

    .ad-row:hover {
      transform: translateY(-1px);
      border-color: rgba(15, 107, 114, 0.28);
    }

    .ad-row.active {
      background: rgba(213,239,240,0.74);
      border-color: rgba(15, 107, 114, 0.34);
    }

    .ad-thumb {
      inline-size: 72px;
      block-size: 72px;
      border-radius: 14px;
      overflow: hidden;
      background: linear-gradient(180deg, #0d2430, #102a35);
      display: grid;
      place-items: center;
      color: rgba(236, 244, 245, 0.76);
      font-size: 12px;
      text-align: center;
      padding: 8px;
    }

    .ad-thumb img {
      inline-size: 100%;
      block-size: 100%;
      object-fit: cover;
      display: block;
    }

    .ad-main {
      display: grid;
      gap: 4px;
      align-content: start;
      min-width: 0;
    }

    .ad-title {
      font-weight: 800;
      line-height: 1.3;
    }

    .ad-meta {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.45;
    }

    .preview-panel {
      display: grid;
      gap: 14px;
    }

    .preview-frame {
      inline-size: 100%;
      block-size: 980px;
      border: 0;
      border-radius: var(--radius-lg);
      background: rgba(255,255,255,0.9);
    }

    @media (max-width: 1280px) {
      .metrics-grid,
      .chart-grid,
      .explorer-grid,
      .toolbar {
        grid-template-columns: 1fr;
      }

      .preview-frame {
        block-size: 760px;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="panel hero">
      <p class="eyebrow">Idealista / Ericeira</p>
      <div class="topbar">
        <a class="nav-link" href="/">Voltar ao painel</a>
        <a class="nav-link" href="/guia">Abrir guia</a>
      </div>
      <h1>Analise dos dados</h1>
      <p class="muted">Métricas principais, 3 gráficos essenciais e um explorador com preview interno por anúncio.</p>
    </section>

    <section id="metricsGrid" class="metrics-grid"></section>

    <section id="chartGrid" class="chart-grid"></section>

    <section class="explorer-grid">
      <article class="panel list-panel">
        <div class="section-copy">
          <h2>Explorador de anuncios</h2>
          <p class="muted">Pesquisa, ordena e clica num anuncio para abrir o `iframe` ao lado.</p>
        </div>
        <div class="toolbar">
          <input id="searchInput" type="search" placeholder="Pesquisar por titulo, zona ou ID">
          <select id="sortSelect">
            <option value="latest">Mais recentes</option>
            <option value="price_desc">Preco mais alto</option>
            <option value="price_asc">Preco mais baixo</option>
          </select>
        </div>
        <div id="adsCount" class="ads-count">A carregar anuncios...</div>
        <div id="adsList" class="ads-list"></div>
      </article>

      <article class="panel preview-panel">
        <div class="section-copy">
          <h2>Preview no iframe</h2>
          <p class="muted">O `iframe` mostra a ficha interna do anuncio selecionado, com galeria de imagens e atalho para o URL original.</p>
        </div>
        <iframe id="previewFrame" class="preview-frame" title="Preview do anuncio"></iframe>
      </article>
    </section>
  </main>

  <script>
    let analysisData = null;
    let selectedListingId = null;

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function formatNumber(value, digits = 0) {
      if (value === null || typeof value === "undefined") {
        return "—";
      }
      return new Intl.NumberFormat("pt-PT", {
        maximumFractionDigits: digits,
        minimumFractionDigits: digits,
      }).format(value);
    }

    function formatEuro(value) {
      if (value === null || typeof value === "undefined") {
        return "—";
      }
      return `${formatNumber(value)} €`;
    }

    function renderMetrics() {
      const summary = analysisData.summary;
      const items = analysisData.metric_specs.map((spec) => {
        const rawValue = summary[spec.key];
        let value = "—";
        if (spec.format === "euro") {
          value = formatEuro(rawValue);
        } else if (spec.format === "sqm") {
          value = rawValue ? `${formatNumber(rawValue, 1)} m²` : "—";
        } else {
          value = formatNumber(rawValue);
        }
        return [spec.label, value];
      });
      document.getElementById("metricsGrid").innerHTML = items.map(([label, value]) => `
        <article class="metric">
          <div class="metric-label">${escapeHtml(label)}</div>
          <div class="metric-value">${escapeHtml(value)}</div>
        </article>
      `).join("");
    }

    function renderCharts() {
      const root = document.getElementById("chartGrid");
      root.innerHTML = analysisData.chart_specs.map((spec) => `
        <article class="panel">
          <div class="section-copy">
            <h2>${escapeHtml(spec.title)}</h2>
            <p class="muted">${escapeHtml(spec.description)}</p>
          </div>
          <div id="${escapeHtml(spec.chart_id)}" class="bar-list"></div>
        </article>
      `).join("");

      for (const spec of analysisData.chart_specs) {
        renderBarList(spec.chart_id, analysisData[spec.data_key], (item) => `${formatNumber(item.count)} anuncios`);
      }
    }

    function renderBarList(elementId, items, formatter) {
      const root = document.getElementById(elementId);
      if (!items || !items.length) {
        root.innerHTML = `<p class="muted">Sem dados suficientes.</p>`;
        return;
      }
      const max = Math.max(...items.map((item) => item.count), 1);
      root.innerHTML = items.map((item) => `
        <article class="bar-row">
          <div class="bar-head">
            <span>${escapeHtml(item.label)}</span>
            <span>${escapeHtml(formatter(item))}</span>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width:${(item.count / max) * 100}%"></div>
          </div>
        </article>
      `).join("");
    }

    function filteredAds() {
      if (!analysisData) return [];
      const query = (document.getElementById("searchInput").value || "").trim().toLowerCase();
      const sort = document.getElementById("sortSelect").value;
      let ads = analysisData.ads.slice();
      if (query) {
        ads = ads.filter((ad) => [ad.title, ad.address, ad.listing_id, ad.property_type]
          .filter(Boolean)
          .join(" ")
          .toLowerCase()
          .includes(query)
        );
      }
      ads.sort((a, b) => {
        if (sort === "price_desc") return (b.price_amount_eur || -1) - (a.price_amount_eur || -1);
        if (sort === "price_asc") return (a.price_amount_eur || Number.MAX_SAFE_INTEGER) - (b.price_amount_eur || Number.MAX_SAFE_INTEGER);
        return String(b.fetched_at || "").localeCompare(String(a.fetched_at || ""));
      });
      return ads;
    }

    function updatePreview() {
      const frame = document.getElementById("previewFrame");
      frame.src = selectedListingId ? `/analise/anuncio/${selectedListingId}` : "about:blank";
    }

    function renderAdsList() {
      const ads = filteredAds();
      const countLabel = ads.length === 1 ? "1 anuncio" : `${formatNumber(ads.length)} anuncios`;
      document.getElementById("adsCount").textContent = countLabel;
      if (!ads.length) {
        selectedListingId = null;
        updatePreview();
        document.getElementById("adsList").innerHTML = `<p class="muted">Nenhum anuncio corresponde ao filtro atual.</p>`;
        return;
      }

      if (!ads.some((ad) => ad.listing_id === selectedListingId)) {
        selectedListingId = ads[0].listing_id;
      }

      document.getElementById("adsList").innerHTML = ads.map((ad) => `
        <article class="ad-row ${ad.listing_id === selectedListingId ? "active" : ""}" data-id="${escapeHtml(ad.listing_id)}">
          <div class="ad-thumb">
            ${ad.preview_image_url
              ? `<img loading="lazy" src="${escapeHtml(ad.preview_image_url)}" alt="${escapeHtml(ad.title || ad.listing_id)}">`
              : `<span>Sem imagem</span>`}
          </div>
          <div class="ad-main">
            <div class="ad-title">${escapeHtml(ad.title || "Sem titulo")}</div>
            <div class="ad-meta">${escapeHtml(ad.address || "Sem localizacao")}</div>
            <div class="ad-meta">${escapeHtml(ad.price_text || formatEuro(ad.price_amount_eur))}</div>
            <div class="ad-meta">${escapeHtml(ad.bedrooms || "—")} · ${escapeHtml(ad.area_label || "—")} · ${escapeHtml(ad.images_label)}</div>
          </div>
        </article>
      `).join("");

      for (const node of document.querySelectorAll(".ad-row")) {
        node.addEventListener("click", () => {
          selectedListingId = node.dataset.id;
          renderAdsList();
        });
      }

      updatePreview();
    }

    async function loadAnalysis() {
      const response = await fetch("/api/analysis");
      analysisData = await response.json();
      renderMetrics();
      renderCharts();
      renderAdsList();
    }

    document.getElementById("searchInput").addEventListener("input", renderAdsList);
    document.getElementById("sortSelect").addEventListener("change", renderAdsList);

    loadAnalysis();
  </script>
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


def _detail_records(config_path: str | None = None) -> list[dict[str, Any]]:
    _, paths = load_config(config_path)
    if not paths.details_output.exists():
        return []

    latest_by_listing_id: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(paths.details_output):
        listing_id = str(record.get("listing_id") or "").strip()
        if not listing_id:
            continue
        latest_by_listing_id[listing_id] = dict(record, listing_id=listing_id)

    records = list(latest_by_listing_id.values())
    records.sort(key=lambda item: str(item.get("fetched_at") or ""), reverse=True)
    return records


def _number_from_text(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = value.strip().replace(" ", "")
    if not cleaned:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif cleaned.count(".") == 1 and len(cleaned.split(".")[1]) == 3:
        cleaned = cleaned.replace(".", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _price_amount(record: dict[str, Any]) -> int | None:
    amount = record.get("price_amount_eur")
    if isinstance(amount, bool):
        return None
    if isinstance(amount, (int, float)):
        return int(amount)

    price_text = str(record.get("price_text") or "")
    match = re.search(r"(\d[\d\s\.,]*)\s*€", price_text)
    if not match:
        return None
    value = _number_from_text(match.group(1))
    return int(value) if value is not None else None


def _area_candidates(text: str | None) -> list[float]:
    if not text:
        return []
    values: list[float] = []
    for match in re.finditer(r"(\d[\d\s\.,]*)\s*m²", text, flags=re.I):
        parsed = _number_from_text(match.group(1))
        if parsed is not None:
            values.append(parsed)
    return values


def _guess_area_m2(record: dict[str, Any]) -> float | None:
    feature_list = record.get("feature_list") or []
    preferred: list[float] = []
    fallback: list[float] = []

    for item in feature_list:
        if not isinstance(item, str):
            continue
        numbers = _area_candidates(item)
        if not numbers:
            continue
        lowered = item.lower()
        if "area bruta" in lowered or "área bruta" in lowered:
            preferred.extend(numbers)
        fallback.extend(numbers)

    if preferred:
        return max(preferred)
    if fallback:
        return max(fallback)

    for value in (
        record.get("title"),
        record.get("description"),
        record.get("page_text_excerpt"),
    ):
        numbers = _area_candidates(str(value or ""))
        if numbers:
            return max(numbers)
    return None


def _guess_bedrooms(record: dict[str, Any]) -> str | None:
    candidates: list[str] = []
    feature_list = record.get("feature_list") or []
    candidates.extend(item for item in feature_list if isinstance(item, str))
    candidates.extend(
        [
            str(record.get("title") or ""),
            str(record.get("description") or ""),
        ]
    )
    for text in candidates:
        match = re.search(r"\bt\s*(\d{1,2})\b", text, flags=re.I)
        if match:
            return f"T{int(match.group(1))}"
    return None


def _guess_property_type(record: dict[str, Any]) -> str | None:
    title = str(record.get("title") or "").strip()
    if title:
        match = re.match(
            r"([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s]+?)(?:\s+t\d+\b|\s+à\s+(?:venda|renda|arrendar|alugar)\b)",
            title,
            flags=re.I,
        )
        if match:
            return match.group(1).strip()

    feature_list = record.get("feature_list") or []
    for item in feature_list:
        if not isinstance(item, str):
            continue
        lowered = item.lower()
        if any(
            token in lowered
            for token in (
                "m²",
                "casa de banho",
                "casas de banho",
                "garagem",
                "elevador",
                "terraço",
                "terraco",
                "varanda",
                "estado",
                "piso",
                "andar",
                "ar condicionado",
            )
        ):
            continue
        cleaned = item.strip()
        if cleaned:
            return cleaned

    match = re.match(r"(.+?)\s+à\s+(?:venda|renda|arrendar|alugar)\b", title, flags=re.I)
    if match:
        return match.group(1).strip()
    return None


def _listing_images(record: dict[str, Any], limit: int | None = 24) -> list[str]:
    raw_images = record.get("images") or []
    images: list[str] = []
    seen_urls: set[str] = set()
    seen_assets: set[str] = set()
    for item in raw_images:
        if not isinstance(item, str):
            continue
        url = item.strip()
        if not url or url in seen_urls:
            continue
        lowered = url.lower()
        if any(
            token in lowered
            for token in (
                ".svg",
                "logo",
                "icon",
                "flag",
                "avatar",
                "social",
                "maps.googleapis",
                "googleapis",
            )
        ):
            continue
        if not re.search(r"\.(?:jpe?g|png|webp)(?:\?|$)", lowered):
            continue
        asset_name = Path(urlparse(url).path).name
        asset_key = asset_name.rsplit(".", 1)[0] if asset_name else lowered
        if asset_key in seen_assets:
            continue
        seen_urls.add(url)
        seen_assets.add(asset_key)
        images.append(url)
        if limit is not None and len(images) >= limit:
            break
    return images


def _format_eur(value: float | int | None, digits: int = 0) -> str:
    if value is None:
        return "—"
    number = f"{value:,.{digits}f}"
    number = number.replace(",", "X").replace(".", ",").replace("X", ".")
    if digits == 0:
        number = number.split(",")[0]
    return f"{number} €"


def _format_count_label(count: int, singular: str, plural: str) -> str:
    return f"{count} {singular if count == 1 else plural}"


def _bedroom_counter_rows(counter: Counter[str], limit: int = 8) -> list[dict[str, Any]]:
    def sort_key(item: tuple[str, int]) -> tuple[int, int | str]:
        label = item[0]
        match = re.match(r"^T(\d+)$", label)
        if match:
            return (0, int(match.group(1)))
        return (1, label)

    return [
        {"label": label, "count": count}
        for label, count in sorted(counter.items(), key=sort_key)[:limit]
    ]


def _counter_rows(counter: Counter[str], limit: int = 8) -> list[dict[str, Any]]:
    return [
        {"label": label, "count": count}
        for label, count in counter.most_common(limit)
    ]


def _analysis_payload(config_path: str | None = None) -> dict[str, Any]:
    records = _detail_records(config_path)
    prices: list[int] = []
    price_per_m2_values: list[float] = []
    areas: list[float] = []
    image_counts: list[int] = []
    location_counter: Counter[str] = Counter()
    bedroom_counter: Counter[str] = Counter()
    property_counter: Counter[str] = Counter()
    ads: list[dict[str, Any]] = []

    for record in records:
        listing_id = str(record.get("listing_id") or "").strip()
        if not listing_id:
            continue

        images = _listing_images(record, limit=ANALYSIS_PREVIEW_IMAGE_LIMIT)
        address = str(record.get("address") or "").strip() or "Sem localizacao"
        title = str(record.get("title") or "").strip() or "Sem titulo"
        property_type = _guess_property_type(record) or "Nao identificado"
        bedrooms = _guess_bedrooms(record) or "Nao indicado"
        area_m2 = _guess_area_m2(record)
        price_amount = _price_amount(record)
        price_text = str(record.get("price_text") or "").strip()
        if not price_text and price_amount is not None:
            price_text = _format_eur(price_amount)

        if price_amount is not None:
            prices.append(price_amount)
        if area_m2 is not None:
            areas.append(area_m2)
        if price_amount is not None and area_m2 and area_m2 > 0:
            price_per_m2_values.append(price_amount / area_m2)

        image_counts.append(len(images))
        location_counter[address] += 1
        bedroom_counter[bedrooms] += 1
        property_counter[property_type] += 1

        ads.append(
            {
                "address": address,
                "area_label": f"{area_m2:.0f} m²" if area_m2 is not None else "Area n/d",
                "bedrooms": bedrooms,
                "fetched_at": record.get("fetched_at"),
                "images_label": _format_count_label(len(images), "imagem", "imagens"),
                "listing_id": listing_id,
                "preview_image_url": images[0] if images else None,
                "price_amount_eur": price_amount,
                "price_text": price_text or "Sem preco",
                "property_type": property_type,
                "title": title,
                "url": record.get("url") or record.get("final_url"),
            }
        )

    summary = {
        "average_area_m2": round(mean(areas), 1) if areas else None,
        "average_images": round(mean(image_counts), 1) if image_counts else None,
        "average_price_eur": round(mean(prices)) if prices else None,
        "average_price_per_m2_eur": round(mean(price_per_m2_values)) if price_per_m2_values else None,
        "max_price_eur": max(prices) if prices else None,
        "median_price_eur": round(median(prices)) if prices else None,
        "min_price_eur": min(prices) if prices else None,
        "priced_listings": len(prices),
        "total_listings": len(records),
        "unique_locations": len(location_counter),
    }

    return {
        "ads": ads,
        "chart_specs": [dict(spec) for spec in ANALYSIS_CHART_SPECS],
        "bedrooms": _bedroom_counter_rows(bedroom_counter),
        "metric_specs": [dict(spec) for spec in ANALYSIS_METRIC_SPECS],
        "property_types": _counter_rows(property_counter),
        "summary": summary,
        "top_locations": _counter_rows(location_counter),
    }


def _analysis_listing_html(config_path: str | None, listing_id: str) -> str:
    record = next((item for item in _detail_records(config_path) if str(item.get("listing_id")) == listing_id), None)
    if record is None:
        return """<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Anuncio nao encontrado</title>
  <style>
    body { margin: 0; padding: 24px; font-family: "Avenir Next", "Segoe UI", sans-serif; background: #f4f0e4; color: #1d262b; }
    .panel { max-width: 720px; margin: 0 auto; padding: 28px; border-radius: 24px; background: rgba(255,255,255,0.88); border: 1px solid rgba(22,39,46,0.12); }
    h1 { margin: 0 0 8px; font-family: "Iowan Old Style", "Palatino Linotype", serif; font-size: 36px; }
  </style>
</head>
<body>
  <main class="panel">
    <h1>Anuncio nao encontrado</h1>
    <p>O ID pedido nao existe no JSONL atual.</p>
  </main>
</body>
</html>"""

    images = _listing_images(record, limit=ANALYSIS_PREVIEW_IMAGE_LIMIT)
    title = str(record.get("title") or "").strip() or "Sem titulo"
    address = str(record.get("address") or "").strip() or "Sem localizacao"
    price_amount = _price_amount(record)
    price_text = str(record.get("price_text") or "").strip() or _format_eur(price_amount)
    property_type = _guess_property_type(record) or "Nao identificado"
    bedrooms = _guess_bedrooms(record) or "Nao indicado"
    area_m2 = _guess_area_m2(record)
    price_per_m2 = round(price_amount / area_m2) if price_amount is not None and area_m2 and area_m2 > 0 else None
    feature_list = [str(item).strip() for item in (record.get("feature_list") or []) if str(item).strip()]
    description = str(record.get("description") or "").strip()
    original_url = str(record.get("url") or record.get("final_url") or "").strip()

    gallery_main = ""
    gallery_thumbs = ""
    if images:
        first_image = html_escape(images[0])
        gallery_main = f"""
          <div class="main-media">
            <img id="mainImage" src="{first_image}" alt="{html_escape(title)}">
          </div>
        """
        gallery_thumbs = "".join(
            f"""
            <button class="thumb {'active' if index == 0 else ''}" type="button" data-src="{html_escape(url)}">
              <img src="{html_escape(url)}" alt="Imagem {index + 1}">
            </button>
            """
            for index, url in enumerate(images)
        )
        gallery_thumbs = f'<div class="thumbs">{gallery_thumbs}</div>'
    else:
        gallery_main = '<div class="main-media empty">Sem imagens filtradas para este anuncio.</div>'

    features_html = "".join(f"<li>{html_escape(item)}</li>" for item in feature_list[:24])
    description_html = f"<p>{html_escape(description)}</p>" if description else "<p>Sem descricao guardada.</p>"
    open_link = (
        f'<a class="external-link" href="{html_escape(original_url)}" target="_blank" rel="noreferrer">Abrir anuncio original</a>'
        if original_url
        else ""
    )

    return f"""<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html_escape(title)}</title>
  <style>
    :root {{
      --ink: #1d262b;
      --muted: #66747a;
      --line: rgba(22, 39, 46, 0.12);
      --accent: #0f6b72;
      --panel: rgba(255, 255, 255, 0.92);
      --shadow: 0 20px 50px rgba(20, 34, 40, 0.15);
      --radius-xl: 28px;
      --radius-lg: 20px;
      --display: "Iowan Old Style", "Palatino Linotype", serif;
      --body: "Avenir Next", "Segoe UI", sans-serif;
    }}

    * {{ box-sizing: border-box; }}

    body {{
      margin: 0;
      min-height: 100vh;
      padding: 18px;
      font-family: var(--body);
      color: var(--ink);
      background:
        radial-gradient(circle at 12% 18%, rgba(255, 255, 255, 0.72), transparent 20%),
        linear-gradient(145deg, #d8e7e6 0%, #efe7d7 70%, #f3eee2 100%);
    }}

    .shell {{
      display: grid;
      gap: 18px;
      max-width: 1280px;
      margin: 0 auto;
    }}

    .panel {{
      border-radius: var(--radius-xl);
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      padding: 20px;
    }}

    .hero {{
      display: grid;
      gap: 10px;
      background: linear-gradient(135deg, rgba(255,255,255,0.9), rgba(213,239,240,0.8));
    }}

    .hero h1 {{
      margin: 0;
      font-family: var(--display);
      font-size: clamp(32px, 4vw, 48px);
      line-height: 0.98;
      letter-spacing: -0.03em;
    }}

    .hero p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.55;
    }}

    .chips {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 4px;
    }}

    .chip {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 8px 12px;
      background: rgba(15, 107, 114, 0.1);
      color: var(--accent);
      font-size: 13px;
      font-weight: 800;
    }}

    .grid {{
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 18px;
    }}

    .gallery {{
      display: grid;
      gap: 12px;
    }}

    .main-media {{
      block-size: 420px;
      border-radius: var(--radius-lg);
      overflow: hidden;
      background: linear-gradient(180deg, #0d2430, #102a35);
      border: 1px solid var(--line);
    }}

    .main-media.empty {{
      display: grid;
      place-items: center;
      padding: 24px;
      color: rgba(236, 244, 245, 0.9);
      text-align: center;
    }}

    .main-media img {{
      inline-size: 100%;
      block-size: 100%;
      object-fit: contain;
      display: block;
      background: #102a35;
    }}

    .thumbs {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(88px, 1fr));
      gap: 10px;
    }}

    .thumb {{
      padding: 0;
      border: 1px solid var(--line);
      border-radius: 16px;
      overflow: hidden;
      background: rgba(255,255,255,0.84);
      cursor: pointer;
    }}

    .thumb.active {{
      border-color: rgba(15, 107, 114, 0.34);
      box-shadow: 0 0 0 3px rgba(15, 107, 114, 0.1);
    }}

    .thumb img {{
      inline-size: 100%;
      block-size: 82px;
      object-fit: cover;
      display: block;
    }}

    .side {{
      display: grid;
      gap: 14px;
      align-content: start;
    }}

    .mini-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}

    .mini-card {{
      border-radius: var(--radius-lg);
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.88);
      padding: 14px;
    }}

    .mini-label {{
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 6px;
    }}

    .mini-value {{
      font-size: 20px;
      font-weight: 800;
      line-height: 1.2;
      word-break: break-word;
    }}

    .external-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 11px 14px;
      background: var(--accent);
      color: white;
      text-decoration: none;
      font-weight: 800;
    }}

    .text-block {{
      display: grid;
      gap: 10px;
    }}

    .text-block h2 {{
      margin: 0;
      font-family: var(--display);
      font-size: 28px;
      letter-spacing: -0.03em;
    }}

    .text-block p {{
      margin: 0;
      line-height: 1.65;
      color: var(--ink);
    }}

    ul {{
      margin: 0;
      padding-left: 20px;
      display: grid;
      gap: 8px;
      line-height: 1.55;
    }}

    @media (max-width: 960px) {{
      .grid,
      .mini-grid {{
        grid-template-columns: 1fr;
      }}

      .main-media {{
        block-size: 320px;
      }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="panel hero">
      <h1>{html_escape(title)}</h1>
      <p>{html_escape(address)}</p>
      <div class="chips">
        <span class="chip">{html_escape(price_text or "Sem preco")}</span>
        <span class="chip">{html_escape(property_type)}</span>
        <span class="chip">{html_escape(bedrooms)}</span>
        <span class="chip">{html_escape(f"{area_m2:.0f} m²" if area_m2 is not None else "Area n/d")}</span>
      </div>
      {open_link}
    </section>

    <section class="grid">
      <article class="panel gallery">
        {gallery_main}
        {gallery_thumbs}
      </article>

      <aside class="side">
        <article class="panel mini-grid">
          <div class="mini-card">
            <div class="mini-label">ID</div>
            <div class="mini-value">{html_escape(str(record.get("listing_id") or "—"))}</div>
          </div>
          <div class="mini-card">
            <div class="mini-label">Data</div>
            <div class="mini-value">{html_escape(str(record.get("fetched_at") or "—"))}</div>
          </div>
          <div class="mini-card">
            <div class="mini-label">Preco / m²</div>
            <div class="mini-value">{html_escape(_format_eur(price_per_m2) if price_per_m2 is not None else "—")}</div>
          </div>
          <div class="mini-card">
            <div class="mini-label">Imagens</div>
            <div class="mini-value">{html_escape(_format_count_label(len(images), "imagem", "imagens"))}</div>
          </div>
        </article>

        <article class="panel text-block">
          <h2>Descricao</h2>
          {description_html}
        </article>

        <article class="panel text-block">
          <h2>Caracteristicas</h2>
          <ul>{features_html or "<li>Sem caracteristicas adicionais guardadas.</li>"}</ul>
        </article>
      </aside>
    </section>
  </main>

  <script>
    const mainImage = document.getElementById("mainImage");
    for (const thumb of document.querySelectorAll(".thumb")) {{
      thumb.addEventListener("click", () => {{
        if (!mainImage) return;
        mainImage.src = thumb.dataset.src;
        for (const node of document.querySelectorAll(".thumb")) {{
          node.classList.remove("active");
        }}
        thumb.classList.add("active");
      }});
    }}
  </script>
</body>
</html>"""


def _latest_record_payload(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    images = _listing_images(record)
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
            if route == "/analise":
                self._send_html(ANALYSIS_HTML)
                return
            if route == "/guia":
                self._send_html(_guide_html(config_path))
                return
            if route == "/api/analysis":
                self._send_json(_analysis_payload(config_path))
                return
            if route.startswith("/analise/anuncio/"):
                listing_id = route.rsplit("/", 1)[-1].strip()
                if not listing_id:
                    self.send_error(404, "Anuncio nao encontrado.")
                    return
                self._send_html(_analysis_listing_html(config_path, listing_id))
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

        def _send_html(self, body: str, status: int = 200) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
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
