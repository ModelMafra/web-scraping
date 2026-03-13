from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any
import tomllib


@dataclass(frozen=True)
class RunConfig:
    request_delay_seconds: float = 2.0
    max_retries: int = 3
    save_html_snapshots: bool = True
    snapshot_overwrite: bool = False
    stop_on_blocked_response: bool = True
    max_pages_per_target: int = 0


@dataclass(frozen=True)
class FetchConfig:
    mode: str = "stealth"
    headless: bool = True
    real_chrome: bool = True
    solve_cloudflare: bool = True
    humanize: bool = True
    network_idle: bool = True
    google_search: bool = False
    disable_resources: bool = False
    timeout_ms: int = 45_000
    wait_ms: int = 2_500
    locale: str = "pt-PT"
    proxy: str = ""
    proxies_file: str = ""
    user_data_dir: str = ""
    cdp_url: str = ""


@dataclass(frozen=True)
class TargetConfig:
    name: str
    search_url: str
    listing_type: str
    property_scope: str


@dataclass(frozen=True)
class AppConfig:
    run: RunConfig
    fetch: FetchConfig
    targets: list[TargetConfig]


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    config_file: Path
    selection_file: Path
    discovery_index: Path
    details_output: Path
    journal_file: Path
    html_snapshots: Path
    logs_dir: Path

    def ensure_dirs(self) -> None:
        self.discovery_index.parent.mkdir(parents=True, exist_ok=True)
        self.details_output.parent.mkdir(parents=True, exist_ok=True)
        self.journal_file.parent.mkdir(parents=True, exist_ok=True)
        self.html_snapshots.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)


def default_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_paths(project_root: Path) -> ProjectPaths:
    return ProjectPaths(
        root=project_root,
        config_file=project_root / "config" / "targets.toml",
        selection_file=project_root / "config" / "extract_fields.json",
        discovery_index=project_root / "data" / "discovery" / "ericeira_listing_index.jsonl",
        details_output=project_root / "data" / "details" / "ericeira_ads.jsonl",
        journal_file=project_root / "state" / "journal.jsonl",
        html_snapshots=project_root / "data" / "html",
        logs_dir=project_root / "logs",
    )


def load_config(config_path: str | Path | None = None) -> tuple[AppConfig, ProjectPaths]:
    project_root = default_project_root()
    paths = build_paths(project_root)
    file_path = Path(config_path).expanduser() if config_path else paths.config_file
    if not file_path.is_absolute():
        file_path = project_root / file_path

    with file_path.open("rb") as handle:
        raw = tomllib.load(handle)

    run = RunConfig(**raw.get("run", {}))
    raw_fetch = dict(raw.get("fetch", {}))
    for key in ("proxies_file", "user_data_dir"):
        value = raw_fetch.get(key)
        if value and not Path(value).expanduser().is_absolute():
            raw_fetch[key] = str((project_root / value).resolve())
    fetch = FetchConfig(**raw_fetch)
    targets = [TargetConfig(**item) for item in raw.get("targets", [])]
    if not targets:
        raise ValueError(f"Nenhum target definido em {file_path}")

    paths = ProjectPaths(
        root=project_root,
        config_file=file_path,
        selection_file=paths.selection_file,
        discovery_index=paths.discovery_index,
        details_output=paths.details_output,
        journal_file=paths.journal_file,
        html_snapshots=paths.html_snapshots,
        logs_dir=paths.logs_dir,
    )
    return AppConfig(run=run, fetch=fetch, targets=targets), paths


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def text_sha256(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def write_text_file(path: Path, content: str, overwrite: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        return
    path.write_text(content, encoding="utf-8")


OUTPUT_FIELD_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "listing_id",
        "label": "ID do anuncio",
        "group": "identificacao",
        "description": "ID unico extraido do URL /imovel/{id}/.",
        "source_hint": "href do anuncio na pagina de resultados e URL final do detalhe",
        "required": True,
        "default": True,
    },
    {
        "name": "url",
        "label": "URL original",
        "group": "identificacao",
        "description": "Link canonico do anuncio usado no extract.",
        "source_hint": "href dos links /imovel/ na listagem",
        "required": True,
        "default": True,
    },
    {
        "name": "target_name",
        "label": "Target",
        "group": "identificacao",
        "description": "Nome interno do target configurado no projeto.",
        "source_hint": "config/targets.toml",
        "required": True,
        "default": True,
    },
    {
        "name": "fetched_at",
        "label": "Data de captura",
        "group": "identificacao",
        "description": "Timestamp UTC de quando o detalhe foi guardado.",
        "source_hint": "gerado pelo scraper",
        "required": True,
        "default": True,
    },
    {
        "name": "title",
        "label": "Titulo",
        "group": "anuncio",
        "description": "Titulo principal do anuncio.",
        "source_hint": ".main-info__title-main, h1, og:title",
        "required": False,
        "default": True,
    },
    {
        "name": "address",
        "label": "Localizacao",
        "group": "anuncio",
        "description": "Zona ou morada curta mostrada na ficha.",
        "source_hint": ".main-info__title-minor e schema address",
        "required": False,
        "default": True,
    },
    {
        "name": "price_text",
        "label": "Preco texto",
        "group": "anuncio",
        "description": "Preco tal como aparece na pagina.",
        "source_hint": ".info-data-price, .price-container .price e bloco JS com price:",
        "required": False,
        "default": True,
    },
    {
        "name": "price_amount_eur",
        "label": "Preco numerico",
        "group": "anuncio",
        "description": "Preco convertido para inteiro em euros.",
        "source_hint": "normalizacao do texto/preco JS",
        "required": False,
        "default": True,
    },
    {
        "name": "description",
        "label": "Descricao",
        "group": "anuncio",
        "description": "Texto principal do anunciante.",
        "source_hint": "bloco de comentario do anuncio e meta description",
        "required": False,
        "default": True,
    },
    {
        "name": "feature_list",
        "label": "Lista de caracteristicas",
        "group": "caracteristicas",
        "description": "Lista crua de caracteristicas visiveis.",
        "source_hint": ".details-property_features li e .info-features span",
        "required": False,
        "default": True,
    },
    {
        "name": "features",
        "label": "Caracteristicas mapeadas",
        "group": "caracteristicas",
        "description": "Mapa chave/valor quando a pagina o permite.",
        "source_hint": "pares dt/dd e itens no formato chave: valor",
        "required": False,
        "default": False,
    },
    {
        "name": "images",
        "label": "Imagens",
        "group": "caracteristicas",
        "description": "Lista de URLs de imagens detetadas no HTML.",
        "source_hint": "meta og:image, JSON-LD e URLs img*.idealista.pt no HTML",
        "required": False,
        "default": True,
    },
    {
        "name": "listing_type",
        "label": "Tipo de operacao",
        "group": "origem",
        "description": "Sale/rent vindo do target.",
        "source_hint": "config/targets.toml",
        "required": False,
        "default": True,
    },
    {
        "name": "property_scope",
        "label": "Escopo do imovel",
        "group": "origem",
        "description": "Categoria configurada para o target.",
        "source_hint": "config/targets.toml",
        "required": False,
        "default": False,
    },
    {
        "name": "page_number",
        "label": "Pagina descoberta",
        "group": "origem",
        "description": "Numero da pagina da listagem onde o anuncio foi encontrado.",
        "source_hint": "URL da pagina de resultados",
        "required": False,
        "default": False,
    },
    {
        "name": "page_url",
        "label": "URL da listagem",
        "group": "origem",
        "description": "Pagina de resultados onde o anuncio foi indexado.",
        "source_hint": "URL corrente do discover",
        "required": False,
        "default": False,
    },
    {
        "name": "position",
        "label": "Posicao na pagina",
        "group": "origem",
        "description": "Ordem relativa do link no HTML da listagem.",
        "source_hint": "sequencia dos href /imovel/ encontrados",
        "required": False,
        "default": False,
    },
    {
        "name": "final_url",
        "label": "URL final",
        "group": "origem",
        "description": "URL final sem query nem fragmentos.",
        "source_hint": "URL final da resposta",
        "required": False,
        "default": True,
    },
    {
        "name": "html_snapshot_path",
        "label": "Snapshot HTML",
        "group": "origem",
        "description": "Caminho para o HTML guardado localmente.",
        "source_hint": "data/html/<listing_id>.html",
        "required": False,
        "default": True,
    },
    {
        "name": "challenge_detected",
        "label": "Desafio detetado",
        "group": "tecnico",
        "description": "Flag interna para HTML de bloqueio/anti-bot.",
        "source_hint": "heuristica sobre o HTML recebido",
        "required": False,
        "default": False,
    },
    {
        "name": "html_sha256",
        "label": "Hash do HTML",
        "group": "tecnico",
        "description": "Hash do HTML para dedupe ou reprocessamento.",
        "source_hint": "gerado pelo scraper a partir do HTML",
        "required": False,
        "default": False,
    },
    {
        "name": "meta",
        "label": "Meta tags",
        "group": "tecnico",
        "description": "Mapa cru das meta tags da pagina.",
        "source_hint": "tags <meta property=...> e <meta name=...>",
        "required": False,
        "default": False,
    },
    {
        "name": "json_ld",
        "label": "JSON-LD",
        "group": "tecnico",
        "description": "Blocos JSON-LD encontrados na pagina.",
        "source_hint": "scripts type=application/ld+json",
        "required": False,
        "default": False,
    },
    {
        "name": "page_text_excerpt",
        "label": "Excerto de texto",
        "group": "tecnico",
        "description": "Excerto do texto visivel da pagina para debug rapido.",
        "source_hint": "texto combinado do body",
        "required": False,
        "default": False,
    },
)

OUTPUT_FIELD_NAMES = {spec["name"] for spec in OUTPUT_FIELD_SPECS}
DEFAULT_OUTPUT_FIELDS = [spec["name"] for spec in OUTPUT_FIELD_SPECS if spec["default"]]
REQUIRED_OUTPUT_FIELDS = [spec["name"] for spec in OUTPUT_FIELD_SPECS if spec["required"]]


def list_output_field_specs() -> list[dict[str, Any]]:
    return [dict(spec) for spec in OUTPUT_FIELD_SPECS]


def normalize_output_fields(selected_fields: list[str] | None = None) -> list[str]:
    requested = list(selected_fields or DEFAULT_OUTPUT_FIELDS)
    normalized: list[str] = []
    for field_name in [*REQUIRED_OUTPUT_FIELDS, *requested]:
        if field_name not in OUTPUT_FIELD_NAMES or field_name in normalized:
            continue
        normalized.append(field_name)
    return normalized


def load_output_selection(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "exists": False,
            "selected_fields": normalize_output_fields(),
            "updated_at": None,
            "version": 1,
        }

    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "exists": True,
        "selected_fields": normalize_output_fields(payload.get("selected_fields")),
        "updated_at": payload.get("updated_at"),
        "version": payload.get("version", 1),
    }


def save_output_selection(path: Path, selected_fields: list[str] | None) -> dict[str, Any]:
    payload = {
        "selected_fields": normalize_output_fields(selected_fields),
        "updated_at": utc_now_iso(),
        "version": 1,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def filter_output_record(record: dict[str, Any], selected_fields: list[str] | None) -> dict[str, Any]:
    chosen = normalize_output_fields(selected_fields)
    filtered: dict[str, Any] = {}
    for spec in OUTPUT_FIELD_SPECS:
        field_name = spec["name"]
        if field_name not in chosen:
            continue
        filtered[field_name] = record.get(field_name)

    for key, value in record.items():
        if key not in filtered and key not in OUTPUT_FIELD_NAMES:
            filtered[key] = value
    return filtered


def read_last_jsonl_record(path: Path) -> dict[str, Any] | None:
    last_record = None
    for record in read_jsonl(path) or []:
        last_record = record
    return last_record


@dataclass
class ResumeState:
    discovered_pages: set[str] = field(default_factory=set)
    indexed_listing_ids: set[str] = field(default_factory=set)
    completed_listing_ids: set[str] = field(default_factory=set)
    failure_counts: Counter[str] = field(default_factory=Counter)


class Journal:
    def __init__(self, path: Path, run_id: str) -> None:
        self.path = path
        self.run_id = run_id

    def record(self, event: str, **payload) -> None:
        append_jsonl(
            self.path,
            {
                "event": event,
                "run_id": self.run_id,
                "ts": utc_now_iso(),
                **payload,
            },
        )


def build_resume_state(index_path: Path, details_path: Path, journal_path: Path) -> ResumeState:
    state = ResumeState()

    for record in read_jsonl(index_path) or []:
        listing_id = record.get("listing_id")
        if listing_id:
            state.indexed_listing_ids.add(str(listing_id))

    for record in read_jsonl(details_path) or []:
        listing_id = record.get("listing_id")
        if listing_id:
            state.completed_listing_ids.add(str(listing_id))

    for record in read_jsonl(journal_path) or []:
        event = record.get("event")
        page_url = record.get("page_url")
        listing_id = record.get("listing_id")

        if event == "page_done" and page_url:
            state.discovered_pages.add(page_url)
        if event == "detail_failed" and listing_id:
            state.failure_counts[str(listing_id)] += 1
        if event == "detail_done" and listing_id:
            state.failure_counts.pop(str(listing_id), None)

    return state


def load_proxy_entries(path: str | Path) -> list[str | dict[str, Any]]:
    file_path = Path(path).expanduser()
    if not file_path.exists():
        raise RuntimeError(f"Ficheiro de proxies nao encontrado: {file_path}")

    proxies: list[str | dict[str, Any]] = []
    for line_number, raw_line in enumerate(file_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("{"):
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Linha {line_number} invalida em {file_path}: JSON mal formado.") from exc
            if not isinstance(parsed, dict) or "server" not in parsed:
                raise RuntimeError(f"Linha {line_number} invalida em {file_path}: JSON tem de incluir 'server'.")
            proxies.append(parsed)
            continue

        proxies.append(line)

    if not proxies:
        raise RuntimeError(f"O ficheiro de proxies esta vazio: {file_path}")
    return proxies


class ScraplingClient:
    def __init__(self, fetch_config: FetchConfig) -> None:
        self.fetch_config = fetch_config
        self._manager = None
        self._session = None

    def _build_proxy_options(self, proxy_rotator_cls):
        if self.fetch_config.proxies_file:
            proxies = load_proxy_entries(self.fetch_config.proxies_file)
            if len(proxies) == 1:
                return {"proxy": proxies[0]}
            return {"proxy_rotator": proxy_rotator_cls(proxies)}

        if self.fetch_config.proxy:
            return {"proxy": self.fetch_config.proxy}
        return {}

    def __enter__(self):
        try:
            from scrapling.fetchers import ProxyRotator

            proxy_options = self._build_proxy_options(ProxyRotator)
            if self.fetch_config.mode == "http":
                from scrapling.fetchers import FetcherSession

                self._manager = FetcherSession(
                    headers={"Accept-Language": self.fetch_config.locale},
                    retries=3,
                    timeout=max(1, int(self.fetch_config.timeout_ms / 1000)),
                    **proxy_options,
                )
            elif self.fetch_config.mode == "dynamic":
                from scrapling.fetchers import DynamicSession

                kwargs = {
                    "headless": self.fetch_config.headless,
                    "disable_resources": self.fetch_config.disable_resources,
                    "google_search": self.fetch_config.google_search,
                    "locale": self.fetch_config.locale,
                    "timeout": self.fetch_config.timeout_ms,
                }
                if self.fetch_config.user_data_dir:
                    kwargs["user_data_dir"] = self.fetch_config.user_data_dir
                if self.fetch_config.cdp_url:
                    kwargs["cdp_url"] = self.fetch_config.cdp_url
                if self.fetch_config.real_chrome:
                    kwargs["real_chrome"] = True
                kwargs.update(proxy_options)
                self._manager = DynamicSession(**kwargs)
            else:
                from scrapling.fetchers import StealthySession

                kwargs = {
                    "headless": self.fetch_config.headless,
                    "disable_resources": self.fetch_config.disable_resources,
                    "google_search": self.fetch_config.google_search,
                    "timeout": self.fetch_config.timeout_ms,
                    "locale": self.fetch_config.locale,
                    "solve_cloudflare": self.fetch_config.solve_cloudflare,
                    "humanize": self.fetch_config.humanize,
                }
                if self.fetch_config.user_data_dir:
                    kwargs["user_data_dir"] = self.fetch_config.user_data_dir
                if self.fetch_config.cdp_url:
                    kwargs["cdp_url"] = self.fetch_config.cdp_url
                if self.fetch_config.real_chrome:
                    kwargs["real_chrome"] = True
                kwargs.update(proxy_options)
                self._manager = StealthySession(**kwargs)
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "A dependencia 'scrapling' nao esta instalada. Corre 'pip install -e .' e depois 'scrapling install'."
            ) from exc

        self._session = self._manager.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._manager is None:
            return False
        return self._manager.__exit__(exc_type, exc, tb)

    def fetch(self, url: str, *, wait_ms: int | None = None, page_action=None):
        if self._session is None:
            raise RuntimeError("A sessao do Scrapling ainda nao foi aberta.")
        if self.fetch_config.mode == "http":
            return self._session.get(url)

        kwargs = {"wait": self.fetch_config.wait_ms if wait_ms is None else wait_ms}
        if self.fetch_config.network_idle:
            kwargs["network_idle"] = True
        if page_action is not None:
            kwargs["page_action"] = page_action
        return self._session.fetch(url, **kwargs)
