from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
    disable_resources: bool = False
    timeout_ms: int = 45_000
    wait_ms: int = 2_500
    locale: str = "pt-PT"
    proxy: str = ""


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
    fetch = FetchConfig(**raw.get("fetch", {}))
    targets = [TargetConfig(**item) for item in raw.get("targets", [])]
    if not targets:
        raise ValueError(f"Nenhum target definido em {file_path}")

    paths = ProjectPaths(
        root=project_root,
        config_file=file_path,
        discovery_index=paths.discovery_index,
        details_output=paths.details_output,
        journal_file=paths.journal_file,
        html_snapshots=paths.html_snapshots,
        logs_dir=paths.logs_dir,
    )
    return AppConfig(run=run, fetch=fetch, targets=targets), paths
