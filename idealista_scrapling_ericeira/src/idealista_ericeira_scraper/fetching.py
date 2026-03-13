from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from idealista_ericeira_scraper.config import FetchConfig


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
