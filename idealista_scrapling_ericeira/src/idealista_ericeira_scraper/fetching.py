from __future__ import annotations

from idealista_ericeira_scraper.config import FetchConfig


class ScraplingClient:
    def __init__(self, fetch_config: FetchConfig) -> None:
        self.fetch_config = fetch_config
        self._manager = None
        self._session = None

    def __enter__(self):
        try:
            if self.fetch_config.mode == "http":
                from scrapling.fetchers import FetcherSession

                self._manager = FetcherSession(
                    headers={"Accept-Language": self.fetch_config.locale},
                    retries=3,
                    timeout=max(1, int(self.fetch_config.timeout_ms / 1000)),
                )
            elif self.fetch_config.mode == "dynamic":
                from scrapling.fetchers import DynamicSession

                kwargs = {
                    "headless": self.fetch_config.headless,
                    "disable_resources": self.fetch_config.disable_resources,
                    "timeout": self.fetch_config.timeout_ms,
                }
                if self.fetch_config.real_chrome:
                    kwargs["real_chrome"] = True
                if self.fetch_config.proxy:
                    kwargs["proxy"] = self.fetch_config.proxy
                self._manager = DynamicSession(**kwargs)
            else:
                from scrapling.fetchers import StealthySession

                kwargs = {
                    "headless": self.fetch_config.headless,
                    "disable_resources": self.fetch_config.disable_resources,
                    "timeout": self.fetch_config.timeout_ms,
                    "solve_cloudflare": self.fetch_config.solve_cloudflare,
                    "humanize": self.fetch_config.humanize,
                }
                if self.fetch_config.real_chrome:
                    kwargs["real_chrome"] = True
                if self.fetch_config.proxy:
                    kwargs["proxy"] = self.fetch_config.proxy
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

    def fetch(self, url: str):
        if self._session is None:
            raise RuntimeError("A sessao do Scrapling ainda nao foi aberta.")
        if self.fetch_config.mode == "http":
            return self._session.get(url)

        kwargs = {"wait": self.fetch_config.wait_ms}
        if self.fetch_config.network_idle:
            kwargs["network_idle"] = True
        return self._session.fetch(url, **kwargs)
