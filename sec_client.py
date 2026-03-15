#!/usr/bin/env python3
"""
Minimal SEC EDGAR client for the oil royalty extractor.

Extracted from bdc_extractor_standalone/src/extraction/sec_api_client.py —
only the pieces needed here: CIK lookup, submissions fetch, index parsing.
"""

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 90

_RETRY_STATUS_CODES = {429, 503, 504}
_RETRY_ATTEMPTS = 4
_RETRY_BACKOFF_BASE = 5.0  # seconds; doubles each retry: 5, 10, 20


def _sec_get(url: str, headers: dict, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """GET with automatic retry on transient SEC errors (503/429/504/timeout)."""
    last_exc: Exception = RuntimeError("No attempt made")
    for attempt in range(_RETRY_ATTEMPTS):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code in _RETRY_STATUS_CODES:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "SEC returned %s for %s; retrying in %.0fs (attempt %d/%d)",
                    resp.status_code, url, wait, attempt + 1, _RETRY_ATTEMPTS,
                )
                time.sleep(wait)
                last_exc = requests.exceptions.HTTPError(
                    f"{resp.status_code} Server Error", response=resp
                )
                continue
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
            logger.warning(
                "SEC request failed (%s) for %s; retrying in %.0fs (attempt %d/%d)",
                type(exc).__name__, url, wait, attempt + 1, _RETRY_ATTEMPTS,
            )
            time.sleep(wait)
            last_exc = exc
    raise last_exc


@dataclass
class FilingDocument:
    """Represents a document within an SEC filing."""
    url: str
    filename: str
    exhibit_type: Optional[str] = None
    description: Optional[str] = None


# Default path: this file's directory / data / company_tickers.json
def _default_tickers_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "company_tickers.json"


class SECAPIClient:
    """Minimal SEC client: CIK lookup + filing index parsing."""

    # Manual overrides for tickers missing from the SEC feed or renamed companies.
    # PHX Minerals Inc. (formerly Panhandle Oil and Gas Company) was acquired by
    # Prairie Operating Co. in Nov 2023; last 10-K under PHX was for FY2022.
    OVERRIDES: Dict[str, Dict[str, Any]] = {
        "PHX": {"cik_str": 315131, "ticker": "PHX", "title": "PHX Minerals Inc."},
    }

    def __init__(
        self,
        user_agent: str = "SEC-API-Client/1.0 (your-email@domain.com)",
        company_tickers_path: Optional[Union[str, Path]] = None,
    ):
        self.headers = {"User-Agent": user_agent}
        self._tickers_path = (
            Path(company_tickers_path) if company_tickers_path else _default_tickers_path()
        )
        self._company_tickers = self._load_tickers()

    # ------------------------------------------------------------------
    # Ticker → CIK
    # ------------------------------------------------------------------

    def _load_tickers(self) -> Dict[str, Any]:
        """Load ticker → CIK map from local JSON, downloading if absent."""
        if self._tickers_path.exists():
            try:
                with open(self._tickers_path, encoding="utf-8") as f:
                    raw = json.load(f)
                ticker_map = self._build_map(raw)
                logger.info("Loaded %d companies from %s", len(ticker_map), self._tickers_path)
                return ticker_map
            except Exception as exc:
                logger.warning("Could not load local tickers: %s", exc)

        url = "https://www.sec.gov/files/company_tickers.json"
        logger.info("Downloading company tickers from %s", url)
        try:
            resp = requests.get(url, headers=self.headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            raw = resp.json()
            ticker_map = self._build_map(raw)
            self._tickers_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._tickers_path, "w", encoding="utf-8") as f:
                json.dump(raw, f)
            logger.info("Saved company tickers to %s", self._tickers_path)
            return ticker_map
        except Exception as exc:
            logger.error("Failed to load company tickers: %s", exc)
            return {}

    def _build_map(self, raw: Dict) -> Dict[str, Any]:
        m: Dict[str, Any] = {}
        for entry in raw.values():
            if isinstance(entry, dict) and entry.get("ticker"):
                m[entry["ticker"]] = entry
        m.update(self.OVERRIDES)
        return m

    def get_cik(self, ticker: str) -> Optional[str]:
        """Return zero-padded 10-digit CIK string, or None."""
        info = self._company_tickers.get(ticker.upper())
        if info:
            cik = str(info["cik_str"]).zfill(10)
            logger.info("Found CIK %s for %s", cik, ticker)
            return cik

        logger.warning("CIK not in cache for %s; trying dynamic lookup.", ticker)
        return self._dynamic_cik_lookup(ticker)

    def _dynamic_cik_lookup(self, ticker: str) -> Optional[str]:
        url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar"
            f"?company={ticker}&owner=exclude&action=getcompany&Find=Search&output=atom"
        )
        try:
            logger.info("Dynamic CIK lookup for %s", ticker)
            resp = requests.get(url, headers=self.headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "xml")
            el = soup.find("CIK")
            if el and el.text:
                cik = el.text.strip().zfill(10)
                self._company_tickers[ticker.upper()] = {"cik_str": int(cik), "ticker": ticker.upper(), "title": ""}
                return cik
            m = re.search(r"/Archives/edgar/data/(\d{10})/", resp.text)
            if m:
                cik = m.group(1).zfill(10)
                self._company_tickers[ticker.upper()] = {"cik_str": int(cik), "ticker": ticker.upper(), "title": ""}
                return cik
            logger.warning("Dynamic lookup found nothing for %s", ticker)
            return None
        except Exception as exc:
            logger.error("Dynamic lookup error for %s: %s", ticker, exc)
            return None

    # ------------------------------------------------------------------
    # Filing index
    # ------------------------------------------------------------------

    def get_documents_from_index(self, index_url: str) -> List[FilingDocument]:
        """Parse a filing index page; return documents sorted by priority."""
        if not index_url:
            return []
        try:
            resp = _sec_get(index_url, headers=self.headers)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")

            documents: List[FilingDocument] = []
            base_url = "https://www.sec.gov"

            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue
                link = cells[2].find("a")
                if not link or not link.get("href"):
                    continue
                doc_path = link.get("href")
                if "ix?doc=" in doc_path:
                    doc_path = doc_path.replace("/ix?doc=", "")
                if not doc_path.startswith("/"):
                    continue
                doc_url = urljoin(base_url, doc_path)
                filename = doc_url.split("/")[-1]
                if self._should_skip(filename):
                    continue

                exhibit_type = cells[3].get_text(strip=True) if len(cells) > 3 else None
                description = cells[4].get_text(strip=True) if len(cells) > 4 else None
                if not exhibit_type:
                    for idx in (0, 1):
                        t = cells[idx].get_text(strip=True) if idx < len(cells) else ""
                        if t.startswith("EX-"):
                            exhibit_type = t
                            break

                documents.append(FilingDocument(url=doc_url, filename=filename,
                                                exhibit_type=exhibit_type, description=description))

            documents.sort(key=self._doc_priority)
            return documents
        except Exception as exc:
            logger.error("Could not parse index %s: %s", index_url, exc)
            return []

    @staticmethod
    def _doc_priority(doc: FilingDocument) -> int:
        exhibit = (doc.exhibit_type or "").lower()
        if exhibit in ("10-k", "10-q", "8-k", "20-f", "40-f", "10-k/a", "10-q/a"):
            return 0
        fn = doc.filename.lower()
        if any(k in fn for k in ("s-3.htm", "s-1.htm", "424b5.htm", "prospectus")):
            return 1
        desc = (doc.description or "").lower()
        if any(k in desc for k in ("prospectus", "indenture", "securities")):
            return 2
        if exhibit.startswith("ex-") and any(n in exhibit for n in ("4.", "3.", "10.")):
            return 3
        return 4

    @staticmethod
    def _should_skip(filename: str) -> bool:
        fn = filename.lower()
        skip_exts = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".tiff",
                     ".webp", ".zip", ".rar", ".tar", ".gz",
                     ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx")
        if any(fn.endswith(e) for e in skip_exts):
            return True
        if any(p in fn for p in ("logo", "image_", "graphic")):
            return True
        if any(p in fn for p in ("_cal.xml", "_def.xml", "_lab.xml", "_pre.xml", ".xsd")):
            return True
        return False
