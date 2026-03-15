#!/usr/bin/env python3
"""
Oil Royalty Trust — Standardized Measure Extractor

Downloads 10-K filings for oil royalty trusts and extracts the
"Standardized Measure of Discounted Future Net Cash Flows" section.

Outputs per ticker per year:
  output/{TICKER}/{filing_date}_standardized_measure.txt   — clean text with table rendering
  output/{TICKER}/{filing_date}_standardized_measure.html  — raw HTML (tables preserved)
"""

import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict

from bs4 import BeautifulSoup, Tag

from sec_client import SECAPIClient, _sec_get

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OIL_ROYALTY_TICKERS = [
    # Classic oil/gas royalty trusts
    "PBT",    # Permian Basin Royalty Trust
    "SBR",    # Sabine Royalty Trust
    "SJT",    # San Juan Basin Royalty Trust (gas-heavy)
    "CRT",    # Cross Timbers Royalty Trust
    "PVL",    # Permianville Royalty Trust
    "PRT",    # PermRock Royalty Trust
    "VOC",    # VOC Energy Trust
    "MVO",    # MV Oil Trust (terminates July 2026)
    "MTR",    # Mesa Royalty Trust
    # "NRT",  # North European Oil Royalty Trust — no SFAS 69 disclosure (European structure)
    # "MARPS",  # Marine Petroleum Trust — explicitly states no SFAS 69 data available
    "HGT",    # Hugoton Royalty Trust
    # "BPT",  # BP Prudhoe Bay Royalty Trust — terminated 2023
    # Mineral rights / royalty aggregators
    "BSM",    # Black Stone Minerals Company LP
    "VNOM",   # Viper Energy Inc (Diamondback subsidiary, Permian)
    "DMLP",   # Dorchester Minerals LP
    "PHX",    # PHX Minerals
    # Broader royalty / resource plays (standardized measure may be partial)
    "TPL",    # Texas Pacific Land (surface rights + royalties)
    "NRP",    # Natural Resource Partners (primarily coal + some oil/gas)
    # Iron ore royalty — no oil/gas standardized measure, included for completeness
    # "MSB",  # Mesabi Trust (iron ore, not oil/gas — section will not be found)
]

SECTION_PHRASE = "STANDARDIZED MEASURE OF DISCOUNTED"

END_MARKERS = [
    "SIGNATURES",
    "EXHIBIT INDEX",
    "EXHIBIT LIST",
]

FALLBACK_CHARS = 60_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filing discovery
# ---------------------------------------------------------------------------

def get_all_10k_filings(
    client: SECAPIClient,
    ticker: str,
    years_back: int = 3,
) -> List[Tuple[str, str]]:
    """
    Return [(filing_date, index_url), ...] for all 10-K filings within
    the last `years_back` years, most-recent first.
    """
    cik = client.get_cik(ticker)
    if not cik:
        logger.warning(f"{ticker}: CIK not found, skipping.")
        return []

    cutoff = (datetime.now() - timedelta(days=365 * years_back)).date()

    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = _sec_get(url, headers=client.headers)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.error(f"{ticker}: submissions fetch failed — {exc}")
        return []

    recent = data["filings"]["recent"]
    results = []

    for i, form in enumerate(recent["form"]):
        if form != "10-K":
            continue
        report_date_str = recent.get("reportDate", [None])[i]
        if not report_date_str:
            continue
        try:
            report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if report_date < cutoff:
            continue

        accession = recent["accessionNumber"][i]
        accession_nodash = accession.replace("-", "")
        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}"
            f"/{accession_nodash}/{accession}-index.html"
        )
        results.append((report_date_str, index_url))

    results.sort(key=lambda x: x[0], reverse=True)
    logger.info(f"{ticker}: found {len(results)} 10-K filing(s) within {years_back} year(s).")
    return results


# ---------------------------------------------------------------------------
# Document fetch
# ---------------------------------------------------------------------------

def fetch_main_document_html(
    client: SECAPIClient,
    index_url: str,
    ticker: str,
) -> Optional[str]:
    """
    Fetch the raw HTML of the primary 10-K document from the filing index.
    Returns raw HTML string, or None on failure.
    """
    documents = client.get_documents_from_index(index_url)
    if not documents:
        logger.warning(f"{ticker}: no documents found at {index_url}")
        return None

    # get_documents_from_index already sorts by doc_priority; the 10-K main
    # doc has priority 0 (exhibit_type == '10-K').  Fall back to first .htm.
    main_doc = None
    for doc in documents:
        et = (doc.exhibit_type or "").upper()
        if et in ("10-K", "10-K/A"):
            main_doc = doc
            break
    if main_doc is None:
        for doc in documents:
            if doc.filename.lower().endswith((".htm", ".html")):
                main_doc = doc
                break
    if main_doc is None:
        logger.warning(f"{ticker}: could not identify main 10-K document at {index_url}")
        return None

    logger.info(f"{ticker}: fetching {main_doc.url}")
    try:
        resp = _sec_get(main_doc.url, headers=client.headers)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        logger.error(f"{ticker}: document fetch failed — {exc}")
        return None


# ---------------------------------------------------------------------------
# Table rendering (for .txt output)
# ---------------------------------------------------------------------------

def _render_table_as_text(table_tag: Tag) -> str:
    """Convert a <table> element to a plain-text grid."""
    rows_data = []
    for tr in table_tag.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
        rows_data.append(cells)

    if not rows_data:
        return ""

    col_count = max(len(r) for r in rows_data)
    # Pad rows to same width
    for row in rows_data:
        while len(row) < col_count:
            row.append("")

    col_widths = [0] * col_count
    for row in rows_data:
        for ci, cell in enumerate(row):
            col_widths[ci] = max(col_widths[ci], len(cell))

    lines = []
    for row in rows_data:
        parts = []
        for ci, cell in enumerate(row):
            parts.append(cell.ljust(col_widths[ci]))
        lines.append("  ".join(parts).rstrip())
    return "\n".join(lines)


def _soup_to_clean_text(container_nodes: List[Tag]) -> str:
    """
    Convert a list of soup nodes to clean text, rendering <table> elements
    as aligned plain-text grids instead of a soup of concatenated cell values.
    """
    parts = []
    for node in container_nodes:
        if not isinstance(node, Tag):
            # NavigableString
            text = str(node).strip()
            if text:
                parts.append(text)
            continue

        if node.name == "table":
            rendered = _render_table_as_text(node)
            if rendered:
                parts.append(rendered)
        else:
            # For non-table block elements, recurse over children so embedded
            # tables are still rendered nicely.
            has_table = node.find("table")
            if has_table:
                parts.append(_soup_to_clean_text(list(node.children)))
            else:
                text = node.get_text(" ", strip=True)
                if text:
                    parts.append(text)

    return "\n\n".join(p for p in parts if p.strip())


# ---------------------------------------------------------------------------
# Section extraction
# ---------------------------------------------------------------------------

def extract_standardized_measure_section(
    raw_html: str,
) -> Optional[Dict[str, str]]:
    """
    Locate the "Standardized Measure of Discounted Future Net Cash Flows"
    section in a 10-K HTML document.

    Returns {"text": str, "html": str} or None if section not found.

    Strategy:
    1. Find the NavigableString containing the section phrase.
    2. Walk up to the nearest top-level child of <body> (or <html>).
    3. Collect that element + subsequent siblings until an end marker or
       FALLBACK_CHARS of accumulated text is reached.
    4. Render collected top-level nodes: HTML verbatim, text with table grids.
    """
    import re as _re

    soup = BeautifulSoup(raw_html, "html.parser")

    phrase_re = _re.compile(_re.escape(SECTION_PHRASE), _re.IGNORECASE)

    # Find ALL NavigableStrings containing the phrase; we want the one that
    # heads the actual financial section (not glossary entries).  Heuristic:
    # walk candidates in order and pick the first one where a <table> tag
    # appears within the next ~30 siblings of the anchor element.
    all_text_nodes = soup.find_all(string=phrase_re)
    if not all_text_nodes:
        return None

    body = soup.find("body") or soup

    BLOCK_NAMES = {"div", "p", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6"}

    def _section_anchor(tn: Tag) -> Tag:
        """Return the narrowest block-level ancestor of tn.
        Stopping early keeps the anchor tight (just the heading element).
        The collection loop's level-climbing logic reaches the tables."""
        a = tn
        while a.parent and a.parent is not body:
            a = a.parent
            if getattr(a, "name", None) in BLOCK_NAMES:
                return a  # stop at the first (narrowest) block ancestor
        return a  # fallback: body-level child

    def _looks_like_heading(s: str) -> bool:
        """Heuristic: section headings start with a capital letter (or are ALL CAPS),
        don't begin with common sentence starters, and don't end with a period."""
        s = s.strip()
        if not s:
            return False
        if len(s) > 200:  # prose sentences are long; headings are short
            return False
        if s.endswith("."):  # sentences end with periods; headings don't
            return False
        # Quoted definitions: "Standardized measure means..." — not a heading
        if s[0] in ('"', '"', "'", "\u2018", "\u201c"):
            return False
        if s[0].islower():   # headings start with capital (or digit)
            return False
        # Common sentence openers that signal prose, not a heading
        prose_starts = ("the ", "a ", "an ", "in ", "for ", "this ", "these ",
                        "it ", "as ", "such ", "all ", "each ", "subsequent ",
                        "following ", "based on ", "information ")
        if s.lower().startswith(prose_starts):
            return False
        return True

    text_node = None
    anchor = None

    def _in_table_cell(tn: Tag) -> bool:
        """Return True if tn is nested inside a <td> or <th> (table row cell)."""
        p = tn.parent
        while p and p is not body:
            if getattr(p, "name", None) in ("td", "th"):
                return True
            p = p.parent
        return False

    # Priority 1: ALL-CAPS heading with a sibling table
    for candidate in all_text_nodes:
        s = str(candidate).strip()
        if s.upper() == s and SECTION_PHRASE in s.upper() and not _in_table_cell(candidate):
            a = _section_anchor(candidate)
            text_node, anchor = candidate, a
            break

    # Priority 2: Title-case heading (not prose) with a sibling table
    if text_node is None:
        for candidate in all_text_nodes:
            s = str(candidate).strip()
            if _looks_like_heading(s) and SECTION_PHRASE.lower() in s.lower() and not _in_table_cell(candidate):
                a = _section_anchor(candidate)
                text_node, anchor = candidate, a
                break

    # Priority 3: any non-cell candidate whose section anchor has a nearby table.
    # Among candidates, prefer deeper anchors (not direct body child) since financial
    # notes come deeper in the document; as a tiebreaker, prefer the last occurrence.
    if text_node is None:
        deep_match = None   # anchor.parent != body
        flat_match = None   # anchor.parent == body
        for candidate in all_text_nodes:
            if _in_table_cell(candidate):
                continue
            a = _section_anchor(candidate)
            sib = a.next_sibling
            for _ in range(10):
                if sib is None:
                    break
                if hasattr(sib, "find") and sib.find("table"):
                    if a.parent is not body:
                        deep_match = (candidate, a)   # keep last deep match
                    else:
                        flat_match = (candidate, a)   # keep last flat match
                    break
        if deep_match:
            text_node, anchor = deep_match
        elif flat_match:
            text_node, anchor = flat_match

    # Priority 4: table-cell candidates (e.g. row headers in old-format filings) —
    # pick the one whose anchor has the most tables among its siblings.
    if text_node is None:
        best_score = -1
        for candidate in all_text_nodes:
            if not _in_table_cell(candidate):
                continue
            a = _section_anchor(candidate)
            sib = a.next_sibling
            score = 0
            for _ in range(20):
                if sib is None:
                    break
                if hasattr(sib, "find") and sib.find("table"):
                    score += 1
                sib = sib.next_sibling
            if score > best_score:
                best_score = score
                text_node, anchor = candidate, a

    # Fallback: last occurrence
    if text_node is None:
        text_node = all_text_nodes[-1]
        anchor = _section_anchor(text_node)

    # Collect anchor + subsequent siblings until end marker or char limit.
    # When siblings run out at the current level, climb one level and continue
    # from the parent's next sibling — this handles filings where the Changes
    # table is in a separate body-level container from the heading.
    collected: List[Tag] = []
    char_count = 0
    climb_limit = 3   # max levels to climb above the anchor
    # Two-state stop: first detect the changes heading phrase, then stop shortly
    # after we collect the first table that follows it (the actual changes table).
    changes_heading_found = False  # phrase "CHANGES IN STANDARDIZED MEASURE" seen
    changes_found = False          # a table has been collected after the heading
    nodes_after_changes = 0
    MAX_NODES_AFTER_CHANGES = 2   # nodes to collect after the changes table

    current = anchor
    climb_parent = anchor.parent

    while current is not None and char_count < FALLBACK_CHARS:
        should_collect = True  # set to False to skip this node without breaking

        if hasattr(current, "get_text"):
            node_text = current.get_text()
            node_upper = node_text.upper()
            stripped = node_text.strip()

            if collected:
                # Hard end markers
                for marker in END_MARKERS:
                    if marker in node_upper:
                        current = None
                        break
                if current is None:
                    break

                # Skip empty/whitespace-only nodes and bare ToC nav divs — these
                # don't carry content and shouldn't consume the post-changes budget.
                # IMPORTANT: we do NOT use `continue` here so that the advance/climb
                # logic at the bottom of the loop always executes.
                if not stripped or stripped.upper() == "TABLE OF CONTENTS":
                    should_collect = False
                else:
                    # State 1: detect the changes heading phrase (various phrasings).
                    # Normalize whitespace so newlines don't break substring matches.
                    node_upper_ws = " ".join(node_upper.split())
                    if not changes_heading_found and (
                        "CHANGES IN STANDARDIZED MEASURE" in node_upper_ws
                        or "CHANGES IN THE STANDARDIZED MEASURE" in node_upper_ws
                        or "CHANGE IN STANDARDIZED MEASURE" in node_upper_ws
                        or "CHANGE IN THE STANDARDIZED MEASURE" in node_upper_ws
                    ):
                        changes_heading_found = True

                    # State 2: once the heading was seen, trigger after the first
                    # table-containing node (the actual changes data table).
                    if changes_heading_found and not changes_found:
                        has_table = (
                            hasattr(current, "find")
                            and bool(current.find("table"))
                        ) or getattr(current, "name", None) == "table"
                        if has_table:
                            changes_found = True

                    # After the changes table, allow a couple more nodes (subsequent
                    # events paragraph, page numbers) then stop.
                    if changes_found:
                        nodes_after_changes += 1
                        if nodes_after_changes > MAX_NODES_AFTER_CHANGES:
                            break

            if should_collect:
                collected.append(current)
                char_count += len(node_text)

        # Advance to next sibling, climbing levels when siblings are exhausted.
        # Multi-level climb: keep going up until we find a next sibling or run out
        # of budget — a single climb may still land on None if the parent is an
        # only child.
        next_sib = current.next_sibling
        if next_sib is None and climb_limit > 0 and climb_parent is not None and climb_parent is not body:
            found_sib = None
            while climb_limit > 0 and climb_parent is not None and climb_parent is not body:
                candidate = climb_parent.next_sibling
                climb_parent = climb_parent.parent
                climb_limit -= 1
                if candidate is not None:
                    found_sib = candidate
                    break
            current = found_sib
        else:
            current = next_sib

    if not collected:
        return None

    html_out = "\n".join(str(n) for n in collected)
    text_out = _soup_to_clean_text(collected)

    # Safety: if rendered text is tiny, fall back to raw get_text slice
    if len(text_out.strip()) < 100:
        full_text = soup.get_text(separator="\n")
        upper_text = full_text.upper()
        start_idx = upper_text.find(SECTION_PHRASE)
        text_out = full_text[start_idx: start_idx + FALLBACK_CHARS]

    return {"text": text_out, "html": html_out}


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_result(
    output_dir: Path,
    ticker: str,
    filing_date: str,
    section: Dict[str, str],
) -> None:
    ticker_dir = output_dir / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    stem = f"{filing_date}_standardized_measure"

    txt_path = ticker_dir / f"{stem}.txt"
    txt_path.write_text(section["text"], encoding="utf-8")
    logger.info(f"  Saved {txt_path}")

    html_path = ticker_dir / f"{stem}.html"
    html_path.write_text(section["html"], encoding="utf-8")
    logger.info(f"  Saved {html_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract Standardized Measure section from oil royalty trust 10-Ks."
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=OIL_ROYALTY_TICKERS,
        metavar="TICKER",
        help="Ticker symbols to process (default: all OIL_ROYALTY_TICKERS).",
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=3,
        metavar="N",
        help="Number of years of 10-K history to fetch (default: 3).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        metavar="DIR",
        help="Root output directory (default: output/).",
    )
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("SEC_USER_AGENT", "YourName yourname@example.com"),
        help="User-Agent header for SEC requests (or set SEC_USER_AGENT env var).",
    )
    args = parser.parse_args()

    client = SECAPIClient(user_agent=args.user_agent)

    total_saved = 0
    total_missing = 0

    for ticker in args.tickers:
        logger.info(f"=== {ticker} ===")
        filings = get_all_10k_filings(client, ticker, years_back=args.years_back)

        for filing_date, index_url in filings:
            logger.info(f"{ticker} {filing_date}: fetching main document …")
            raw_html = fetch_main_document_html(client, index_url, ticker)
            if raw_html is None:
                total_missing += 1
                continue

            section = extract_standardized_measure_section(raw_html)
            if section is None:
                logger.warning(
                    f"{ticker} {filing_date}: section '{SECTION_PHRASE}' not found."
                )
                total_missing += 1
                continue

            save_result(args.output_dir, ticker, filing_date, section)
            total_saved += 1

    logger.info(
        f"Done. {total_saved} section(s) saved, {total_missing} filing(s) skipped."
    )


if __name__ == "__main__":
    main()
