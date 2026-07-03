import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import (
    PreprocessingConfigError,
    PreprocessingColumnNotFoundError,
)

PROCESSOR_NAME = "url_events"

# Matches 2-letter BCP-47 locale prefixes commonly used in URL paths:
# e.g. en, fr, de, zh-cn, fr-ca, zh-hant.
# Deliberately uses exactly 2 base letters so common path segments like
# "api" or "img" (3 letters) are not misidentified as locales.
_LOCALE_RE = re.compile(r"^[a-z]{2}(-[a-z]{2,4})*$")


def _normalize_path(path: str) -> str:
    """Normalize a node path: strip leading/trailing slashes, lowercase."""
    result = path.strip().lower().strip("/")
    return result or ("/" if path.strip() else "")


def _strip_locale(path: str) -> str:
    """
    Remove locale prefix from the first path segment when it matches a
    2-letter BCP-47 tag (e.g. 'en', 'fr-ca').  Input must already be
    lowercased and stripped of leading slashes.
    """
    first, _, rest = path.partition("/")
    if _LOCALE_RE.match(first):
        return rest or "/"
    return path


def _parse_url_parts(
    url: str,
    strip_host: bool = True,
    strip_cgi: bool = True,
    strip_locale: bool = True,
) -> Tuple[str, str, str, str]:
    """
    Parse a URL column value into (effective_path, host, query, locale).

    Always extracts host/query/locale regardless of strip_* flags so that
    the *_col outputs can be populated even when the corresponding strip_*
    flag is False.

    Effective path computation:
      strip_host=True  → use urlparse().path (removes scheme + netloc)
      strip_host=False, strip_cgi=True  → same path component, query removed
      strip_host=False, strip_cgi=False → path + "?" + query if query exists
    """
    raw = str(url)

    try:
        parsed = urlparse(raw)
        host = parsed.netloc or ""
        query = parsed.query or ""
        parsed_path = parsed.path or ""
    except Exception:
        stripped = raw.lower().strip("/")
        return stripped or "/", "", "", ""

    # Build effective path
    if strip_host or strip_cgi:
        # In both cases we want the path without the query string.
        # urlparse().path never contains the query, so this covers:
        #   strip_host=True  (any strip_cgi)
        #   strip_host=False, strip_cgi=True
        path = parsed_path
    else:
        # strip_host=False, strip_cgi=False: keep query in the path
        path = parsed_path + ("?" + query if query else "")

    # Normalize: lowercase, strip leading/trailing slashes
    path = path.lower().strip("/")
    if not path and parsed_path:
        path = "/"

    # Locale: always extract (so locale_col can be populated regardless of
    # strip_locale), but only modify path when strip_locale=True.
    locale = ""
    if path:
        first, _, rest = path.partition("/")
        if _LOCALE_RE.match(first):
            locale = first
            if strip_locale:
                path = _strip_locale(path)

    return path, host, query, locale


def _parse_effective_path(
    url: str,
    strip_host: bool = True,
    strip_cgi: bool = True,
    strip_locale: bool = True,
) -> str:
    """Convenience wrapper returning only the effective path."""
    path, _, _, _ = _parse_url_parts(url, strip_host, strip_cgi, strip_locale)
    return path


def _build_event_name(
    effective_path: str,
    cut_node: dict,
    nodes_by_path: Dict[str, dict],
) -> Tuple[str, Optional[str]]:
    """
    Recursively build the new event name for effective_path starting from
    cut_node.  Returns (event_name, slug).

    Case A – URL exactly equals the cut point → (cut_path, None).
    Case B – URL is deeper than the cut point → look up the first child segment.
      * If that child node exists and is itself a cut → recurse (exception cut).
      * Otherwise → (cut_path + "/" + slug_name, slug_name).
    """
    cut_path = cut_node["_norm_path"]
    slug_default = (cut_node.get("custom_name") or "").strip() or "sub-page"

    # Case A
    if effective_path == cut_path:
        return cut_path, None

    # Case B
    tail = effective_path[len(cut_path) + 1 :]  # strip "cut_path/"
    first_seg = tail.split("/")[0]
    child_path = cut_path + "/" + first_seg

    child_node = nodes_by_path.get(child_path)

    if child_node is not None and child_node["is_cut"]:
        # Exception cut — recurse with child as new cut node
        return _build_event_name(effective_path, child_node, nodes_by_path)

    slug_name = (child_node.get("custom_name") or "").strip() if child_node else ""
    if not slug_name:
        slug_name = slug_default
    return cut_path + "/" + slug_name, slug_name


class UrlEvents(DataProcessor):
    """
    Transforms raw URL events into structured event names based on a URL
    path tree configured by the user.  Optionally saves intermediate
    extraction results (host, query string, locale, slug) to separate columns.

    Args:
        column:       Column with the raw URL.
        nodes:        URL tree node list.  Each node must have a 'path' (str)
                      and may have 'is_cut', 'is_deleted', 'custom_name'.
        strip_host:   Extract only the pathname (strip scheme + host). Default True.
        strip_cgi:    Remove query string and URL fragment.  Default True.
        strip_locale: Remove first path segment when it is a 2-letter BCP-47
                      locale tag (e.g. 'en', 'fr-ca').  Default True.
        slug_enabled: When False, cut nodes are ignored.  Default True.
        host_col:     Column name to write the extracted hostname.
        cgi_col:      Column name to write the extracted query string.
        locale_col:   Column name to write the extracted locale code.
        slug_col:     Column name to write the slug value used in the event
                      name (empty string when no slug was applied).
    """

    def __init__(
        self,
        column: str,
        nodes: List[dict],
        strip_host: bool = True,
        strip_cgi: bool = True,
        strip_locale: bool = True,
        slug_enabled: bool = True,
        host_col: Optional[str] = None,
        cgi_col: Optional[str] = None,
        locale_col: Optional[str] = None,
        slug_col: Optional[str] = None,
    ) -> None:
        if not isinstance(column, str) or not column:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'column' must be a non-empty string."
            )
        if not isinstance(nodes, list):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'nodes' must be a list."
            )
        for i, node in enumerate(nodes):
            if not isinstance(node, dict):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, f"Node at index {i} must be a dict."
                )
            if "path" not in node or not isinstance(node["path"], str):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"Node at index {i} must have a string 'path' field.",
                )

        self.column = column
        self._strip_host = strip_host
        self._strip_cgi = strip_cgi
        self._strip_locale = strip_locale
        self._slug_enabled = slug_enabled
        self._host_col = host_col or None
        self._cgi_col = cgi_col or None
        self._locale_col = locale_col or None
        self._slug_col = slug_col or None

        # Preprocess and sort nodes by path length descending (longest = most specific first)
        processed: List[dict] = []
        for node in nodes:
            n = dict(node)
            n["_norm_path"] = _normalize_path(n["path"])
            n["is_cut"] = bool(n.get("is_cut", False))
            n["is_deleted"] = bool(n.get("is_deleted", False))
            processed.append(n)

        processed.sort(key=lambda n: len(n["_norm_path"]), reverse=True)

        self._nodes = processed
        self._nodes_by_path: Dict[str, dict] = {n["_norm_path"]: n for n in processed}

        super().__init__()

    # ------------------------------------------------------------------
    # Core path transformation
    # ------------------------------------------------------------------

    def _transform_path(
        self, effective_path: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Return (new_event, slug) for effective_path.

        new_event is None when the row should be deleted.
        slug is None when no cut-node slug was applied (unchanged paths,
        case-A exact matches, deleted rows).

        Expects effective_path to be pre-normalised (lowercase, no
        leading/trailing slashes) — as produced by _parse_url_parts.
        """
        # Step 2: Deletion check (longest matching deleted prefix wins)
        for node in self._nodes:
            if not node["is_deleted"]:
                continue
            p = node["_norm_path"]
            if effective_path == p or effective_path.startswith(p + "/"):
                return None, None

        # slug_enabled=False → skip cut-node logic entirely
        if not self._slug_enabled:
            return effective_path, None

        # Step 3: Find the deepest cut node whose path is a prefix
        cut_node = None
        for node in self._nodes:
            if not node["is_cut"]:
                continue
            p = node["_norm_path"]
            if effective_path == p or effective_path.startswith(p + "/"):
                cut_node = node
                break  # sorted desc → first match is the longest

        if cut_node is None:
            return effective_path, None  # no matching cut node → unchanged

        # Step 4: Build new event name
        return _build_event_name(effective_path, cut_node, self._nodes_by_path)

    # ------------------------------------------------------------------
    # DataProcessor interface
    # ------------------------------------------------------------------

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.column not in df.columns:
            raise PreprocessingColumnNotFoundError(
                PROCESSOR_NAME, self.column, df.columns.tolist()
            )

        event_col = schema.event_col
        if event_col not in df.columns:
            raise PreprocessingColumnNotFoundError(
                PROCESSOR_NAME, event_col, df.columns.tolist()
            )

        df = df.copy()

        # Parse each URL into (path, host, query, locale)
        url_parts = (
            df[self.column]
            .astype(str)
            .apply(
                lambda url: _parse_url_parts(
                    url, self._strip_host, self._strip_cgi, self._strip_locale
                )
            )
        )
        effective_paths = url_parts.apply(lambda p: p[0])
        hosts = url_parts.apply(lambda p: p[1])
        queries = url_parts.apply(lambda p: p[2])
        locales = url_parts.apply(lambda p: p[3])

        # Transform each effective_path → (new_event, slug)
        transform_results = effective_paths.apply(self._transform_path)
        new_events = transform_results.apply(lambda r: r[0])
        slugs = transform_results.apply(lambda r: r[1])

        # Capture original event names before any row filtering
        original_events = df[event_col].astype(str)

        # Filter out rows marked for deletion
        keep_mask = new_events.notna()
        df = df[keep_mask].copy()
        new_events = new_events[keep_mask]
        slugs = slugs[keep_mask]
        hosts = hosts[keep_mask]
        queries = queries[keep_mask]
        locales = locales[keep_mask]
        original_events = original_events[keep_mask]

        # Build final event name.
        # When the URL source column IS the event column, the computed path
        # becomes the new event name directly (no "old_name:/" prefix).
        # Otherwise: "{original_event}:/{computed_path}".
        if self.column == event_col:
            final_events = new_events.astype(str)
        else:
            final_events = original_events + ":/" + new_events.astype(str)
        df[event_col] = final_events.astype("category").cat.as_unordered()

        # Write optional extraction columns and update schema
        extra_cols = [
            (self._host_col, hosts),
            (self._cgi_col, queries),
            (self._locale_col, locales),
            (self._slug_col, slugs.fillna("")),
        ]
        has_new_cols = any(col for col, _ in extra_cols)
        out_schema = schema.copy() if has_new_cols else schema

        for col_name, series in extra_cols:
            if not col_name:
                continue
            df[col_name] = series.astype(str).values
            if col_name not in out_schema.custom_cols:
                out_schema.custom_cols.append(col_name)

        return df, out_schema
