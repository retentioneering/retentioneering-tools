"""
Tests for the url_events data processor.

Structure:
  TestNormalization      – _normalize_path, _strip_locale, _parse_url_parts
  TestTransformPath      – _transform_path (unit, no DataFrame)
    TestSpecExamples     – spec examples
    TestExceptionCut     – exception-cut spec examples
    TestEdgeCases        – slug defaults, optional node fields, etc.
  TestDeletion           – is_deleted logic
  TestNoCutNode          – events unchanged when no cut node matches
  TestSlugEnabled        – slug_enabled=False bypasses cut logic
  TestExtractionColumns  – host_col / cgi_col / locale_col / slug_col
  TestUrlEventsApply     – integration via Eventstream.url_events()
  TestUrlEventsValidation – constructor / apply validation errors
"""

import pandas as pd
import pytest

from retentioneering.data_processors.url_events import (
    UrlEvents,
    _normalize_path,
    _parse_effective_path,
    _parse_url_parts,
    _strip_locale,
)
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    PreprocessingConfigError,
    PreprocessingColumnNotFoundError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stream(rows, schema=None):
    """Build an Eventstream from (user_id, page_url, event, timestamp) rows.
    Use a meaningful event name (e.g. 'page_view') rather than the URL, so
    the resulting 'event_name:/computed_path' format is easy to assert on."""
    columns = ["user_id", "page_url", "event", "timestamp"]
    df = pd.DataFrame(rows, columns=columns)
    return Eventstream(df, schema or {"custom_cols": ["page_url"]})


# Node configurations reused across tests (paths WITHOUT leading slash)
NODES_BASIC = [
    {"path": "A", "is_cut": True, "custom_name": "xxx", "is_deleted": False},
    {"path": "A/D", "is_cut": False, "custom_name": "custom-name", "is_deleted": False},
    {"path": "shop", "is_cut": False, "custom_name": None, "is_deleted": True},
]

NODES_EXCEPTION_CUT = [
    {"path": "devices", "is_cut": True, "custom_name": "device", "is_deleted": False},
    {
        "path": "devices/models",
        "is_cut": True,
        "custom_name": "device models",
        "is_deleted": False,
    },
]


def _event(proc: UrlEvents, path: str) -> str | None:
    """Normalize path then return just the event from _transform_path."""
    ep = _parse_effective_path(
        path, strip_host=False, strip_cgi=False, strip_locale=False
    )
    event, _ = proc._transform_path(ep)
    return event


def _slug(proc: UrlEvents, path: str) -> str | None:
    """Normalize path then return just the slug from _transform_path."""
    ep = _parse_effective_path(
        path, strip_host=False, strip_cgi=False, strip_locale=False
    )
    _, slug = proc._transform_path(ep)
    return slug


# ---------------------------------------------------------------------------
# TestNormalization
# ---------------------------------------------------------------------------


class TestNormalization:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("A", "a"),
            ("/A", "a"),
            ("/A/", "a"),
            ("A/D", "a/d"),
            ("/A/D", "a/d"),
            ("devices/models/", "devices/models"),
            ("/", "/"),
            ("", ""),
        ],
    )
    def test_normalize_path(self, raw, expected):
        assert _normalize_path(raw) == expected

    @pytest.mark.parametrize(
        "path,expected",
        [
            ("en/shop/cart", "shop/cart"),
            ("fr-ca/blog", "blog"),
            ("zh-hant/page", "page"),
            ("en", "/"),  # locale-only → root
            ("shop/cart", "shop/cart"),  # no locale
            ("about", "about"),  # no locale
            ("api/v2/users", "api/v2/users"),  # 3-letter segment → not a locale
        ],
    )
    def test_strip_locale(self, path, expected):
        assert _strip_locale(path) == expected

    @pytest.mark.parametrize(
        "url,kw,expected_path,expected_host,expected_query,expected_locale",
        [
            # strip_host=True, strip_cgi=True, strip_locale=True  (all defaults)
            ("/", {}, "/", "", "", ""),
            ("https://x.com/", {}, "/", "x.com", "", ""),
            ("https://x.com/en", {}, "/", "x.com", "", "en"),
            ("/A", {}, "a", "", "", ""),
            ("https://x.com/A/B?ref=1", {}, "a/b", "x.com", "ref=1", ""),
            ("https://x.com/en/shop?a=1", {}, "shop", "x.com", "a=1", "en"),
            # strip_locale=False: locale NOT removed from path, but still captured
            (
                "https://x.com/en/shop",
                {"strip_locale": False},
                "en/shop",
                "x.com",
                "",
                "en",
            ),
            # strip_host=False, strip_cgi=True
            (
                "/shop/cart?foo=bar",
                {"strip_host": False},
                "shop/cart",
                "",
                "foo=bar",
                "",
            ),
            # strip_host=False, strip_cgi=False: query stays in path
            (
                "/shop/cart?foo=bar",
                {"strip_host": False, "strip_cgi": False},
                "shop/cart?foo=bar",
                "",
                "foo=bar",
                "",
            ),
            # locale extraction with strip_locale=False
            (
                "/fr-ca/products",
                {"strip_host": False, "strip_locale": False},
                "fr-ca/products",
                "",
                "",
                "fr-ca",
            ),
        ],
    )
    def test_parse_url_parts(
        self, url, kw, expected_path, expected_host, expected_query, expected_locale
    ):
        path, host, query, locale = _parse_url_parts(url, **kw)
        assert path == expected_path
        assert host == expected_host
        assert query == expected_query
        assert locale == expected_locale


# ---------------------------------------------------------------------------
# TestTransformPath – pure unit tests, no DataFrame
# ---------------------------------------------------------------------------


class TestSpecExamples:
    """Spec table examples (no-leading-slash format)."""

    def setup_method(self):
        self.proc = UrlEvents(column="page_url", nodes=NODES_BASIC)

    def test_case_a_exact_cut(self):
        assert _event(self.proc, "A") == "a"

    def test_case_a_slug_is_none(self):
        assert _slug(self.proc, "A") is None

    def test_case_b_unknown_child(self):
        assert _event(self.proc, "A/B") == "a/xxx"

    def test_case_b_slug_is_custom_name(self):
        assert _slug(self.proc, "A/B") == "xxx"

    def test_case_b_deep_unknown_child(self):
        assert _event(self.proc, "A/B/C") == "a/xxx"

    def test_case_b_known_child_with_custom_name(self):
        assert _event(self.proc, "A/D") == "a/custom-name"

    def test_case_b_known_child_slug(self):
        assert _slug(self.proc, "A/D") == "custom-name"

    def test_case_b_deep_known_child(self):
        assert _event(self.proc, "A/D/E") == "a/custom-name"

    def test_deletion_prefix(self):
        assert _event(self.proc, "shop/cart") is None

    def test_no_cut_node_unchanged(self):
        assert _event(self.proc, "other/page") == "other/page"

    def test_no_cut_node_slug_is_none(self):
        assert _slug(self.proc, "other/page") is None


class TestExceptionCut:
    """Exception-cut spec examples."""

    def setup_method(self):
        self.proc = UrlEvents(column="page_url", nodes=NODES_EXCEPTION_CUT)

    def test_shallow_unknown_child(self):
        assert _event(self.proc, "devices/abc") == "devices/device"

    def test_shallow_slug(self):
        assert _slug(self.proc, "devices/abc") == "device"

    def test_case_a_inner_cut_exact(self):
        assert _event(self.proc, "devices/models") == "devices/models"

    def test_inner_cut_exact_slug_is_none(self):
        assert _slug(self.proc, "devices/models") is None

    def test_inner_cut_unknown_child(self):
        assert _event(self.proc, "devices/models/abc") == "devices/models/device models"

    def test_inner_cut_slug(self):
        assert _slug(self.proc, "devices/models/abc") == "device models"

    def test_inner_cut_deep_unknown_child(self):
        assert (
            _event(self.proc, "devices/models/x/y/z") == "devices/models/device models"
        )


class TestEdgeCases:
    def test_sub_page_default_used_when_no_custom_name(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "blog", "is_cut": True, "custom_name": None},
            ],
        )
        assert _event(proc, "blog/some-post") == "blog/sub-page"
        assert _slug(proc, "blog/some-post") == "sub-page"

    def test_empty_custom_name_treated_as_absent(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "blog", "is_cut": True, "custom_name": "   "},
            ],
        )
        assert _event(proc, "blog/some-post") == "blog/sub-page"

    def test_node_path_with_leading_slash_normalised(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "/section", "is_cut": True, "custom_name": "item"},
            ],
        )
        assert _event(proc, "section/abc") == "section/item"

    def test_case_insensitive_matching(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "Devices", "is_cut": True, "custom_name": "device"},
            ],
        )
        assert _event(proc, "devices/abc") == "devices/device"

    def test_empty_nodes_list(self):
        proc = UrlEvents(column="url", nodes=[])
        assert _event(proc, "any/path") == "any/path"

    def test_optional_node_fields_default_to_false(self):
        """Node with only 'path' should not cut or delete anything."""
        proc = UrlEvents(column="url", nodes=[{"path": "A"}])
        assert _event(proc, "a/b") == "a/b"


# ---------------------------------------------------------------------------
# TestDeletion
# ---------------------------------------------------------------------------


class TestDeletion:
    def test_exact_deleted_path(self):
        proc = UrlEvents(column="url", nodes=[{"path": "shop", "is_deleted": True}])
        assert _event(proc, "shop") is None

    def test_child_of_deleted_path(self):
        proc = UrlEvents(column="url", nodes=[{"path": "shop", "is_deleted": True}])
        assert _event(proc, "shop/cart") is None
        assert _event(proc, "shop/cart/item") is None

    def test_sibling_not_deleted(self):
        proc = UrlEvents(column="url", nodes=[{"path": "shop", "is_deleted": True}])
        assert _event(proc, "shopfront") == "shopfront"

    def test_deleted_takes_priority_over_cut(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "A", "is_cut": True, "custom_name": "x", "is_deleted": True},
            ],
        )
        assert _event(proc, "a/b") is None

    def test_deeper_deleted_node_wins_over_shorter_cut(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "A", "is_cut": True, "custom_name": "x"},
                {"path": "A/B", "is_deleted": True},
            ],
        )
        assert _event(proc, "a/b/c") is None
        assert _event(proc, "a/x") == "a/x"


# ---------------------------------------------------------------------------
# TestNoCutNode
# ---------------------------------------------------------------------------


class TestNoCutNode:
    def test_path_returned_unchanged_when_no_cut(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "A/D", "is_cut": False, "custom_name": "custom"},
            ],
        )
        assert _event(proc, "other/page") == "other/page"

    def test_non_cut_node_does_not_act_as_cut(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "A", "is_cut": False, "custom_name": "something"},
            ],
        )
        assert _event(proc, "a/b") == "a/b"


# ---------------------------------------------------------------------------
# TestSlugEnabled
# ---------------------------------------------------------------------------


class TestSlugEnabled:
    def test_slug_enabled_false_ignores_all_cuts(self):
        proc = UrlEvents(column="url", nodes=NODES_EXCEPTION_CUT, slug_enabled=False)
        assert _event(proc, "devices/abc") == "devices/abc"
        assert _event(proc, "devices/models/abc") == "devices/models/abc"

    def test_slug_enabled_false_still_deletes(self):
        proc = UrlEvents(
            column="url",
            nodes=[
                {"path": "admin", "is_deleted": True},
                {"path": "blog", "is_cut": True, "custom_name": "post"},
            ],
            slug_enabled=False,
        )
        assert _event(proc, "admin/users") is None
        assert _event(proc, "blog/article") == "blog/article"

    def test_slug_enabled_via_eventstream(self):
        stream = _make_stream(
            [
                ["u1", "/devices/abc", "page_view", "2024-01-01"],
                ["u1", "/devices/models/abc", "page_view", "2024-01-02"],
            ]
        )
        res = stream.url_events(
            column="page_url", nodes=NODES_EXCEPTION_CUT, slug_enabled=False
        )
        assert res.df["event"].tolist() == [
            "page_view:/devices/abc",
            "page_view:/devices/models/abc",
        ]


# ---------------------------------------------------------------------------
# TestExtractionColumns
# ---------------------------------------------------------------------------


class TestExtractionColumns:
    def _stream(self, rows):
        return _make_stream(rows)

    def test_host_col_populated(self):
        stream = self._stream(
            [
                ["u1", "https://example.com/A/B", "page_view", "2024-01-01"],
                ["u1", "https://other.org/A/B", "page_view", "2024-01-02"],
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC, host_col="host")
        assert "host" in res.df.columns
        assert res.df["host"].tolist() == ["example.com", "other.org"]

    def test_host_col_empty_when_no_domain(self):
        stream = self._stream([["u1", "/A/B", "page_view", "2024-01-01"]])
        res = stream.url_events(column="page_url", nodes=NODES_BASIC, host_col="host")
        assert res.df["host"].tolist() == [""]

    def test_cgi_col_populated(self):
        stream = self._stream(
            [
                ["u1", "/A/B?ref=home&utm=1", "page_view", "2024-01-01"],
                ["u1", "/A/D", "page_view", "2024-01-02"],
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC, cgi_col="qs")
        assert res.df["qs"].tolist() == ["ref=home&utm=1", ""]

    def test_locale_col_populated(self):
        stream = self._stream(
            [
                ["u1", "/en/A/B", "page_view", "2024-01-01"],
                ["u1", "/fr-ca/A/D", "page_view", "2024-01-02"],
                ["u1", "/A/B", "page_view", "2024-01-03"],
            ]
        )
        res = stream.url_events(
            column="page_url", nodes=NODES_BASIC, locale_col="locale"
        )
        assert res.df["locale"].tolist() == ["en", "fr-ca", ""]

    def test_locale_col_populated_even_when_strip_locale_false(self):
        """locale_col is always filled regardless of strip_locale flag."""
        stream = self._stream([["u1", "/en/A/B", "page_view", "2024-01-01"]])
        res = stream.url_events(
            column="page_url",
            nodes=NODES_BASIC,
            strip_locale=False,
            locale_col="locale",
        )
        assert res.df["locale"].tolist() == ["en"]

    def test_slug_col_populated_for_case_b(self):
        stream = self._stream(
            [
                ["u1", "/A/B", "page_view", "2024-01-01"],  # Case B → slug "xxx"
                [
                    "u1",
                    "/A/D",
                    "page_view",
                    "2024-01-02",
                ],  # Case B → slug "custom-name"
                [
                    "u1",
                    "/A/D/E",
                    "page_view",
                    "2024-01-03",
                ],  # Case B deep → slug "custom-name"
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC, slug_col="slug")
        assert res.df["slug"].tolist() == ["xxx", "custom-name", "custom-name"]

    def test_slug_col_empty_for_unchanged_paths(self):
        stream = self._stream([["u1", "/other/page", "page_view", "2024-01-01"]])
        res = stream.url_events(column="page_url", nodes=NODES_BASIC, slug_col="slug")
        assert res.df["slug"].tolist() == [""]

    def test_slug_col_empty_for_case_a(self):
        """Case A (exact match with cut point) → no slug appended."""
        stream = self._stream([["u1", "/A", "page_view", "2024-01-01"]])
        res = stream.url_events(column="page_url", nodes=NODES_BASIC, slug_col="slug")
        assert res.df["slug"].tolist() == [""]

    def test_slug_col_absent_for_deleted_rows(self):
        """Deleted rows are removed; remaining rows get slug_col."""
        stream = self._stream(
            [
                ["u1", "/A/B", "page_view", "2024-01-01"],
                ["u1", "/shop/cart", "page_view", "2024-01-02"],  # deleted
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC, slug_col="slug")
        assert len(res.df) == 1
        assert res.df["slug"].tolist() == ["xxx"]

    def test_all_cols_together(self):
        """All four extra columns populated in one call."""
        stream = self._stream(
            [
                ["u1", "https://site.com/en/A/B?ref=1", "page_view", "2024-01-01"],
            ]
        )
        res = stream.url_events(
            column="page_url",
            nodes=NODES_BASIC,
            host_col="host",
            cgi_col="qs",
            locale_col="locale",
            slug_col="slug",
        )
        assert res.df["host"].tolist() == ["site.com"]
        assert res.df["qs"].tolist() == ["ref=1"]
        assert res.df["locale"].tolist() == ["en"]
        assert res.df["slug"].tolist() == ["xxx"]

    def test_new_cols_added_to_schema_custom_cols(self):
        """Extra columns are registered in schema.custom_cols."""
        stream = self._stream([["u1", "/A/B", "page_view", "2024-01-01"]])
        res = stream.url_events(
            column="page_url",
            nodes=NODES_BASIC,
            host_col="host",
            slug_col="slug",
        )
        assert "host" in res.schema.custom_cols
        assert "slug" in res.schema.custom_cols

    def test_no_extra_cols_schema_unchanged(self):
        """When no *_col args are given, schema is returned as-is."""
        stream = self._stream([["u1", "/A/B", "page_view", "2024-01-01"]])
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.schema.custom_cols == stream.schema.custom_cols

    def test_exception_cut_slug_col(self):
        """slug_col works correctly through recursive exception-cut path."""
        stream = self._stream(
            [
                ["u1", "/devices/models/abc", "page_view", "2024-01-01"],
            ]
        )
        res = stream.url_events(
            column="page_url",
            nodes=NODES_EXCEPTION_CUT,
            slug_col="slug",
        )
        assert res.df["slug"].tolist() == ["device models"]


# ---------------------------------------------------------------------------
# TestUrlEventsApply – integration via Eventstream
# ---------------------------------------------------------------------------


class TestUrlEventsApply:
    def test_basic_rename(self):
        stream = _make_stream(
            [
                ["u1", "/A", "page_view", "2024-01-01"],
                ["u1", "/A/B", "page_view", "2024-01-02"],
                ["u1", "/A/D", "page_view", "2024-01-03"],
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == [
            "page_view:/a",
            "page_view:/a/xxx",
            "page_view:/a/custom-name",
        ]

    def test_deleted_rows_removed(self):
        stream = _make_stream(
            [
                ["u1", "/A", "page_view", "2024-01-01"],
                ["u1", "/shop/cart", "page_view", "2024-01-02"],
                ["u1", "/A/B", "page_view", "2024-01-03"],
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert len(res.df) == 2
        assert "/shop/cart" not in res.df["page_url"].tolist()

    def test_no_matching_cut_rows_unchanged(self):
        stream = _make_stream([["u1", "/other/page", "page_view", "2024-01-01"]])
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == ["page_view:/other/page"]

    def test_page_url_column_preserved(self):
        stream = _make_stream([["u1", "/A/B", "page_view", "2024-01-01"]])
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert "page_url" in res.df.columns
        assert res.df["page_url"].tolist() == ["/A/B"]

    def test_event_col_is_categorical(self):
        stream = _make_stream([["u1", "/A/B", "page_view", "2024-01-01"]])
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.df["event"].dtype.name == "category"

    def test_original_event_name_used_as_prefix(self):
        """Different original event names produce different prefixes."""
        stream = _make_stream(
            [
                ["u1", "/A/B", "page_view", "2024-01-01"],
                ["u2", "/A/B", "screen_view", "2024-01-02"],
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == [
            "page_view:/a/xxx",
            "screen_view:/a/xxx",
        ]

    def test_full_pipeline_spec_example(self):
        rows = [
            ["u1", "/A", "page_view", "2024-01-01"],
            ["u1", "/A/B", "page_view", "2024-01-02"],
            ["u1", "/A/B/C", "page_view", "2024-01-03"],
            ["u1", "/A/D", "page_view", "2024-01-04"],
            ["u1", "/A/D/E", "page_view", "2024-01-05"],
            ["u1", "/shop/cart", "page_view", "2024-01-06"],
            ["u1", "/other/page", "page_view", "2024-01-07"],
        ]
        stream = _make_stream(rows)
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert len(res.df) == 6
        assert res.df["event"].tolist() == [
            "page_view:/a",
            "page_view:/a/xxx",
            "page_view:/a/xxx",
            "page_view:/a/custom-name",
            "page_view:/a/custom-name",
            "page_view:/other/page",
        ]

    def test_exception_cut_full_pipeline(self):
        rows = [
            ["u1", "/devices/abc", "page_view", "2024-01-01"],
            ["u1", "/devices/models", "page_view", "2024-01-02"],
            ["u1", "/devices/models/abc", "page_view", "2024-01-03"],
            ["u1", "/devices/models/x/y/z", "page_view", "2024-01-04"],
        ]
        stream = _make_stream(rows)
        res = stream.url_events(column="page_url", nodes=NODES_EXCEPTION_CUT)
        assert res.df["event"].tolist() == [
            "page_view:/devices/device",
            "page_view:/devices/models",
            "page_view:/devices/models/device models",
            "page_view:/devices/models/device models",
        ]

    def test_strip_host_strips_scheme_and_domain(self):
        stream = _make_stream(
            [["u1", "https://example.com/A/B", "page_view", "2024-01-01"]]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == ["page_view:/a/xxx"]

    def test_strip_cgi_removes_query_string(self):
        stream = _make_stream(
            [["u1", "/A/B?ref=home&utm=1", "page_view", "2024-01-01"]]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == ["page_view:/a/xxx"]

    def test_strip_cgi_false_keeps_query_string(self):
        """With strip_host=False and strip_cgi=False the query stays in the path.
        Paths not matching any cut node return the full effective_path."""
        stream = _make_stream([["u1", "/other?foo=bar", "page_view", "2024-01-01"]])
        res = stream.url_events(
            column="page_url",
            nodes=NODES_BASIC,
            strip_host=False,
            strip_cgi=False,
        )
        assert res.df["event"].tolist() == ["page_view:/other?foo=bar"]

    def test_strip_locale_removes_locale_segment(self):
        stream = _make_stream(
            [
                ["u1", "/en/A/B", "page_view", "2024-01-01"],
                ["u1", "/fr-ca/A/D", "page_view", "2024-01-02"],
            ]
        )
        res = stream.url_events(column="page_url", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == [
            "page_view:/a/xxx",
            "page_view:/a/custom-name",
        ]

    def test_strip_locale_false_keeps_locale_segment(self):
        stream = _make_stream([["u1", "/en/A/B", "page_view", "2024-01-01"]])
        res = stream.url_events(
            column="page_url", nodes=NODES_BASIC, strip_locale=False
        )
        assert res.df["event"].tolist() == ["page_view:/en/a/b"]

    def test_payload_example_from_spec(self):
        nodes = [
            {"path": "shop", "is_cut": True, "custom_name": "shop page"},
            {"path": "shop/cart", "is_cut": False, "custom_name": "cart"},
            {"path": "blog", "is_cut": True},
            {"path": "admin", "is_deleted": True},
        ]
        rows = [
            ["u1", "/shop", "page_view", "2024-01-01"],
            ["u1", "/shop/new", "page_view", "2024-01-02"],
            ["u1", "/shop/cart", "page_view", "2024-01-03"],
            ["u1", "/shop/cart/checkout", "page_view", "2024-01-04"],
            ["u1", "/blog/post-1", "page_view", "2024-01-05"],
            ["u1", "/admin/users", "page_view", "2024-01-06"],
            ["u1", "/about", "page_view", "2024-01-07"],
        ]
        stream = _make_stream(rows)
        res = stream.url_events(column="page_url", nodes=nodes, strip_locale=False)
        assert len(res.df) == 6
        assert res.df["event"].tolist() == [
            "page_view:/shop",
            "page_view:/shop/shop page",
            "page_view:/shop/cart",
            "page_view:/shop/cart",
            "page_view:/blog/sub-page",
            "page_view:/about",
        ]


# ---------------------------------------------------------------------------
# TestUrlColumnIsEventColumn – when column == event_col, no prefix added
# ---------------------------------------------------------------------------


class TestUrlColumnIsEventColumn:
    """When the URL source column is the same as the schema's event column,
    the event is renamed to the computed path directly (no 'old_name:/' prefix)."""

    def _stream_with_url_as_event(self, rows):
        """Eventstream where the event column itself contains raw URLs."""
        columns = ["user_id", "event", "timestamp"]
        df = pd.DataFrame(rows, columns=columns)
        return Eventstream(df, {})

    def test_no_prefix_when_url_col_is_event_col(self):
        # /A/B  → cut node A, child B has no custom_name → slug "xxx" → "a/xxx"
        # /A/D/E → cut node A, child D has custom_name "custom-name" → "a/custom-name"
        stream = self._stream_with_url_as_event(
            [
                ["u1", "/A/B", "2024-01-01"],
                ["u1", "/A/D/E", "2024-01-02"],
            ]
        )
        res = stream.url_events(column="event", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == ["a/xxx", "a/custom-name"]

    def test_no_prefix_exact_match(self):
        stream = self._stream_with_url_as_event(
            [
                ["u1", "/A", "2024-01-01"],
            ]
        )
        res = stream.url_events(column="event", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == ["a"]

    def test_no_prefix_unchanged_path(self):
        """Paths without a matching cut node are renamed to computed path, not old_name:/path."""
        stream = self._stream_with_url_as_event(
            [
                ["u1", "/other/page", "2024-01-01"],
            ]
        )
        res = stream.url_events(column="event", nodes=NODES_BASIC)
        assert res.df["event"].tolist() == ["other/page"]

    def test_no_prefix_deletion_still_works(self):
        stream = self._stream_with_url_as_event(
            [
                ["u1", "/shop/item", "2024-01-01"],
                ["u1", "/A/B", "2024-01-02"],
            ]
        )
        res = stream.url_events(column="event", nodes=NODES_BASIC)
        assert len(res.df) == 1
        assert res.df["event"].tolist() == ["a/xxx"]


# ---------------------------------------------------------------------------
# TestUrlEventsValidation
# ---------------------------------------------------------------------------


class TestUrlEventsValidation:
    def test_empty_column_name_raises(self):
        with pytest.raises(PreprocessingConfigError):
            UrlEvents(column="", nodes=[])

    def test_non_string_column_raises(self):
        with pytest.raises(PreprocessingConfigError):
            UrlEvents(column=123, nodes=[])  # type: ignore[arg-type]

    def test_nodes_not_a_list_raises(self):
        with pytest.raises(PreprocessingConfigError):
            UrlEvents(column="url", nodes={"path": "A"})  # type: ignore[arg-type]

    def test_node_not_a_dict_raises(self):
        with pytest.raises(PreprocessingConfigError):
            UrlEvents(column="url", nodes=["A"])

    def test_node_missing_path_raises(self):
        with pytest.raises(PreprocessingConfigError):
            UrlEvents(column="url", nodes=[{"is_cut": True}])

    def test_node_path_not_string_raises(self):
        with pytest.raises(PreprocessingConfigError):
            UrlEvents(column="url", nodes=[{"path": 42}])  # type: ignore[arg-type]

    def test_column_not_in_df_raises(self):
        stream = _make_stream([["u1", "/A", "/A", "2024-01-01"]])
        with pytest.raises(PreprocessingColumnNotFoundError):
            stream.url_events(column="nonexistent_col", nodes=NODES_BASIC)
