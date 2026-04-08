#!/usr/bin/env python3
"""Agentic conference directory scraper.

The scraper tries to infer:
1. which links on the seed page are company/profile links,
2. how pagination works for the directory, and
3. which external link on each profile is the company's website.

It is designed for server-rendered directories and "HTML-first" sites.
JS-only apps may still need a browser automation layer.
"""

from __future__ import annotations

import argparse
import csv
from functools import lru_cache
from html import unescape as html_unescape
import json
import re
import subprocess
import sys
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

DEFAULT_DIRECTORY_URL = "https://www.highpointmarket.org/ExhibitorDirectory"
DEFAULT_WORKERS = 8
DEFAULT_MAX_PAGES = 250
DEFAULT_SAMPLE_SIZE = 3
DEFAULT_BROWSER_MODE = "auto"
DEFAULT_BROWSER_TIMEOUT_MS = 25000
MYS_DEFAULT_PAGE_SIZE = 50
EXPOFP_DATA_FILENAME = "data.js"
REQUEST_TIMEOUT_SECONDS = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 1.5
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)

COMMON_PAGE_PARAMS = (
    "page",
    "pageindex",
    "p",
    "pg",
    "paged",
    "page_num",
    "pageid",
    "pagenum",
    "page_no",
)
ID_LIKE_QUERY_KEYS = {
    "id",
    "companyid",
    "exhibitorid",
    "vendorid",
    "listingid",
    "profileid",
    "recordid",
}
SOCIAL_HOST_MARKERS = (
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "youtu.be",
    "pinterest.com",
    "tiktok.com",
    "threads.net",
    "wechat.com",
    "wa.me",
    "whatsapp.com",
)
ASSET_EXTENSIONS = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".css",
    ".js",
    ".json",
    ".xml",
    ".pdf",
    ".zip",
    ".mp4",
    ".mp3",
)
PAGINATION_WORDS = {
    "next",
    "next page",
    "previous",
    "previous page",
    "prev",
    "older",
    "newer",
    "load more",
    "more",
}
BROWSER_LOAD_MORE_RE = re.compile(
    r"\b(?:load more|show more|view more|see more|more results|more exhibitors|"
    r"more vendors|more sponsors|more brands)\b",
    re.IGNORECASE,
)
BROWSER_ACCEPT_RE = re.compile(
    r"\b(?:accept|accept all|allow all|agree|got it|i understand)\b",
    re.IGNORECASE,
)
GENERIC_ANCHOR_PHRASES = {
    "about",
    "about us",
    "account",
    "add to cart",
    "add to mymarket",
    "all",
    "apply",
    "back",
    "blog",
    "browse",
    "calendar",
    "categories",
    "clear",
    "clear all filters",
    "close",
    "contact",
    "contact us",
    "directions",
    "download",
    "events",
    "faq",
    "filters",
    "forgot password",
    "help",
    "home",
    "hotels",
    "info",
    "learn more",
    "login",
    "log in",
    "logout",
    "menu",
    "my account",
    "my market",
    "next page",
    "news",
    "our story",
    "partners",
    "plan your trip",
    "previous page",
    "privacy policy",
    "products",
    "read more",
    "register",
    "registration",
    "search",
    "services",
    "show all",
    "sign in",
    "sign up",
    "skip to main content",
    "skip to navigation",
    "skip to footer",
    "sponsors",
    "travel planning",
    "view all",
    "view details",
}
TOTAL_PAGE_PATTERNS = (
    r"\bPAGINATION_TOTAL\s*=\s*(\d+)\b",
    r'"totalPages"\s*:\s*(\d+)',
    r"\btotalPages\b['\"]?\s*[:=]\s*(\d+)",
    r"\bpageCount\b['\"]?\s*[:=]\s*(\d+)",
    r"\btotal_pages\b['\"]?\s*[:=]\s*(\d+)",
    r"\bpage_count\b['\"]?\s*[:=]\s*(\d+)",
)
WHITESPACE_RE = re.compile(r"\s+")
PAGE_PATH_RE = re.compile(r"/page/(\d+)(?:/)?$", re.IGNORECASE)
DOMAIN_TEXT_RE = re.compile(
    r"^(?:https?://)?(?:www\.)?[a-z0-9][a-z0-9.-]+\.[a-z]{2,}(?:/.*)?$",
    re.IGNORECASE,
)
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
TITLE_SPLIT_RE = re.compile(r"\s*[|\-:\u2013\u2014]\s*")
META_TAG_RE = re.compile(r"<meta\b([^>]+)>", re.IGNORECASE)
META_ATTR_RE = re.compile(
    r"([A-Za-z_:][A-Za-z0-9_.:-]*)\s*=\s*([\"'])(.*?)\2",
    re.IGNORECASE | re.DOTALL,
)
COMPANY_NAME_REGION_MARKERS = {
    "ca",
    "canada",
    "eu",
    "global",
    "na",
    "north america",
    "uk",
    "us",
    "usa",
    "worldwide",
}
COMPANY_NAME_UI_NOISE_RE = re.compile(
    r"\b(?:search|cart|loader|close panel|chevron(?:-[a-z]+)?|"
    r"design trade(?:\s*&\s*contract sales)?|exclamation-circle|menu|open menu|x)\b.*$",
    re.IGNORECASE,
)
COMPANY_NAME_GENERIC_MARKERS = (
    "accessories",
    "bedding",
    "cabinet hardware",
    "decor",
    "exclusive materials",
    "furniture",
    "furnishings",
    "hardwood",
    "hardware",
    "home decor",
    "materials",
    "outdoor",
    "provide versatility",
    "rugs",
    "solid brass",
)
HOST_LABEL_PREFIXES = (
    "go",
    "get",
    "shop",
    "store",
    "visit",
)
META_NAME_WEIGHTS = {
    "og:site_name": 170.0,
    "application-name": 160.0,
    "apple-mobile-web-app-title": 155.0,
    "og:title": 95.0,
    "twitter:title": 85.0,
}
JSON_LD_NAME_WEIGHTS = {
    "brand": 180.0,
    "organization": 175.0,
    "corporation": 170.0,
    "localbusiness": 165.0,
    "store": 160.0,
    "website": 150.0,
    "webpage": 70.0,
}
GENERIC_DIRECTORY_LABELS = {
    "companies",
    "company directory",
    "conference directory",
    "directory",
    "directory listing",
    "event directory",
    "exhibitor",
    "exhibitor directory",
    "exhibitors",
    "listing",
    "listings",
    "sponsor directory",
    "sponsors",
    "vendor directory",
    "vendors",
}
EVENT_NAME_HINTS = {
    "conference",
    "convention",
    "event",
    "expo",
    "fair",
    "forum",
    "market",
    "meeting",
    "show",
    "summit",
    "trade",
}
MYS_FILTER_PARAMS = {
    "featured",
    "alpha",
    "categories",
    "country",
    "state",
    "pavilion",
    "hall",
    "georegion",
    "search",
}
MYS_SOCIAL_FIELDS = (
    "facebookValue",
    "twitterValue",
    "linkedInValue",
    "linkedinValue",
    "instagramValue",
)
MYS_WEBSITE_FIELDS = ("websiteValue",)
MYS_VERSION_SEGMENT_RE = re.compile(r"^\d+(?:_\d+)+$")
LIKELY_DIRECTORY_LIST_KEYS = {
    "companies",
    "company",
    "directory",
    "exhibitor",
    "exhibitors",
    "listing",
    "listings",
    "organization",
    "organizations",
    "organisation",
    "organisations",
    "partner",
    "partners",
    "profile",
    "profiles",
    "sponsor",
    "sponsors",
    "vendor",
    "vendors",
}
DIRECTORY_NAME_KEYS = {
    "companyname",
    "companytitle",
    "displayname",
    "exhibitorname",
    "label",
    "name",
    "organizationname",
    "organisationname",
    "partnername",
    "publicname",
    "sponsorname",
    "title",
    "vendorname",
}
DIRECTORY_PROFILE_URL_KEYS = {
    "detailsurl",
    "href",
    "link",
    "path",
    "profilelink",
    "profilepath",
    "profileurl",
    "publicurl",
    "url",
}
DIRECTORY_WEBSITE_KEYS = {
    "businesswebsite",
    "companyurl",
    "companywebsite",
    "contactwebsite",
    "contacturl",
    "externalurl",
    "homepage",
    "publicwebsite",
    "website",
    "websiteurl",
}
EVENT_LINK_HOST_MARKERS = (
    "eventscribe.net",
    "expofp.com",
    "mapyourshow.com",
    "myexpoonline.com",
)
EVENT_LINK_PATH_MARKERS = (
    "eventmap",
    "floorplan",
    "floor-plan",
    "mapitbooth",
    "showrooms",
)
EVENT_LINK_QUERY_KEYS = {
    "boothid",
    "eventid",
    "hallid",
    "mapid",
    "shmode",
}
EVENT_LINK_TEXT_MARKERS = (
    "booth",
    "event map",
    "find us",
    "floor plan",
    "floorplan",
    "map",
    "showroom",
    "view booth",
)
IFRAME_SRC_RE = re.compile(
    r"<iframe[^>]+\bsrc=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
NEXT_DATA_SCRIPT_RE = re.compile(
    r"<script[^>]+\bid=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
JS_STATE_VARIABLES = (
    "__INITIAL_STATE__",
    "__PRELOADED_STATE__",
    "__APOLLO_STATE__",
    "__data",
)
DISCOVERY_EXACT_LABELS = {
    "brands",
    "exhibitor directory",
    "exhibitors",
    "participating brands",
    "participating exhibitors",
    "participating manufacturers",
    "participating partners",
    "participating sponsors",
    "participating vendors",
    "partners",
    "sponsors",
    "vendor directory",
    "vendors",
}
DISCOVERY_PARTICIPANT_WORDS = (
    "brand",
    "directory",
    "exhibitor",
    "manufacturer",
    "partner",
    "sponsor",
    "vendor",
)
WIX_WARMUP_DATA_RE = re.compile(
    r"<script[^>]+\bid=[\"']wix-warmup-data[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
WIX_GALLERY_DATA_SUFFIX = "_galleryData"
GENERIC_MEDIA_NAME_RE = re.compile(
    r"^(?:image(?: \d+)?|untitled design(?: \d+)?|\d+)$",
    re.IGNORECASE,
)
HASHY_NAME_RE = re.compile(r"^(?:[a-f0-9]{6,}|\d[\da-f ]{8,})$", re.IGNORECASE)
INLINE_SCRIPT_RE = re.compile(
    r"<script\b(?![^>]+\bsrc=)[^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
JS_VARIABLE_OBJECT_RE = re.compile(
    r"(?:var|let|const)\s+([A-Za-z_$][\w$]*)\s*=\s*(\{.*?\})\s*;",
    re.DOTALL,
)
JS_PAGINATOR_CALL_RE = re.compile(
    r"\.jsPaginator\(\s*(\{.*?\}|[A-Za-z_$][\w$]*)\s*,\s*(\{.*?\})\s*(?:,\s*(\d+))?",
    re.DOTALL,
)
JS_OBJECT_PAIR_RE = re.compile(
    r"""
    (?P<quote>['"])(?P<key>.+?)(?P=quote)\s*:\s*
    (?:
        (?P<string_quote>['"])(?P<string>.*?)(?P=string_quote)
        |(?P<number>-?\d+(?:\.\d+)?)
        |(?P<boolean>true|false)
        |(?P<null>null)
    )
    """,
    re.VERBOSE | re.DOTALL,
)
FORM_TOKEN_RE = re.compile(
    r"(?:var|let|const)\s+(tk|tm)\s*=\s*(['\"])(.*?)\2\s*;",
    re.IGNORECASE,
)
ONCLICK_NAVIGATION_RE = re.compile(
    r"""
    (?:
        (?:window|document)?\.?location(?:\.href)?
        |window\.open
        |open
    )
    \s*(?:=\s*|\(\s*)
    ['"]([^'"]+)['"]
    """,
    re.IGNORECASE | re.VERBOSE,
)
HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
CONTAINER_TAGS = {"article", "div", "li", "section", "tr"}
NAVIGABLE_DATA_ATTRS = (
    "data-option-url",
    "data-href",
    "data-url",
    "data-link",
    "data-target-url",
    "formaction",
)
PROFILE_ACTION_LABELS = {
    "details",
    "explore",
    "learn more",
    "more details",
    "profile",
    "read more",
    "see more",
    "view",
    "view company",
    "view details",
    "view profile",
    "visit",
}
PROFILE_URL_MARKERS = (
    "/co/",
    "/company",
    "/companies/",
    "/exhibitor",
    "/exhibitors/",
    "/member",
    "/members/",
    "/organization",
    "/organizations/",
    "/participant",
    "/participants/",
    "/partner",
    "/partners/",
    "/profile",
    "/profiles/",
    "/speaker",
    "/speakers/",
    "/sponsor",
    "/sponsors/",
    "/vendor",
    "/vendors/",
)


@dataclass(frozen=True)
class AnchorRecord:
    text: str
    href: str
    absolute_url: str
    signature: tuple[str, ...]
    order: int
    in_header: bool
    in_footer: bool
    in_nav: bool
    in_main: bool
    title_attr: str
    aria_label: str

    @property
    def display_text(self) -> str:
        return normalize_text(self.text or self.title_attr or self.aria_label)


@dataclass(frozen=True)
class ActionRecord:
    text: str
    raw_url: str
    absolute_url: str
    signature: tuple[str, ...]
    order: int
    tag: str
    source_attr: str
    in_header: bool
    in_footer: bool
    in_nav: bool
    in_main: bool
    title_attr: str
    aria_label: str

    @property
    def display_text(self) -> str:
        return normalize_text(self.text or self.title_attr or self.aria_label)


@dataclass(frozen=True)
class ContainerRecord:
    signature: tuple[str, ...]
    order: int
    tag: str
    text: str
    heading_texts: tuple[str, ...]
    actions: tuple[ActionRecord, ...]
    in_header: bool
    in_footer: bool
    in_nav: bool
    in_main: bool


@dataclass(frozen=True)
class ListingStrategy:
    source_kind: str
    signature: tuple[str, ...]
    url_group: str
    base_score: float
    unique_urls: int
    sample_names: tuple[str, ...]


@dataclass(frozen=True)
class DirectoryEntry:
    sort_index: int
    directory_page: int
    company_name: str
    profile_url: str
    website_url_hint: str = ""


@dataclass(frozen=True)
class CompanyRecord:
    sort_index: int
    directory_page: int
    company_name: str
    profile_url: str
    website_url: str


@dataclass(frozen=True)
class ParsedPage:
    url: str
    anchors: tuple[AnchorRecord, ...]
    title: str
    h1_texts: tuple[str, ...]
    json_ld_blocks: tuple[str, ...]
    actions: tuple[ActionRecord, ...] = ()
    containers: tuple[ContainerRecord, ...] = ()


@dataclass(frozen=True)
class ContainerEntryCandidate:
    company_name: str
    profile_url: str
    signature: tuple[str, ...]
    order: int


@dataclass(frozen=True)
class AjaxPaginatorConfig:
    endpoint_url: str
    params: tuple[tuple[str, str], ...]
    limit: int
    total_results: int
    next_offset: int
    page_id: str = "openAjax"


@dataclass(frozen=True)
class ExtractedEntryCandidate:
    company_name: str
    profile_url: str
    website_url_hint: str = ""


@dataclass(frozen=True)
class BrowserFallbackOptions:
    mode: str = DEFAULT_BROWSER_MODE
    timeout_ms: int = DEFAULT_BROWSER_TIMEOUT_MS

    @property
    def enabled(self) -> bool:
        return self.mode != "off"

    @property
    def prefer_browser(self) -> bool:
        return self.mode == "prefer"


class BrowserRenderer:
    def __init__(self, timeout_ms: int) -> None:
        self.timeout_ms = timeout_ms
        self._playwright = None
        self._browser = None
        self._context = None

    @staticmethod
    def is_available() -> bool:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
        except ModuleNotFoundError:
            return False
        return True

    def ensure_started(self) -> None:
        if self._context is not None:
            return

        try:
            from playwright.sync_api import sync_playwright
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Browser fallback requires Playwright. "
                "Install it with `python3 -m pip install playwright` and "
                "`python3 -m playwright install chromium`."
            ) from exc

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            ignore_https_errors=True,
            locale="en-US",
            user_agent=USER_AGENT,
        )
        self._context.set_default_navigation_timeout(self.timeout_ms)
        self._context.set_default_timeout(min(self.timeout_ms, 10000))

    def close(self) -> None:
        if self._context is not None:
            self._context.close()
            self._context = None
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None

    def __enter__(self) -> BrowserRenderer:
        self.ensure_started()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _dismiss_overlays(self, page: object) -> None:
        for pattern in (BROWSER_ACCEPT_RE,):
            try:
                locator = page.locator("button, [role='button'], a").filter(has_text=pattern)
                count = min(locator.count(), 3)
                for index in range(count):
                    button = locator.nth(index)
                    if not button.is_visible():
                        continue
                    button.click(timeout=1500)
                    page.wait_for_timeout(300)
                    return
            except Exception:  # noqa: BLE001
                continue

    def _click_progress_controls(self, page: object) -> bool:
        try:
            locator = page.locator("button, [role='button'], a").filter(
                has_text=BROWSER_LOAD_MORE_RE
            )
            count = min(locator.count(), 4)
        except Exception:  # noqa: BLE001
            return False

        for index in range(count):
            control = locator.nth(index)
            try:
                if not control.is_visible():
                    continue
                control.scroll_into_view_if_needed(timeout=1500)
                control.click(timeout=2000)
                page.wait_for_timeout(800)
                return True
            except Exception:  # noqa: BLE001
                continue
        return False

    def _stabilize_page(self, page: object) -> None:
        try:
            page.wait_for_load_state("networkidle", timeout=min(self.timeout_ms, 6000))
        except Exception:  # noqa: BLE001
            pass

        page.wait_for_timeout(800)
        self._dismiss_overlays(page)

        last_height = 0
        stalls = 0
        for _ in range(6):
            clicked = self._click_progress_controls(page)
            try:
                page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            except Exception:  # noqa: BLE001
                break
            page.wait_for_timeout(700)
            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:  # noqa: BLE001
                pass
            try:
                height = int(
                    page.evaluate(
                        "() => Math.max(document.body.scrollHeight, "
                        "document.documentElement.scrollHeight)"
                    )
                )
            except Exception:  # noqa: BLE001
                break

            if height <= last_height and not clicked:
                stalls += 1
            else:
                stalls = 0
            last_height = height
            if stalls >= 2:
                break

        try:
            page.evaluate("() => window.scrollTo(0, 0)")
        except Exception:  # noqa: BLE001
            pass

    def render(self, url: str) -> tuple[str, str]:
        self.ensure_started()
        page = self._context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded")
            self._stabilize_page(page)
            final_url = normalize_http_url(page.url) or url
            return final_url, page.content()
        finally:
            page.close()

class HtmlSignalParser(HTMLParser):
    """Collect page signals we can use for heuristic scraping."""

    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.anchors: list[AnchorRecord] = []
        self.actions: list[ActionRecord] = []
        self.containers: list[ContainerRecord] = []
        self.title_parts: list[str] = []
        self.h1_texts: list[str] = []
        self.json_ld_blocks: list[str] = []

        self._stack: list[dict[str, object]] = []
        self._capture_title = False
        self._capture_h1 = False
        self._h1_parts: list[str] = []
        self._capture_json_ld = False
        self._json_ld_parts: list[str] = []

        self._header_depth = 0
        self._footer_depth = 0
        self._nav_depth = 0
        self._main_depth = 0
        self._anchor_counter = 0
        self._action_counter = 0
        self._container_counter = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key: value or "" for key, value in attrs}
        classes = tuple(
            normalize_text(attributes.get("class", "")).split()
            if attributes.get("class")
            else ()
        )

        if tag == "header":
            self._header_depth += 1
        elif tag == "footer":
            self._footer_depth += 1
        elif tag == "nav":
            self._nav_depth += 1
        elif tag in {"main", "article"}:
            self._main_depth += 1

        frame = {
            "tag": tag,
            "classes": classes,
            "id": attributes.get("id", ""),
            "text_parts": [],
            "heading_texts": [],
            "actions": [],
            "in_header": self._header_depth > 0,
            "in_footer": self._footer_depth > 0,
            "in_nav": self._nav_depth > 0,
            "in_main": self._main_depth > 0,
        }
        frame["signature"] = build_signature([*self._stack, frame])

        if tag == "title":
            self._capture_title = True
        elif tag == "h1":
            self._capture_h1 = True
            self._h1_parts = []
        elif tag == "script" and attributes.get("type", "").lower() == "application/ld+json":
            self._capture_json_ld = True
            self._json_ld_parts = []

        if tag == "a":
            absolute_url = normalize_http_url(urljoin(self.base_url, attributes.get("href", "")))
            frame["anchor_meta"] = {
                "href": attributes.get("href", ""),
                "absolute_url": absolute_url or "",
                "signature": tuple(frame["signature"]),  # type: ignore[arg-type]
                "order": self._anchor_counter,
                "in_header": frame["in_header"],
                "in_footer": frame["in_footer"],
                "in_nav": frame["in_nav"],
                "in_main": frame["in_main"],
                "title_attr": normalize_text(attributes.get("title", "")),
                "aria_label": normalize_text(attributes.get("aria-label", "")),
            }
            self._anchor_counter += 1

        action_targets = extract_navigable_targets(self.base_url, tag, attributes)
        if action_targets:
            frame["action_meta"] = {
                "targets": action_targets,
                "signature": tuple(frame["signature"]),  # type: ignore[arg-type]
                "order": self._action_counter,
                "tag": tag,
                "source_attrs": tuple(source_attr for source_attr, _, _ in action_targets),
                "in_header": frame["in_header"],
                "in_footer": frame["in_footer"],
                "in_nav": frame["in_nav"],
                "in_main": frame["in_main"],
                "title_attr": normalize_text(attributes.get("title", "")),
                "aria_label": normalize_text(attributes.get("aria-label", "")),
            }
            self._action_counter += 1

        self._stack.append(frame)

        if tag == "img":
            alt_text = normalize_text(attributes.get("alt", ""))
            if alt_text:
                for open_frame in self._stack:
                    open_frame["text_parts"].append(alt_text)  # type: ignore[index]

    def handle_endtag(self, tag: str) -> None:
        frame = self._stack.pop() if self._stack else None
        frame_text_parts = frame.get("text_parts", []) if frame else []
        element_text = normalize_text(" ".join(frame_text_parts))
        frame_heading_texts = list(frame.get("heading_texts", [])) if frame else []
        frame_actions = list(frame.get("actions", [])) if frame else []

        anchor_meta = frame.get("anchor_meta") if frame else None
        if tag == "a" and anchor_meta is not None:
            text = element_text
            absolute_url = str(anchor_meta["absolute_url"])
            href = str(anchor_meta["href"])
            if absolute_url and href:
                self.anchors.append(
                    AnchorRecord(
                        text=text,
                        href=href,
                        absolute_url=absolute_url,
                        signature=tuple(anchor_meta["signature"]),  # type: ignore[arg-type]
                        order=int(anchor_meta["order"]),
                        in_header=bool(anchor_meta["in_header"]),
                        in_footer=bool(anchor_meta["in_footer"]),
                        in_nav=bool(anchor_meta["in_nav"]),
                        in_main=bool(anchor_meta["in_main"]),
                        title_attr=str(anchor_meta["title_attr"]),
                        aria_label=str(anchor_meta["aria_label"]),
                    )
                )

        action_meta = frame.get("action_meta") if frame else None
        if action_meta is not None:
            targets = action_meta["targets"]  # type: ignore[index]
            source_attrs = action_meta["source_attrs"]  # type: ignore[index]
            for (source_attr, raw_url, absolute_url), stored_source_attr in zip(
                targets,
                source_attrs,
            ):
                action_record = ActionRecord(
                    text=element_text,
                    raw_url=raw_url,
                    absolute_url=absolute_url,
                    signature=tuple(action_meta["signature"]),  # type: ignore[arg-type]
                    order=int(action_meta["order"]),
                    tag=str(action_meta["tag"]),
                    source_attr=str(stored_source_attr or source_attr),
                    in_header=bool(action_meta["in_header"]),
                    in_footer=bool(action_meta["in_footer"]),
                    in_nav=bool(action_meta["in_nav"]),
                    in_main=bool(action_meta["in_main"]),
                    title_attr=str(action_meta["title_attr"]),
                    aria_label=str(action_meta["aria_label"]),
                )
                self.actions.append(action_record)
                frame_actions.append(action_record)

        if tag == "title":
            self._capture_title = False
        elif tag == "h1":
            self._capture_h1 = False
            heading = normalize_text(" ".join(self._h1_parts))
            if heading:
                self.h1_texts.append(heading)
            self._h1_parts = []
        elif tag == "script" and self._capture_json_ld:
            self._capture_json_ld = False
            block = "".join(self._json_ld_parts).strip()
            if block:
                self.json_ld_blocks.append(block)
            self._json_ld_parts = []

        if tag in HEADING_TAGS and element_text:
            frame_heading_texts.append(element_text)

        if (
            frame is not None
            and tag in CONTAINER_TAGS
            and frame_actions
            and (frame_heading_texts or element_text)
        ):
            self.containers.append(
                ContainerRecord(
                    signature=tuple(frame["signature"]),  # type: ignore[arg-type]
                    order=self._container_counter,
                    tag=tag,
                    text=element_text,
                    heading_texts=tuple(frame_heading_texts),
                    actions=tuple(frame_actions),
                    in_header=bool(frame["in_header"]),
                    in_footer=bool(frame["in_footer"]),
                    in_nav=bool(frame["in_nav"]),
                    in_main=bool(frame["in_main"]),
                )
            )
            self._container_counter += 1

        if self._stack:
            for parent in self._stack:
                if frame_heading_texts:
                    parent["heading_texts"].extend(frame_heading_texts)  # type: ignore[index]
                if frame_actions:
                    parent["actions"].extend(frame_actions)  # type: ignore[index]

        if tag == "header" and self._header_depth > 0:
            self._header_depth -= 1
        elif tag == "footer" and self._footer_depth > 0:
            self._footer_depth -= 1
        elif tag == "nav" and self._nav_depth > 0:
            self._nav_depth -= 1
        elif tag in {"main", "article"} and self._main_depth > 0:
            self._main_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._stack:
            for frame in self._stack:
                frame["text_parts"].append(data)  # type: ignore[index]
        if self._capture_title:
            self.title_parts.append(data)
        if self._capture_h1:
            self._h1_parts.append(data)
        if self._capture_json_ld:
            self._json_ld_parts.append(data)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)


def normalize_text(text: str) -> str:
    return WHITESPACE_RE.sub(" ", text).strip()


def slugify_filename_part(text: str) -> str:
    slug = NON_ALNUM_RE.sub("_", text.lower()).strip("_")
    return slug[:80].strip("_")


def normalize_http_url(url: str) -> str | None:
    if not url:
        return None
    parsed = urlparse(url.strip())
    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return urlunparse(parsed._replace(fragment=""))


def normalize_navigable_target(base_url: str, raw_url: str) -> str | None:
    cleaned = raw_url.strip()
    if not cleaned:
        return None

    lowered = cleaned.lower()
    if lowered.startswith(("javascript:", "mailto:", "tel:", "#")):
        return None

    if "://" not in cleaned and not cleaned.startswith(("/", "?")):
        host_candidate = cleaned.split("/", 1)[0]
        if "." in host_candidate and " " not in cleaned:
            cleaned = f"https://{cleaned.lstrip('/')}"

    return normalize_http_url(urljoin(base_url, cleaned))


def extract_navigable_targets(
    base_url: str,
    tag: str,
    attributes: dict[str, str],
) -> list[tuple[str, str, str]]:
    candidates: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    if tag == "a" and attributes.get("href"):
        normalized = normalize_navigable_target(base_url, attributes["href"])
        if normalized:
            key = ("href", normalized)
            seen.add(key)
            candidates.append(("href", attributes["href"], normalized))

    for attr_name in NAVIGABLE_DATA_ATTRS:
        raw_value = attributes.get(attr_name, "")
        normalized = normalize_navigable_target(base_url, raw_value)
        if not normalized:
            continue
        key = (attr_name, normalized)
        if key in seen:
            continue
        seen.add(key)
        candidates.append((attr_name, raw_value, normalized))

    onclick = attributes.get("onclick", "")
    if onclick:
        for match in ONCLICK_NAVIGATION_RE.findall(onclick):
            normalized = normalize_navigable_target(base_url, match)
            if not normalized:
                continue
            key = ("onclick", normalized)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(("onclick", match, normalized))

    return candidates


def host_key(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def same_site(url_a: str, url_b: str) -> bool:
    host_a = host_key(url_a)
    host_b = host_key(url_b)
    if not host_a or not host_b:
        return False
    return (
        host_a == host_b
        or host_a.endswith("." + host_b)
        or host_b.endswith("." + host_a)
    )


def looks_like_asset(url: str) -> bool:
    lowered_path = urlparse(url).path.lower()
    return lowered_path.endswith(ASSET_EXTENSIONS)


def signature_token(tag: str, class_names: tuple[str, ...], element_id: str) -> str:
    pieces = [tag]
    if element_id:
        pieces.append(f"#{element_id.lower()}")
    for class_name in class_names[:2]:
        pieces.append(f".{class_name.lower()}")
    return "".join(pieces)


def build_signature(stack: list[dict[str, object]]) -> tuple[str, ...]:
    relevant: list[str] = []
    for frame in stack:
        tag = str(frame["tag"])
        if tag in {"html", "body"}:
            continue
        classes = tuple(frame["classes"])  # type: ignore[arg-type]
        element_id = str(frame["id"])
        relevant.append(signature_token(tag, classes, element_id))
    return tuple(relevant[-4:])


def dedupe_preserving_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_text(value)
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def fetch_text(
    url: str,
    extra_headers: dict[str, str] | None = None,
    form_data: list[tuple[str, str]] | dict[str, str] | None = None,
) -> str:
    request_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": USER_AGENT,
    }
    if extra_headers:
        request_headers.update(extra_headers)

    form_payload = None
    request_data = None
    if form_data is not None:
        request_headers.setdefault(
            "Content-Type",
            "application/x-www-form-urlencoded; charset=UTF-8",
        )
        form_payload = urlencode(form_data, doseq=True)
        request_data = form_payload.encode("utf-8")

    request = Request(url, headers=request_headers, data=request_data)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                encoding = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(encoding, errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            try:
                return fetch_text_with_curl(
                    url=url,
                    request_headers=request_headers,
                    form_payload=form_payload,
                )
            except RuntimeError as curl_exc:
                combined_exc = RuntimeError(
                    f"{exc}; curl fallback failed: {curl_exc}"
                )
            if attempt == RETRY_ATTEMPTS:
                raise RuntimeError(f"Unable to fetch {url}: {combined_exc}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"Unable to fetch {url}")


def fetch_text_with_curl(
    url: str,
    request_headers: dict[str, str],
    form_payload: str | None = None,
) -> str:
    command = [
        "curl",
        "-L",
        "--compressed",
        "--silent",
        "--show-error",
        "--max-time",
        str(REQUEST_TIMEOUT_SECONDS),
        "--user-agent",
        USER_AGENT,
    ]

    for key, value in request_headers.items():
        if key.lower() == "user-agent":
            continue
        command.extend(["-H", f"{key}: {value}"])

    if form_payload is not None:
        command.extend(["--data", form_payload])

    command.append(url)

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise RuntimeError(f"curl execution failed: {exc}") from exc

    if completed.returncode != 0:
        stderr_text = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(stderr_text or f"curl exited with status {completed.returncode}")

    return completed.stdout.decode("utf-8", errors="replace")


def fetch_html(url: str, extra_headers: dict[str, str] | None = None) -> str:
    return fetch_text(url, extra_headers=extra_headers)


def load_static_page(url: str) -> tuple[str, str, ParsedPage]:
    normalized_url = normalize_http_url(url) or url
    html_text = fetch_html(normalized_url)
    return normalized_url, html_text, parse_page(normalized_url, html_text)


def load_browser_page(browser_renderer: BrowserRenderer, url: str) -> tuple[str, str, ParsedPage]:
    final_url, html_text = browser_renderer.render(url)
    return final_url, html_text, parse_page(final_url, html_text)


def parse_page(url: str, html_text: str) -> ParsedPage:
    parser = HtmlSignalParser(url)
    parser.feed(html_text)
    return ParsedPage(
        url=url,
        anchors=tuple(parser.anchors),
        title=normalize_text(" ".join(parser.title_parts)),
        h1_texts=tuple(parser.h1_texts),
        json_ld_blocks=tuple(parser.json_ld_blocks),
        actions=tuple(parser.actions),
        containers=tuple(parser.containers),
    )


def canonical_label(text: str) -> str:
    lowered = normalize_text(text).lower()
    return NON_ALNUM_RE.sub(" ", lowered).strip()


def canonical_key(text: str) -> str:
    return NON_ALNUM_RE.sub("", normalize_text(text).lower())


def looks_generic_directory_label(text: str) -> bool:
    label = canonical_label(text)
    if not label:
        return True
    if label in GENERIC_DIRECTORY_LABELS:
        return True
    if "directory" in label and any(
        keyword in label for keyword in ("exhibitor", "vendor", "company", "conference", "event")
    ):
        return True
    return False


def score_conference_name_candidate(text: str) -> float:
    normalized = normalize_text(text)
    if not normalized:
        return float("-inf")
    if looks_generic_directory_label(normalized):
        return float("-inf")

    label = canonical_label(normalized)
    score = len(normalized)
    score += sum(12 for hint in EVENT_NAME_HINTS if hint in label)
    if len(normalized.split()) <= 1:
        score -= 8
    if len(normalized) > 60:
        score -= 10
    return score


def infer_conference_name(seed_url: str, seed_page: ParsedPage) -> str:
    candidates: list[str] = []

    if seed_page.title:
        title_parts = [part for part in TITLE_SPLIT_RE.split(seed_page.title) if normalize_text(part)]
        candidates.extend(title_parts)
        candidates.append(seed_page.title)

    candidates.extend(seed_page.h1_texts)

    unique_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = normalize_text(candidate)
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        unique_candidates.append(normalized)

    if unique_candidates:
        best = max(unique_candidates, key=score_conference_name_candidate)
        if score_conference_name_candidate(best) > float("-inf"):
            return best

    host = host_key(seed_url)
    host_parts = [part for part in host.split(".") if part and part not in {"www", "com", "org", "net", "io", "co", "us"}]
    if host_parts:
        return " ".join(part.capitalize() for part in host_parts)
    return "conference_directory"


def resolve_output_path(
    output_arg: str | None,
    seed_url: str,
    seed_page: ParsedPage,
) -> Path:
    suggested_name = slugify_filename_part(infer_conference_name(seed_url, seed_page)) or "conference_directory"
    suggested_filename = f"{suggested_name}.csv"

    if not output_arg:
        return Path(suggested_filename).resolve()

    output_path = Path(output_arg).expanduser()
    if output_arg.endswith(("/", "\\")) or (output_path.exists() and output_path.is_dir()):
        return (output_path / suggested_filename).resolve()
    return output_path.resolve()


def retitle_page(page: ParsedPage, title: str) -> ParsedPage:
    normalized_title = normalize_text(title)
    if not normalized_title:
        return page

    updated_h1s = page.h1_texts
    if normalized_title.lower() not in {text.lower() for text in page.h1_texts}:
        updated_h1s = (normalized_title, *page.h1_texts)

    return ParsedPage(
        url=page.url,
        anchors=page.anchors,
        title=normalized_title,
        h1_texts=updated_h1s,
        json_ld_blocks=page.json_ld_blocks,
        actions=page.actions,
        containers=page.containers,
    )


def normalize_url_ignoring_fragment(url: str) -> str | None:
    normalized = normalize_http_url(url)
    if not normalized:
        return None
    parsed = urlparse(normalized)
    return urlunparse(parsed._replace(fragment=""))


def score_directory_discovery_label(text: str) -> float:
    label = canonical_label(text)
    if not label:
        return float("-inf")

    score = 0.0
    if label in DISCOVERY_EXACT_LABELS:
        score += 140
    if "participating" in label:
        score += 55
    if "directory" in label:
        score += 40
    score += sum(25 for word in DISCOVERY_PARTICIPANT_WORDS if word in label)
    if "portal" in label:
        score -= 45
    if "floor plan" in label or "floorplan" in label:
        score -= 70
    if "register" in label or "faq" in label:
        score -= 25
    return score


def discover_related_directory_url(page: ParsedPage, seed_url: str) -> str | None:
    current_without_fragment = normalize_url_ignoring_fragment(seed_url)
    best_url = None
    best_score = float("-inf")

    for anchor in page.anchors:
        if not anchor.absolute_url or looks_like_asset(anchor.absolute_url):
            continue
        label_candidates = (
            anchor.display_text,
            anchor.title_attr,
            anchor.aria_label,
        )
        score = max(score_directory_discovery_label(label) for label in label_candidates)
        if score == float("-inf"):
            continue

        candidate_without_fragment = normalize_url_ignoring_fragment(anchor.absolute_url)
        if not candidate_without_fragment:
            continue

        if candidate_without_fragment == current_without_fragment:
            score -= 10
        elif same_site(anchor.absolute_url, seed_url):
            score += 20

        if score > best_score:
            best_score = score
            best_url = anchor.absolute_url

    if best_score < 60:
        return None
    return best_url


def find_embedded_directory_url(seed_url: str, seed_html: str) -> str | None:
    candidates: list[tuple[int, str]] = []

    for match in IFRAME_SRC_RE.finditer(seed_html):
        normalized = normalize_http_url(urljoin(seed_url, match.group(1)))
        if not normalized:
            continue

        score = 0
        lowered = normalized.lower()
        if any(marker in lowered for marker in ("exhibitor", "directory", "vendor", "sponsor")):
            score += 40
        if any(marker in lowered for marker in ("mapyourshow.com", "expofp.com", "swapcard.com")):
            score += 80
        if same_site(normalized, seed_url):
            score += 10
        if score > 0:
            candidates.append((score, normalized))

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def resolve_seed_page(
    seed_url: str,
    page_loader: callable[[str], tuple[str, str, ParsedPage]],
) -> tuple[str, str, ParsedPage]:
    current_url, current_html, current_page = page_loader(seed_url)
    visited_seed_urls = {
        normalize_url_ignoring_fragment(current_url) or current_url,
    }

    for _ in range(3):
        embedded_url = find_embedded_directory_url(current_url, current_html)
        normalized_embedded_url = (
            normalize_url_ignoring_fragment(embedded_url)
            if embedded_url
            else None
        )
        if (
            embedded_url
            and normalized_embedded_url
            and normalized_embedded_url not in visited_seed_urls
        ):
            print(f"Following embedded directory iframe: {embedded_url}")
            current_url, current_html, current_page = page_loader(embedded_url)
            visited_seed_urls.add(normalize_url_ignoring_fragment(current_url) or current_url)
            continue

        discovered_url = discover_related_directory_url(current_page, current_url)
        discovered_without_fragment = (
            normalize_url_ignoring_fragment(discovered_url)
            if discovered_url
            else None
        )
        current_without_fragment = normalize_url_ignoring_fragment(current_url)

        if (
            discovered_url
            and discovered_without_fragment
            and discovered_without_fragment != current_without_fragment
            and discovered_without_fragment not in visited_seed_urls
        ):
            print(f"Following discovered participant directory link: {discovered_url}")
            current_url, current_html, current_page = page_loader(discovered_url)
            visited_seed_urls.add(normalize_url_ignoring_fragment(current_url) or current_url)
            continue

        break

    return current_url, current_html, current_page


def is_mapyourshow_site(url: str) -> bool:
    return "mapyourshow.com" in host_key(url)


def is_expofp_site(url: str) -> bool:
    return "expofp.com" in host_key(url)


def is_swapcard_site(url: str) -> bool:
    return "swapcard.com" in host_key(url)


def is_mapyourshow_directory(seed_url: str, seed_html: str) -> bool:
    if not is_mapyourshow_site(seed_url):
        return False
    return (
        "remote-proxy.cfm?action=search" in seed_html
        and "getExhibitorURL" in seed_html
        and (
            "searchtype=exhibitorgallery" in seed_html
            or 'searchtype:"exhibitorgallery"' in seed_html
            or "searchtype:'exhibitorgallery'" in seed_html
        )
    )


def is_expofp_directory(seed_url: str, seed_html: str) -> bool:
    if not is_expofp_site(seed_url):
        return False
    return (
        'class="expofp-floorplan"' in seed_html
        and "data-data-url=" in seed_html
        and "data/version.js" in seed_html
    )


def mapyourshow_root_prefix(seed_url: str) -> str:
    path_segments = [segment for segment in urlparse(seed_url).path.split("/") if segment]
    if path_segments and MYS_VERSION_SEGMENT_RE.fullmatch(path_segments[0]):
        return f"/{path_segments[0]}"
    return ""


def build_mapyourshow_url(seed_url: str, relative_path: str, query: str = "") -> str:
    parsed = urlparse(seed_url)
    root_prefix = mapyourshow_root_prefix(seed_url)
    normalized_path = relative_path if relative_path.startswith("/") else f"/{relative_path}"
    final_path = (
        normalized_path
        if normalized_path.startswith(root_prefix + "/")
        else f"{root_prefix}{normalized_path}"
    )
    return urlunparse(
        parsed._replace(path=final_path, query=query, fragment="")
    )


def parse_mapyourshow_filters(seed_url: str) -> list[tuple[str, str]]:
    filters: list[tuple[str, str]] = []
    for key, value in parse_qsl(urlparse(seed_url).query, keep_blank_values=True):
        if key.lower() in MYS_FILTER_PARAMS and value != "":
            filters.append((key, value))
    return filters


def build_mapyourshow_search_url(seed_url: str, start: int, search_size: int) -> str:
    query_items = [
        ("action", "search"),
        ("searchtype", "exhibitorgallery"),
        ("searchsize", str(search_size)),
        ("start", str(start)),
        *parse_mapyourshow_filters(seed_url),
    ]
    return build_mapyourshow_url(
        seed_url,
        "/ajax/remote-proxy.cfm",
        urlencode(query_items, doseq=True),
    )


def mapyourshow_request_headers(seed_url: str) -> dict[str, str]:
    parsed = urlparse(seed_url)
    origin = urlunparse(parsed._replace(path="", params="", query="", fragment=""))
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": seed_url,
        "Origin": origin.rstrip("/"),
        "X-Requested-With": "XMLHttpRequest",
    }


def build_mapyourshow_profile_url(seed_url: str, exhibitor_id: str) -> str:
    return build_mapyourshow_url(
        seed_url,
        "/exhibitor/exhibitor-details.cfm",
        urlencode([("exhid", exhibitor_id)]),
    )


def collect_directory_entries_mapyourshow(
    seed_url: str,
    start_page: int | None,
    end_page: int | None,
    page_size: int = MYS_DEFAULT_PAGE_SIZE,
) -> list[DirectoryEntry]:
    first_payload_text = fetch_html(
        build_mapyourshow_search_url(seed_url, start=0, search_size=page_size),
        extra_headers=mapyourshow_request_headers(seed_url),
    )
    try:
        payload = json.loads(first_payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Map Your Show search response was not valid JSON.") from exc

    exhibitor_results = (
        payload.get("DATA", {})
        .get("results", {})
        .get("exhibitor", {})
    )
    total_found = int(exhibitor_results.get("found") or 0)
    total_pages = max(1, (total_found + page_size - 1) // page_size)

    requested_start = max(start_page or 1, 1)
    requested_end = min(end_page or total_pages, total_pages)
    if requested_start > requested_end:
        raise ValueError("--start-page cannot be greater than --end-page.")

    entries: list[DirectoryEntry] = []
    seen_profiles: set[str] = set()

    for page_number in range(requested_start, requested_end + 1):
        start_index = (page_number - 1) * page_size
        if page_number == 1:
            page_payload = payload
        else:
            page_payload_text = fetch_html(
                build_mapyourshow_search_url(
                    seed_url,
                    start=start_index,
                    search_size=page_size,
                ),
                extra_headers=mapyourshow_request_headers(seed_url),
            )
            try:
                page_payload = json.loads(page_payload_text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Map Your Show page {page_number} did not return valid JSON."
                ) from exc

        hits = (
            page_payload.get("DATA", {})
            .get("results", {})
            .get("exhibitor", {})
            .get("hit", [])
        )

        added = 0
        for hit in hits:
            fields = hit.get("fields", {})
            exhibitor_id = str(fields.get("exhid_l") or "").strip()
            company_name = normalize_text(str(fields.get("exhname_t") or ""))
            if not exhibitor_id or not company_name:
                continue

            profile_url = build_mapyourshow_profile_url(seed_url, exhibitor_id)
            if profile_url in seen_profiles:
                continue

            seen_profiles.add(profile_url)
            entries.append(
                DirectoryEntry(
                    sort_index=len(entries),
                    directory_page=page_number,
                    company_name=company_name,
                    profile_url=profile_url,
                )
            )
            added += 1

        print(
            f"Collected Map Your Show page {page_number}/{requested_end} "
            f"({len(hits)} results, {len(entries)} total, {added} new)."
        )

    return entries


def decode_js_string_value(raw_value: str) -> str:
    try:
        return json.loads(f'"{raw_value}"')
    except json.JSONDecodeError:
        return raw_value.replace("\\/", "/")


def extract_embedded_js_url(profile_html: str, field_names: tuple[str, ...]) -> str:
    for field_name in field_names:
        patterns = (
            re.compile(
                rf"{re.escape(field_name)}\s*:\s*\"((?:\\.|[^\"])*)\"",
                re.IGNORECASE,
            ),
            re.compile(
                rf"{re.escape(field_name)}\s*:\s*'((?:\\.|[^'])*)'",
                re.IGNORECASE,
            ),
        )
        for pattern in patterns:
            match = pattern.search(profile_html)
            if not match:
                continue
            normalized = normalize_http_url(
                decode_js_string_value(match.group(1)).strip()
            )
            if normalized:
                return normalized
    return ""


def extract_balanced_json_fragment(text: str, start_index: int) -> str | None:
    stack: list[str] = []
    quote_char: str | None = None
    escaped = False

    for index in range(start_index, len(text)):
        character = text[index]

        if quote_char is not None:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote_char:
                quote_char = None
            continue

        if character in {"'", '"'}:
            quote_char = character
            continue
        if character == "{":
            stack.append("}")
            continue
        if character == "[":
            stack.append("]")
            continue
        if character in {"}", "]"}:
            if not stack or character != stack[-1]:
                return None
            stack.pop()
            if not stack:
                return text[start_index:index + 1]

    return None


def extract_json_assignment_from_html(
    html_text: str,
    variable_name: str,
) -> object | None:
    assignment_patterns = (
        rf"window\.{re.escape(variable_name)}\s*=",
        rf"var\s+{re.escape(variable_name)}\s*=",
        rf"\b{re.escape(variable_name)}\s*=",
    )

    for pattern in assignment_patterns:
        for match in re.finditer(pattern, html_text):
            cursor = match.end()
            while cursor < len(html_text) and html_text[cursor].isspace():
                cursor += 1
            if cursor >= len(html_text) or html_text[cursor] not in "{[":
                continue

            fragment = extract_balanced_json_fragment(html_text, cursor)
            if not fragment:
                continue

            try:
                return json.loads(fragment)
            except json.JSONDecodeError:
                continue

    return None


def parse_json_assignment(js_text: str, variable_name: str) -> object:
    patterns = (
        rf"^\ufeff?\s*(?:var\s+)?{re.escape(variable_name)}\s*=",
        rf"^\ufeff?\s*window\.{re.escape(variable_name)}\s*=",
    )

    for pattern in patterns:
        match = re.search(pattern, js_text)
        if not match:
            continue
        cursor = match.end()
        while cursor < len(js_text) and js_text[cursor].isspace():
            cursor += 1
        if cursor >= len(js_text) or js_text[cursor] not in "{[":
            continue

        fragment = extract_balanced_json_fragment(js_text, cursor)
        if fragment is None:
            continue
        return json.loads(fragment)

    raise ValueError(f"Could not parse assignment for `{variable_name}`.")


def extract_next_data(seed_html: str) -> object | None:
    match = NEXT_DATA_SCRIPT_RE.search(seed_html)
    if not match:
        return None

    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def extract_expofp_data_url(seed_url: str, seed_html: str) -> str | None:
    match = re.search(
        r"\bdata-data-url=[\"']([^\"']+)[\"']",
        seed_html,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return normalize_http_url(urljoin(seed_url, match.group(1)))


def extract_wix_warmup_data(seed_html: str) -> object | None:
    match = WIX_WARMUP_DATA_RE.search(seed_html)
    if not match:
        return None

    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def build_expofp_profile_url(seed_url: str, external_id: str) -> str:
    parsed = urlparse(seed_url)
    return urlunparse(
        parsed._replace(fragment=f"exhibitor={quote(external_id, safe='')}")
    )


def collect_directory_entries_expofp(
    seed_url: str,
    seed_html: str,
) -> tuple[list[DirectoryEntry], str]:
    data_url = extract_expofp_data_url(seed_url, seed_html)
    if not data_url:
        raise RuntimeError("Could not locate the ExpoFP data endpoint.")

    payload_text = fetch_html(urljoin(data_url, EXPOFP_DATA_FILENAME))
    try:
        payload = parse_json_assignment(payload_text, "__data")
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("ExpoFP data payload could not be parsed.") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("ExpoFP data payload was not a JSON object.")

    title = normalize_text(str(payload.get("title") or ""))
    exhibitors = payload.get("exhibitors")
    if not isinstance(exhibitors, list):
        raise RuntimeError("ExpoFP data payload did not include an exhibitor list.")

    entries: list[DirectoryEntry] = []
    seen_profiles: set[str] = set()

    for exhibitor in exhibitors:
        if not isinstance(exhibitor, dict):
            continue

        company_name = normalize_text(str(exhibitor.get("name") or ""))
        if not company_name:
            continue

        external_id = normalize_text(
            str(exhibitor.get("externalId") or exhibitor.get("id") or "")
        )
        if not external_id:
            continue

        profile_url = build_expofp_profile_url(seed_url, external_id)
        if profile_url in seen_profiles:
            continue

        seen_profiles.add(profile_url)
        entries.append(
            DirectoryEntry(
                sort_index=len(entries),
                directory_page=1,
                company_name=company_name,
                profile_url=profile_url,
            )
        )

    print(f"Collected {len(entries)} ExpoFP exhibitor entries from the public data payload.")
    return entries, title


def iter_mapping_leaves(
    value: object,
    path: tuple[str, ...] = (),
    depth: int = 0,
    max_depth: int = 2,
):
    if depth > max_depth:
        return

    if isinstance(value, dict):
        for key, nested in value.items():
            yield from iter_mapping_leaves(
                nested,
                path + (canonical_key(str(key)),),
                depth + 1,
                max_depth,
            )
        return

    if isinstance(value, list):
        if depth >= max_depth:
            return
        for nested in value:
            if isinstance(nested, (dict, list)):
                yield from iter_mapping_leaves(nested, path, depth + 1, max_depth)
        return

    yield path, value


def extract_candidate_name(mapping: dict[str, object]) -> str:
    for path, value in iter_mapping_leaves(mapping, max_depth=2):
        if not path or not isinstance(value, str):
            continue
        if path[-1] in DIRECTORY_NAME_KEYS and is_companyish_text(value):
            return normalize_text(value)
    return ""


def extract_candidate_profile_url(mapping: dict[str, object], seed_url: str) -> str:
    for path, value in iter_mapping_leaves(mapping, max_depth=2):
        if not path or not isinstance(value, str):
            continue
        if path[-1] not in DIRECTORY_PROFILE_URL_KEYS:
            continue

        normalized = normalize_http_url(urljoin(seed_url, value))
        if normalized and same_site(normalized, seed_url):
            return normalized
    return ""


def extract_candidate_website_url(mapping: dict[str, object], seed_url: str) -> str:
    for path, value in iter_mapping_leaves(mapping, max_depth=2):
        if not path or not isinstance(value, str):
            continue

        if path[-1] not in DIRECTORY_WEBSITE_KEYS and not any(
            "website" in part for part in path
        ):
            continue

        normalized = normalize_http_url(urljoin(seed_url, value))
        if normalized and not same_site(normalized, seed_url):
            return normalized
    return ""


def build_swapcard_profile_url(seed_url: str, event_slug: str, exhibitor_id: str) -> str:
    parsed = urlparse(seed_url)
    return urlunparse(
        parsed._replace(
            path=(
                f"/event/{quote(event_slug, safe='')}/"
                f"exhibitor/{quote(exhibitor_id, safe='')}"
            ),
            query="",
            fragment="",
        )
    )


def extract_event_slug(data: object) -> str:
    candidate_containers: list[dict[str, object]] = []
    if isinstance(data, dict):
        candidate_containers.append(data)
        props = data.get("props")
        if isinstance(props, dict):
            candidate_containers.append(props)
            page_props = props.get("pageProps")
            if isinstance(page_props, dict):
                candidate_containers.append(page_props)
        query = data.get("query")
        if isinstance(query, dict):
            candidate_containers.append(query)

    for container in candidate_containers:
        for key in ("eventSlug", "activeEventSlug", "slug"):
            value = normalize_text(str(container.get(key) or ""))
            if value:
                return value
    return ""


def extract_candidate_title_from_data(data: object) -> str:
    candidates: list[str] = []
    if not isinstance(data, dict):
        return ""

    for container in (
        data,
        data.get("props") if isinstance(data.get("props"), dict) else None,
        (
            data.get("props", {}).get("pageProps")
            if isinstance(data.get("props"), dict)
            and isinstance(data.get("props", {}).get("pageProps"), dict)
            else None
        ),
    ):
        if not isinstance(container, dict):
            continue
        for key in ("title", "eventTitle", "eventName", "conferenceName", "name"):
            value = normalize_text(str(container.get(key) or ""))
            if value:
                candidates.append(value)

    valid_candidates = [
        candidate
        for candidate in candidates
        if score_conference_name_candidate(candidate) > float("-inf")
    ]
    if not valid_candidates:
        return ""
    return max(valid_candidates, key=score_conference_name_candidate)


def build_candidate_from_mapping(
    mapping: dict[str, object],
    seed_url: str,
    event_slug: str,
) -> ExtractedEntryCandidate | None:
    company_name = extract_candidate_name(mapping)
    if not company_name:
        return None

    profile_url = extract_candidate_profile_url(mapping, seed_url)
    website_url_hint = extract_candidate_website_url(mapping, seed_url)

    if not profile_url and is_swapcard_site(seed_url) and event_slug:
        typename = canonical_label(str(mapping.get("__typename") or ""))
        exhibitor_id = normalize_text(str(mapping.get("id") or mapping.get("_id") or ""))
        if exhibitor_id and "exhibitor" in typename:
            profile_url = build_swapcard_profile_url(seed_url, event_slug, exhibitor_id)

    if not profile_url and not website_url_hint:
        return None

    return ExtractedEntryCandidate(
        company_name=company_name,
        profile_url=profile_url,
        website_url_hint=website_url_hint,
    )


def extract_apollo_items(connection_value: object) -> list[object]:
    if isinstance(connection_value, dict):
        nodes = connection_value.get("nodes")
        if isinstance(nodes, list):
            return list(nodes)

        edges = connection_value.get("edges")
        if isinstance(edges, list):
            items: list[object] = []
            for edge in edges:
                if isinstance(edge, dict) and "node" in edge:
                    items.append(edge["node"])
            return items

    if isinstance(connection_value, list):
        return list(connection_value)
    return []


def resolve_apollo_ref(value: object, apollo_state: dict[str, object]) -> dict[str, object] | None:
    if isinstance(value, dict):
        reference = value.get("__ref")
        if isinstance(reference, str):
            target = apollo_state.get(reference)
            return target if isinstance(target, dict) else None
        return value

    if isinstance(value, str):
        target = apollo_state.get(value)
        return target if isinstance(target, dict) else None

    return None


def extract_entries_from_apollo_state(
    apollo_state: dict[str, object],
    seed_url: str,
    event_slug: str,
) -> list[ExtractedEntryCandidate]:
    candidates: list[ExtractedEntryCandidate] = []

    for container in apollo_state.values():
        if not isinstance(container, dict):
            continue

        for key, value in container.items():
            key_label = canonical_label(key)
            if not any(token in key_label for token in LIKELY_DIRECTORY_LIST_KEYS):
                continue

            for item in extract_apollo_items(value):
                resolved = resolve_apollo_ref(item, apollo_state)
                if not resolved:
                    continue
                candidate = build_candidate_from_mapping(resolved, seed_url, event_slug)
                if candidate is not None:
                    candidates.append(candidate)

    if candidates:
        return candidates

    for item in apollo_state.values():
        if not isinstance(item, dict):
            continue
        typename = canonical_label(str(item.get("__typename") or ""))
        if not any(token in typename for token in ("company", "exhibitor", "sponsor", "vendor")):
            continue
        candidate = build_candidate_from_mapping(item, seed_url, event_slug)
        if candidate is not None:
            candidates.append(candidate)

    return candidates


def extract_entries_from_named_lists(
    value: object,
    seed_url: str,
    event_slug: str,
    parent_key: str = "",
    depth: int = 0,
    max_depth: int = 6,
) -> list[ExtractedEntryCandidate]:
    if depth > max_depth:
        return []

    candidates: list[ExtractedEntryCandidate] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            key_label = canonical_label(key)
            if (
                isinstance(nested, list)
                and len(nested) >= 3
                and all(isinstance(item, dict) for item in nested)
                and any(token in key_label for token in LIKELY_DIRECTORY_LIST_KEYS)
            ):
                for item in nested:
                    candidate = build_candidate_from_mapping(item, seed_url, event_slug)
                    if candidate is not None:
                        candidates.append(candidate)

            candidates.extend(
                extract_entries_from_named_lists(
                    nested,
                    seed_url=seed_url,
                    event_slug=event_slug,
                    parent_key=key,
                    depth=depth + 1,
                    max_depth=max_depth,
                )
            )
    elif isinstance(value, list) and parent_key:
        for nested in value:
            candidates.extend(
                extract_entries_from_named_lists(
                    nested,
                    seed_url=seed_url,
                    event_slug=event_slug,
                    parent_key=parent_key,
                    depth=depth + 1,
                    max_depth=max_depth,
                )
            )

    return candidates


def dedupe_extracted_candidates(
    candidates: list[ExtractedEntryCandidate],
) -> list[DirectoryEntry]:
    entries: list[DirectoryEntry] = []
    seen: set[str] = set()

    for candidate in candidates:
        dedupe_key = (
            candidate.profile_url
            or candidate.website_url_hint
            or candidate.company_name.lower()
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        entries.append(
            DirectoryEntry(
                sort_index=len(entries),
                directory_page=1,
                company_name=candidate.company_name,
                profile_url=candidate.profile_url,
                website_url_hint=candidate.website_url_hint,
            )
        )

    return entries


def collect_directory_entries_from_embedded_data(
    seed_url: str,
    seed_html: str,
) -> tuple[list[DirectoryEntry], str] | None:
    data_objects: list[object] = []

    next_data = extract_next_data(seed_html)
    if next_data is not None:
        data_objects.append(next_data)

    for variable_name in JS_STATE_VARIABLES:
        parsed = extract_json_assignment_from_html(seed_html, variable_name)
        if parsed is not None:
            data_objects.append(parsed)

    best_entries: list[DirectoryEntry] = []
    best_title = ""

    for data in data_objects:
        event_slug = extract_event_slug(data)
        candidates: list[ExtractedEntryCandidate] = []

        if isinstance(data, dict):
            apollo_state = data.get("apolloState")
            if isinstance(apollo_state, dict):
                candidates.extend(
                    extract_entries_from_apollo_state(
                        apollo_state=apollo_state,
                        seed_url=seed_url,
                        event_slug=event_slug,
                    )
                )

        candidates.extend(
            extract_entries_from_named_lists(
                value=data,
                seed_url=seed_url,
                event_slug=event_slug,
            )
        )

        entries = dedupe_extracted_candidates(candidates)
        if len(entries) > len(best_entries):
            best_entries = entries
            best_title = extract_candidate_title_from_data(data)

    if len(best_entries) < 3:
        return None
    return best_entries, best_title


def clean_media_stem(text: str) -> str:
    stem = Path(unquote(text or "")).stem
    if not stem:
        return ""

    cleaned = stem.replace("_", " ").replace("-", " ").replace(".", " ")
    cleaned = re.sub(r"([a-z])([A-Z])", r"\1 \2", cleaned)
    cleaned = re.sub(r"([0-9])([A-Za-z])", r"\1 \2", cleaned)
    cleaned = re.sub(r"([A-Za-z])([0-9])", r"\1 \2", cleaned)
    cleaned = re.sub(r"(?i)logo$", " ", cleaned)
    cleaned = re.sub(
        r"\b(?:logo|monogram|stacked|square|final|new|hr|butternut)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+\d{2,}\s*$", " ", cleaned)
    return normalize_text(cleaned)


def looks_machine_generated_name(text: str) -> bool:
    label = canonical_label(text)
    if not label:
        return True
    if GENERIC_MEDIA_NAME_RE.fullmatch(label):
        return True
    if "mv 2" in label:
        return True
    compact = label.replace(" ", "")
    if compact.startswith("924723") and any(character.isdigit() for character in compact):
        return True
    if HASHY_NAME_RE.fullmatch(compact):
        return True
    if sum(character.isdigit() for character in compact) >= 6 and len(compact) >= 12:
        return True
    return False


def looks_low_confidence_company_name(text: str) -> bool:
    normalized = normalize_seed_company_name(text)
    if looks_machine_generated_name(normalized):
        return True
    words = normalized.split()
    if not words:
        return True
    if all(len(word) <= 2 for word in words):
        return True
    label = canonical_label(normalized)
    if any(marker in label for marker in ("logo", "monogram", "stacked", "square")):
        return True
    if re.search(r"\(\d+\)$", normalized):
        return True
    if len(words) == 2 and words[-1].isdigit():
        return True
    if len(words) == 1 and len(words[0]) >= 4 and not words[0].isupper():
        return True
    return False


def infer_name_from_url(url: str) -> str:
    host = host_key(url)
    if not host:
        return ""

    label = host.split(".", 1)[0]
    for prefix in HOST_LABEL_PREFIXES:
        if label.startswith(prefix) and len(label) - len(prefix) >= 3:
            label = label[len(prefix):]
            break

    cleaned = label.replace("-", " ").replace("_", " ")
    cleaned = re.sub(r"([a-z])([A-Z])", r"\1 \2", cleaned)
    cleaned = re.sub(r"([0-9])([A-Za-z])", r"\1 \2", cleaned)
    cleaned = re.sub(r"([A-Za-z])([0-9])", r"\1 \2", cleaned)
    cleaned = normalize_text(cleaned)
    if not cleaned:
        return ""

    if cleaned.islower():
        if " " not in cleaned and len(cleaned) <= 4:
            return cleaned.upper()
        return " ".join(part.capitalize() for part in cleaned.split())
    return cleaned


def normalize_seed_company_name(text: str) -> str:
    cleaned = html_unescape(text or "")
    cleaned = COMPANY_NAME_UI_NOISE_RE.sub("", cleaned)
    cleaned = re.sub(r"(?i)logo$", " ", cleaned)
    cleaned = re.sub(r"\b(?:logo|monogram|stacked|square)\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\(\d+\)$", " ", cleaned)
    cleaned = re.sub(r"\s+\d{2,}\s*$", " ", cleaned)
    cleaned = normalize_text(cleaned)
    words = cleaned.split()
    while words and canonical_label(words[-1]) in COMPANY_NAME_REGION_MARKERS:
        words.pop()
    cleaned = " ".join(words)
    if cleaned.islower():
        if " " not in cleaned and len(cleaned) <= 4:
            cleaned = cleaned.upper()
        else:
            cleaned = " ".join(part.capitalize() for part in cleaned.split())
    return cleaned


def infer_wix_gallery_item_name(item: dict[str, object], website_url: str) -> str:
    meta = item.get("metaData")
    if not isinstance(meta, dict):
        meta = {}

    candidates = [
        normalize_text(str(meta.get("title") or "")),
        normalize_text(str(meta.get("alt") or "")),
        clean_media_stem(str(meta.get("fileName") or "")),
        infer_name_from_url(website_url),
        clean_media_stem(str(meta.get("name") or "")),
    ]

    for candidate in candidates:
        candidate = normalize_seed_company_name(candidate)
        if not candidate or looks_machine_generated_name(candidate):
            continue
        if candidate.lower().startswith("http"):
            continue
        return candidate
    return ""


def wix_gallery_item_website_url(item: dict[str, object], seed_url: str) -> str:
    meta = item.get("metaData")
    if not isinstance(meta, dict):
        return ""

    link = meta.get("link")
    if not isinstance(link, dict):
        return ""

    values_to_try: list[str] = []
    data = link.get("data")
    if isinstance(data, dict):
        values_to_try.extend(
            str(data.get(key) or "")
            for key in ("url", "href", "link")
        )

    values_to_try.append(str(link.get("text") or ""))

    for value in values_to_try:
        normalized = normalize_http_url(urljoin(seed_url, value))
        if normalized and not same_site(normalized, seed_url):
            return normalized

    return ""


def score_wix_gallery_payload(
    gallery_data: dict[str, object],
    seed_url: str,
) -> tuple[float, list[ExtractedEntryCandidate]]:
    items = gallery_data.get("items")
    if not isinstance(items, list) or len(items) < 3:
        return float("-inf"), []

    candidates: list[ExtractedEntryCandidate] = []
    external_links = 0

    for item in items:
        if not isinstance(item, dict):
            continue

        website_url = wix_gallery_item_website_url(item, seed_url)
        if not website_url:
            continue
        external_links += 1

        company_name = infer_wix_gallery_item_name(item, website_url)
        if not company_name:
            continue

        candidates.append(
            ExtractedEntryCandidate(
                company_name=company_name,
                profile_url="",
                website_url_hint=website_url,
            )
        )

    if external_links < 3 or len(candidates) < 3:
        return float("-inf"), []

    score = external_links * 8 + len(candidates) * 4
    if external_links / max(len(items), 1) > 0.7:
        score += 40

    return score, candidates


def collect_wix_gallery_entries(
    seed_url: str,
    seed_html: str,
) -> tuple[list[DirectoryEntry], str] | None:
    warmup_data = extract_wix_warmup_data(seed_html)
    if not isinstance(warmup_data, dict):
        return None

    apps_warmup = warmup_data.get("appsWarmupData")
    if not isinstance(apps_warmup, dict):
        return None

    best_score = float("-inf")
    best_entries: list[DirectoryEntry] = []

    for app_data in apps_warmup.values():
        if not isinstance(app_data, dict):
            continue

        for key, value in app_data.items():
            if not key.endswith(WIX_GALLERY_DATA_SUFFIX) or not isinstance(value, dict):
                continue

            score, candidates = score_wix_gallery_payload(value, seed_url)
            if score <= best_score:
                continue

            deduped_entries = dedupe_extracted_candidates(candidates)
            if len(deduped_entries) < 3:
                continue

            best_score = score
            best_entries = deduped_entries

    if not best_entries:
        return None

    print(
        f"Recovered {len(best_entries)} direct participant website links from Wix gallery data."
    )
    return best_entries, ""


def collect_direct_landing_entries(
    seed_url: str,
    seed_html: str,
) -> tuple[list[DirectoryEntry], str] | None:
    wix_entries = collect_wix_gallery_entries(seed_url, seed_html)
    if wix_entries is not None:
        return wix_entries
    return None


def is_social_url(url: str) -> bool:
    host = host_key(url)
    return any(marker in host for marker in SOCIAL_HOST_MARKERS)


def is_companyish_text(text: str) -> bool:
    text = normalize_text(text)
    if not text:
        return False
    lower = text.lower()
    if lower in GENERIC_ANCHOR_PHRASES:
        return False
    if lower in PROFILE_ACTION_LABELS:
        return False
    if lower in PAGINATION_WORDS:
        return False
    if lower.startswith("page ") and lower[5:].isdigit():
        return False
    if len(text) < 2 or len(text) > 140:
        return False
    if len(text.split()) > 20:
        return False
    alpha_count = sum(character.isalpha() for character in text)
    digit_count = sum(character.isdigit() for character in text)
    if alpha_count < 2:
        compact = text.replace(" ", "")
        if not (
            alpha_count >= 1
            and digit_count >= 1
            and len(compact) >= 3
            and len(compact) <= 10
            and re.fullmatch(r"[A-Za-z0-9.+&_-]+", compact)
            and compact.upper() == compact
        ):
            return False
    if not re.search(r"[A-Za-z]", text):
        return False
    return True


def url_group(url: str) -> str:
    parsed = urlparse(url)
    path_segments = [segment.lower() for segment in parsed.path.split("/") if segment]
    query_keys = sorted(
        key.lower()
        for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in COMMON_PAGE_PARAMS
    )

    if any(key in ID_LIKE_QUERY_KEYS for key in query_keys):
        cleaned_keys = [key for key in query_keys if key in ID_LIKE_QUERY_KEYS]
        return f"{parsed.path.lower()}?{','.join(cleaned_keys)}"

    if len(path_segments) >= 2:
        return "/" + "/".join(path_segments[:-1]) + "/*"
    if len(path_segments) == 1:
        segment = path_segments[0]
        if segment.isdigit():
            return "/{num}"
        return "/{slug}"
    return parsed.path.lower() or "/"


def choose_better_link(
    existing: AnchorRecord | ActionRecord,
    candidate: AnchorRecord | ActionRecord,
) -> AnchorRecord | ActionRecord:
    existing_text = existing.display_text
    candidate_text = candidate.display_text

    if not existing_text:
        return candidate
    if not candidate_text:
        return existing
    if DOMAIN_TEXT_RE.fullmatch(candidate_text) and not DOMAIN_TEXT_RE.fullmatch(existing_text):
        return candidate
    if len(candidate_text) > len(existing_text):
        return candidate
    return existing


def choose_better_container_candidate(
    existing: ContainerEntryCandidate,
    candidate: ContainerEntryCandidate,
) -> ContainerEntryCandidate:
    if len(candidate.company_name) > len(existing.company_name):
        return candidate
    return existing


def is_same_site_company_link(
    record: AnchorRecord | ActionRecord,
    directory_url: str,
) -> bool:
    if not record.absolute_url or looks_like_asset(record.absolute_url):
        return False
    if not same_site(record.absolute_url, directory_url):
        return False
    if record.in_header or record.in_footer or record.in_nav:
        return False
    if not is_companyish_text(record.display_text):
        return False
    return True


def listing_strategy_bonus(group: str, texts: list[str]) -> float:
    bonus = 0.0
    if any(
        keyword in group
        for keyword in ("exhibitor", "vendor", "profile", "company", "listing", "sponsor", "showroom")
    ):
        bonus += 20
    average_length = sum(len(text) for text in texts) / len(texts)
    if 4 <= average_length <= 80:
        bonus += 10
    if any(text.lower().startswith("www.") for text in texts):
        bonus -= 15
    if group in {"/{slug}", "/{num}"}:
        bonus -= 8
    return bonus


def build_link_listing_candidates(
    records: list[AnchorRecord] | list[ActionRecord],
    directory_url: str,
    source_kind: str,
) -> list[ListingStrategy]:
    grouped: dict[tuple[tuple[str, ...], str], list[AnchorRecord | ActionRecord]] = defaultdict(list)

    for record in records:
        if not is_same_site_company_link(record, directory_url):
            continue
        key = (record.signature, url_group(record.absolute_url))
        grouped[key].append(record)

    strategies: list[ListingStrategy] = []
    for (signature, group), members in grouped.items():
        by_url: dict[str, AnchorRecord | ActionRecord] = {}
        for member in members:
            existing = by_url.get(member.absolute_url)
            if existing is None:
                by_url[member.absolute_url] = member
            else:
                by_url[member.absolute_url] = choose_better_link(existing, member)

        unique_urls = len(by_url)
        if unique_urls < 3:
            continue

        texts = [record.display_text for record in by_url.values() if record.display_text]
        if not texts:
            continue

        diversity = len({text.lower() for text in texts}) / unique_urls
        score = unique_urls * 12 + diversity * 30 + listing_strategy_bonus(group, texts)
        strategies.append(
            ListingStrategy(
                source_kind=source_kind,
                signature=signature,
                url_group=group,
                base_score=score,
                unique_urls=unique_urls,
                sample_names=tuple(texts[:3]),
            )
        )

    fallback_links = [
        record
        for record in records
        if is_same_site_company_link(record, directory_url)
    ]
    unique_fallback_urls = {record.absolute_url for record in fallback_links}
    if len(unique_fallback_urls) >= 3:
        fallback_texts = [record.display_text for record in fallback_links if record.display_text]
        diversity = (
            len({text.lower() for text in fallback_texts}) / len(unique_fallback_urls)
            if fallback_texts
            else 0.0
        )
        strategies.append(
            ListingStrategy(
                source_kind=source_kind,
                signature=(),
                url_group="*",
                base_score=len(unique_fallback_urls) * 12 + diversity * 20 + 5,
                unique_urls=len(unique_fallback_urls),
                sample_names=tuple(record.display_text for record in fallback_links[:3]),
            )
        )

    return strategies


def page_action_records(page: ParsedPage) -> list[ActionRecord]:
    return [
        action
        for action in page.actions
        if action.tag != "a" or action.source_attr != "href"
    ]


def score_container_company_name(text: str) -> float:
    if not is_companyish_text(text):
        return float("-inf")

    score = 20.0
    words = text.split()
    if 1 <= len(words) <= 6:
        score += 10
    if len(text) <= 80:
        score += 8
    if any(character.isdigit() for character in text):
        score -= 6
    return score


def score_profile_action(action: ActionRecord, directory_url: str) -> float:
    if not action.absolute_url or looks_like_asset(action.absolute_url):
        return float("-inf")
    if not same_site(action.absolute_url, directory_url):
        return float("-inf")
    if action.in_header or action.in_footer or action.in_nav:
        return float("-inf")

    score = 18.0
    path = urlparse(action.absolute_url).path.lower()
    label = action.display_text.lower()
    query_keys = {
        key.lower()
        for key, _ in parse_qsl(urlparse(action.absolute_url).query, keep_blank_values=True)
    }

    if action.source_attr != "href":
        score += 8
    if action.tag in {"button", "div"}:
        score += 6
    if action.in_main:
        score += 5
    if any(marker in path for marker in PROFILE_URL_MARKERS):
        score += 35
    if label in PROFILE_ACTION_LABELS or any(marker in label for marker in PROFILE_ACTION_LABELS):
        score += 20
    if len([segment for segment in path.split("/") if segment]) >= 2:
        score += 6
    if normalize_http_url(action.absolute_url) == normalize_http_url(directory_url):
        score -= 80
    if extract_page_number_from_url(action.absolute_url) is not None:
        score -= 40
    if query_keys & set(COMMON_PAGE_PARAMS):
        score -= 35

    return score


def extract_container_candidate(
    container: ContainerRecord,
    directory_url: str,
) -> ContainerEntryCandidate | None:
    if container.in_header or container.in_footer or container.in_nav:
        return None

    heading_candidates = dedupe_preserving_order(list(container.heading_texts))
    if not heading_candidates:
        return None
    if len(heading_candidates) > 5:
        return None

    company_name = ""
    best_name_score = float("-inf")
    for heading_text in heading_candidates:
        score = score_container_company_name(heading_text)
        if score > best_name_score:
            best_name_score = score
            company_name = heading_text

    if not company_name:
        return None

    by_url: dict[str, tuple[float, ActionRecord]] = {}
    for action in container.actions:
        score = score_profile_action(action, directory_url)
        if score == float("-inf"):
            continue
        existing = by_url.get(action.absolute_url)
        if existing is None or score > existing[0]:
            by_url[action.absolute_url] = (score, action)

    if not by_url or len(by_url) > 3:
        return None

    best_profile_url = ""
    best_profile_score = float("-inf")
    for profile_url, (score, _action) in by_url.items():
        if score > best_profile_score:
            best_profile_score = score
            best_profile_url = profile_url

    if best_profile_score < 18.0 or not best_profile_url:
        return None

    return ContainerEntryCandidate(
        company_name=company_name,
        profile_url=best_profile_url,
        signature=container.signature,
        order=container.order,
    )


def build_container_listing_candidates(
    page: ParsedPage,
    directory_url: str,
) -> list[ListingStrategy]:
    grouped: dict[tuple[tuple[str, ...], str], list[ContainerEntryCandidate]] = defaultdict(list)
    fallback_candidates: list[ContainerEntryCandidate] = []

    for container in page.containers:
        candidate = extract_container_candidate(container, directory_url)
        if candidate is None:
            continue
        fallback_candidates.append(candidate)
        grouped[(candidate.signature, url_group(candidate.profile_url))].append(candidate)

    strategies: list[ListingStrategy] = []
    for (signature, group), candidates in grouped.items():
        by_url: dict[str, ContainerEntryCandidate] = {}
        for candidate in candidates:
            existing = by_url.get(candidate.profile_url)
            if existing is None:
                by_url[candidate.profile_url] = candidate
            else:
                by_url[candidate.profile_url] = choose_better_container_candidate(existing, candidate)

        unique_urls = len(by_url)
        if unique_urls < 3:
            continue

        texts = [candidate.company_name for candidate in by_url.values() if candidate.company_name]
        if not texts:
            continue

        diversity = len({text.lower() for text in texts}) / unique_urls
        score = unique_urls * 14 + diversity * 35 + listing_strategy_bonus(group, texts)
        strategies.append(
            ListingStrategy(
                source_kind="container",
                signature=signature,
                url_group=group,
                base_score=score,
                unique_urls=unique_urls,
                sample_names=tuple(texts[:3]),
            )
        )

    unique_fallback_urls = {candidate.profile_url for candidate in fallback_candidates}
    if len(unique_fallback_urls) >= 3:
        fallback_texts = [
            candidate.company_name
            for candidate in fallback_candidates
            if candidate.company_name
        ]
        diversity = (
            len({text.lower() for text in fallback_texts}) / len(unique_fallback_urls)
            if fallback_texts
            else 0.0
        )
        strategies.append(
            ListingStrategy(
                source_kind="container",
                signature=(),
                url_group="*",
                base_score=len(unique_fallback_urls) * 14 + diversity * 30 + 10,
                unique_urls=len(unique_fallback_urls),
                sample_names=tuple(candidate.company_name for candidate in fallback_candidates[:3]),
            )
        )

    return strategies


def build_listing_candidates(page: ParsedPage, directory_url: str) -> list[ListingStrategy]:
    strategies: list[ListingStrategy] = []
    strategies.extend(
        build_link_listing_candidates(
            list(page.anchors),
            directory_url=directory_url,
            source_kind="anchor",
        )
    )
    strategies.extend(
        build_link_listing_candidates(
            page_action_records(page),
            directory_url=directory_url,
            source_kind="action",
        )
    )
    strategies.extend(build_container_listing_candidates(page, directory_url))
    strategies.sort(key=lambda strategy: (strategy.base_score, strategy.unique_urls), reverse=True)
    return strategies[:12]


def extract_directory_entries_from_links(
    records: list[AnchorRecord] | list[ActionRecord],
    strategy: ListingStrategy,
    directory_url: str,
) -> list[tuple[str, str]]:
    grouped: dict[str, AnchorRecord | ActionRecord] = {}

    def matches(record: AnchorRecord | ActionRecord) -> bool:
        if not is_same_site_company_link(record, directory_url):
            return False
        if strategy.signature and record.signature != strategy.signature:
            return False
        if strategy.url_group != "*" and url_group(record.absolute_url) != strategy.url_group:
            return False
        return True

    candidates = [record for record in records if matches(record)]
    if len(candidates) < 2 and strategy.signature:
        candidates = [
            record
            for record in records
            if is_same_site_company_link(record, directory_url)
            and record.signature == strategy.signature
        ]

    if len(candidates) < 2 and strategy.url_group != "*":
        candidates = [
            record
            for record in records
            if is_same_site_company_link(record, directory_url)
            and url_group(record.absolute_url) == strategy.url_group
        ]

    for candidate in candidates:
        existing = grouped.get(candidate.absolute_url)
        if existing is None:
            grouped[candidate.absolute_url] = candidate
        else:
            grouped[candidate.absolute_url] = choose_better_link(existing, candidate)

    entries = [
        (record.display_text, record.absolute_url)
        for record in grouped.values()
        if record.display_text
    ]
    entries.sort(key=lambda item: item[0].lower())
    return entries


def extract_directory_entries_from_containers(
    page: ParsedPage,
    strategy: ListingStrategy,
    directory_url: str,
) -> list[tuple[str, str]]:
    grouped: dict[str, ContainerEntryCandidate] = {}
    candidates = [
        candidate
        for container in page.containers
        if (candidate := extract_container_candidate(container, directory_url)) is not None
    ]

    def matches(candidate: ContainerEntryCandidate) -> bool:
        if strategy.signature and candidate.signature != strategy.signature:
            return False
        if strategy.url_group != "*" and url_group(candidate.profile_url) != strategy.url_group:
            return False
        return True

    matching_candidates = [candidate for candidate in candidates if matches(candidate)]
    if len(matching_candidates) < 2 and strategy.signature:
        matching_candidates = [
            candidate for candidate in candidates if candidate.signature == strategy.signature
        ]

    if len(matching_candidates) < 2 and strategy.url_group != "*":
        matching_candidates = [
            candidate
            for candidate in candidates
            if url_group(candidate.profile_url) == strategy.url_group
        ]

    for candidate in matching_candidates:
        existing = grouped.get(candidate.profile_url)
        if existing is None:
            grouped[candidate.profile_url] = candidate
        else:
            grouped[candidate.profile_url] = choose_better_container_candidate(existing, candidate)

    entries = [
        (candidate.company_name, candidate.profile_url)
        for candidate in grouped.values()
        if candidate.company_name
    ]
    entries.sort(key=lambda item: item[0].lower())
    return entries


def extract_directory_entries(
    page: ParsedPage,
    strategy: ListingStrategy,
    directory_url: str,
) -> list[tuple[str, str]]:
    if strategy.source_kind == "anchor":
        return extract_directory_entries_from_links(
            list(page.anchors),
            strategy=strategy,
            directory_url=directory_url,
        )
    if strategy.source_kind == "action":
        return extract_directory_entries_from_links(
            page_action_records(page),
            strategy=strategy,
            directory_url=directory_url,
        )
    if strategy.source_kind == "container":
        return extract_directory_entries_from_containers(
            page,
            strategy=strategy,
            directory_url=directory_url,
        )
    return []


def parse_json_ld_urls(blocks: tuple[str, ...], directory_url: str) -> list[str]:
    urls: list[str] = []

    def visit(value: object, parent_key: str | None = None) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                visit(nested, key)
            return
        if isinstance(value, list):
            for nested in value:
                visit(nested, parent_key)
            return
        if not isinstance(value, str):
            return

        normalized = normalize_http_url(value)
        if not normalized:
            return
        if same_site(normalized, directory_url):
            return
        if parent_key == "url":
            urls.append(normalized)
        elif parent_key == "sameAs" and not is_social_url(normalized):
            urls.append(normalized)

    for block in blocks:
        try:
            parsed = json.loads(block)
        except json.JSONDecodeError:
            continue
        visit(parsed)

    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def score_external_link(
    link: AnchorRecord | ActionRecord,
    profile_url: str,
) -> float:
    if not link.absolute_url:
        return float("-inf")
    if same_site(link.absolute_url, profile_url):
        return float("-inf")
    if looks_like_asset(link.absolute_url):
        return float("-inf")

    text = link.display_text.lower()
    host = host_key(link.absolute_url)
    parsed_target = urlparse(link.absolute_url)
    path_and_query = f"{parsed_target.path.lower()}?{parsed_target.query.lower()}"
    query_keys = {
        key.lower()
        for key, _ in parse_qsl(parsed_target.query, keep_blank_values=True)
    }
    score = 40.0

    if link.in_footer or link.in_header:
        score -= 35
    if link.in_nav:
        score -= 25
    if link.in_main:
        score += 12
    if any(part in " ".join(link.signature).lower() for part in ("social", "share", "footer", "nav")):
        score -= 25

    if "website" in text or "homepage" in text or "visit" in text:
        score += 25
    if text.startswith("www.") or DOMAIN_TEXT_RE.fullmatch(text):
        score += 30
    if host and "." in host:
        score += 4
    if is_social_url(link.absolute_url):
        score -= 18
    if any(text == phrase for phrase in ("facebook", "instagram", "linkedin", "twitter", "youtube", "pinterest")):
        score -= 30
    if any(marker in host for marker in EVENT_LINK_HOST_MARKERS):
        score -= 18
    if any(marker in path_and_query for marker in EVENT_LINK_PATH_MARKERS):
        score -= 85
    if query_keys & EVENT_LINK_QUERY_KEYS:
        score -= 85
    if any(marker in text for marker in EVENT_LINK_TEXT_MARKERS):
        score -= 45

    return score


def extract_company_website(page: ParsedPage, profile_url: str) -> str:
    json_ld_urls = parse_json_ld_urls(page.json_ld_blocks, profile_url)
    if json_ld_urls:
        return json_ld_urls[0]

    non_social_candidates: list[tuple[float, str]] = []
    candidate_links: dict[str, AnchorRecord | ActionRecord] = {}

    for link in [*page.anchors, *page_action_records(page)]:
        if not link.absolute_url:
            continue
        if same_site(link.absolute_url, profile_url):
            continue

        existing = candidate_links.get(link.absolute_url)
        if existing is None:
            candidate_links[link.absolute_url] = link
        else:
            candidate_links[link.absolute_url] = choose_better_link(existing, link)

    for link in candidate_links.values():
        score = score_external_link(link, profile_url)
        if score == float("-inf"):
            continue
        if is_social_url(link.absolute_url):
            continue
        non_social_candidates.append((score, link.absolute_url))

    non_social_candidates.sort(key=lambda item: item[0], reverse=True)

    if non_social_candidates and non_social_candidates[0][0] >= 35:
        return non_social_candidates[0][1]
    return ""


def company_name_brand_key(text: str) -> str:
    tokens = canonical_label(normalize_seed_company_name(text)).split()
    filtered_tokens = [
        token
        for token in tokens
        if token not in {"home", "homepage", "official", "site", "store", "website"}
    ]
    while filtered_tokens and filtered_tokens[-1] in COMPANY_NAME_REGION_MARKERS:
        filtered_tokens.pop()
    return "".join(filtered_tokens)


def company_name_lookup_url(website_url: str) -> str:
    normalized = normalize_http_url(website_url)
    if not normalized:
        return website_url

    parsed = urlparse(normalized)
    if parsed.path not in {"", "/"} or parsed.query:
        return urlunparse(parsed._replace(path="/", query="", fragment=""))
    return normalized


def candidate_matches_website_brand(text: str, website_url: str) -> bool:
    host_brand_key = company_name_brand_key(infer_name_from_url(website_url))
    candidate_brand_key = company_name_brand_key(text)
    if not host_brand_key or not candidate_brand_key:
        return False
    return (
        candidate_brand_key == host_brand_key
        or host_brand_key in candidate_brand_key
        or candidate_brand_key in host_brand_key
    )


def extract_brandish_subcandidates(text: str) -> list[str]:
    cleaned = normalize_seed_company_name(text)
    if not cleaned:
        return []

    candidates = [cleaned]
    lowered = cleaned.lower()
    for marker in (" by ", " from "):
        index = lowered.find(marker)
        if index == -1:
            continue
        tail = normalize_seed_company_name(cleaned[index + len(marker):])
        if tail:
            candidates.append(tail)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def extract_meta_name_candidates(html_text: str) -> list[tuple[float, str]]:
    candidates: list[tuple[float, str]] = []

    for match in META_TAG_RE.finditer(html_text):
        attrs = {
            key.lower(): html_unescape(value)
            for key, _, value in META_ATTR_RE.findall(match.group(1))
        }
        key = attrs.get("property") or attrs.get("name")
        content = normalize_seed_company_name(attrs.get("content", ""))
        if not key or not content:
            continue

        weight = META_NAME_WEIGHTS.get(key.lower())
        if weight is None:
            continue

        for candidate in extract_brandish_subcandidates(content):
            candidates.append((weight, candidate))

    return candidates


def extract_json_ld_name_candidates(blocks: tuple[str, ...]) -> list[tuple[float, str]]:
    candidates: list[tuple[float, str]] = []

    def visit(value: object) -> None:
        if isinstance(value, dict):
            raw_type = value.get("@type") or value.get("type")
            type_names: list[str] = []
            if isinstance(raw_type, str):
                type_names = [raw_type]
            elif isinstance(raw_type, list):
                type_names = [str(item) for item in raw_type if isinstance(item, str)]

            weight = 0.0
            for type_name in type_names:
                weight = max(weight, JSON_LD_NAME_WEIGHTS.get(type_name.lower(), 0.0))

            for field_name in ("name", "alternateName", "legalName"):
                field_value = value.get(field_name)
                if isinstance(field_value, str) and weight > 0:
                    for candidate in extract_brandish_subcandidates(field_value):
                        candidates.append((weight, candidate))

            for nested in value.values():
                visit(nested)
            return

        if isinstance(value, list):
            for nested in value:
                visit(nested)

    for block in blocks:
        try:
            parsed = json.loads(block)
        except json.JSONDecodeError:
            continue
        visit(parsed)

    return candidates


def score_company_name_candidate(
    text: str,
    website_url: str,
    source_weight: float = 0.0,
) -> float:
    normalized = normalize_seed_company_name(text)
    if not normalized or looks_machine_generated_name(normalized):
        return float("-inf")

    label = canonical_label(normalized)
    if label in {"home", "homepage", "welcome"}:
        return float("-inf")

    words = normalized.split()
    score = source_weight + 20.0
    if 1 <= len(words) <= 5:
        score += 32
    elif len(words) <= 7:
        score += 12
    else:
        score -= 30
    if len(normalized) > 60:
        score -= 25
    if len(normalized) > 90:
        score -= 30
    if any(marker in label for marker in COMPANY_NAME_GENERIC_MARKERS):
        score -= 30
    if any(character.isdigit() for character in normalized):
        score -= 12

    host_brand_key = company_name_brand_key(infer_name_from_url(website_url))
    candidate_brand_key = company_name_brand_key(normalized)
    if host_brand_key and candidate_brand_key:
        if candidate_brand_key == host_brand_key:
            score += 125
        elif host_brand_key in candidate_brand_key or candidate_brand_key in host_brand_key:
            score += 70

    if normalized.isupper() and len(normalized) <= 10:
        score += 8
    return score


@lru_cache(maxsize=512)
def infer_company_name_from_website(website_url: str) -> str:
    lookup_url = company_name_lookup_url(website_url)
    website_html = fetch_html(lookup_url)
    website_page = parse_page(lookup_url, website_html)

    candidates: list[tuple[float, str]] = []
    candidates.extend(extract_meta_name_candidates(website_html))
    candidates.extend(extract_json_ld_name_candidates(website_page.json_ld_blocks))

    if website_page.title:
        for part in TITLE_SPLIT_RE.split(website_page.title):
            if not normalize_text(part):
                continue
            for candidate in extract_brandish_subcandidates(part):
                candidates.append((90.0, candidate))
        for candidate in extract_brandish_subcandidates(website_page.title):
            candidates.append((55.0, candidate))

    for heading in website_page.h1_texts:
        for candidate in extract_brandish_subcandidates(heading):
            candidates.append((75.0, candidate))

    best_candidate = ""
    best_score = float("-inf")
    best_matches_brand = False
    best_source_weight = 0.0
    seen: set[str] = set()
    for source_weight, candidate in candidates:
        normalized = normalize_seed_company_name(candidate)
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)

        score = score_company_name_candidate(
            normalized,
            website_url=website_url,
            source_weight=source_weight,
        )
        if score > best_score:
            best_score = score
            best_candidate = normalized
            best_matches_brand = candidate_matches_website_brand(normalized, website_url)
            best_source_weight = source_weight

    if best_score == float("-inf"):
        return ""

    if not best_matches_brand and best_source_weight < 150.0:
        fallback = normalize_seed_company_name(infer_name_from_url(website_url))
        if looks_low_confidence_company_name(fallback):
            return ""
        return fallback

    return best_candidate or normalize_seed_company_name(infer_name_from_url(website_url))


def maybe_enrich_company_name(company_name: str, website_url: str) -> str:
    cleaned_company_name = normalize_seed_company_name(company_name)
    if not website_url:
        return cleaned_company_name
    if not looks_low_confidence_company_name(cleaned_company_name):
        return cleaned_company_name

    try:
        enriched = infer_company_name_from_website(website_url)
    except Exception:  # noqa: BLE001
        return cleaned_company_name

    return normalize_seed_company_name(enriched or cleaned_company_name)


def evaluate_listing_strategy(
    strategy: ListingStrategy,
    seed_page: ParsedPage,
    directory_url: str,
    sample_size: int,
    profile_website_scraper: callable[[str], str],
) -> tuple[float, list[tuple[str, str]]]:
    entries = extract_directory_entries(seed_page, strategy, directory_url)
    if not entries:
        return float("-inf"), []

    score = strategy.base_score + min(len(entries), 20)
    successes = 0
    sample_entries = entries[:sample_size]

    for company_name, profile_url in sample_entries:
        try:
            website_url = profile_website_scraper(profile_url)
        except Exception:  # noqa: BLE001
            website_url = ""

        if website_url:
            successes += 1
            score += 90
        elif company_name:
            score += 5

    return score, entries


def choose_listing_strategy(
    seed_page: ParsedPage,
    directory_url: str,
    sample_size: int,
    profile_website_scraper: callable[[str], str] = None,
) -> tuple[ListingStrategy, list[tuple[str, str]]]:
    if profile_website_scraper is None:
        profile_website_scraper = scrape_profile_website

    strategies = build_listing_candidates(seed_page, directory_url)
    if not strategies:
        raise RuntimeError(
            "Could not infer company/profile links from the directory page. "
            "This directory may be JS-only or structurally unusual."
        )

    best_strategy: ListingStrategy | None = None
    best_entries: list[tuple[str, str]] = []
    best_score = float("-inf")

    for strategy in strategies:
        score, entries = evaluate_listing_strategy(
            strategy=strategy,
            seed_page=seed_page,
            directory_url=directory_url,
            sample_size=sample_size,
            profile_website_scraper=profile_website_scraper,
        )
        if score > best_score:
            best_score = score
            best_strategy = strategy
            best_entries = entries

    if best_strategy is None or not best_entries:
        raise RuntimeError("Could not validate a reliable listing strategy from the seed page.")

    print(
        "Selected listing strategy with "
        f"{best_strategy.unique_urls} unique links on the seed page. "
        f"Examples: {', '.join(best_strategy.sample_names)}"
    )
    return best_strategy, best_entries


def extract_total_pages(html_text: str) -> int | None:
    values: list[int] = []
    for pattern in TOTAL_PAGE_PATTERNS:
        for match in re.findall(pattern, html_text, flags=re.IGNORECASE):
            try:
                value = int(match)
            except ValueError:
                continue
            if 1 < value <= 5000:
                values.append(value)
    if not values:
        return None
    return max(values)


def page_series_fingerprint(url: str) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    parsed = urlparse(url)
    path = PAGE_PATH_RE.sub("/page/{page}", parsed.path.lower())
    query_items = tuple(
        sorted(
            (key.lower(), value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() not in COMMON_PAGE_PARAMS
        )
    )
    return host_key(url), path, query_items


def extract_page_number_from_url(url: str) -> int | None:
    parsed = urlparse(url)
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in COMMON_PAGE_PARAMS and value.isdigit():
            return int(value)

    path_match = PAGE_PATH_RE.search(parsed.path)
    if path_match:
        return int(path_match.group(1))
    return None


def build_query_page_url(url: str, param_name: str, page_number: int) -> str:
    parsed = urlparse(url)
    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() != param_name.lower()
    ]
    query_items.append((param_name, str(page_number)))
    return urlunparse(
        parsed._replace(query=urlencode(query_items, doseq=True), fragment="")
    )


def build_path_page_url(url: str, page_number: int) -> str:
    parsed = urlparse(url)
    if PAGE_PATH_RE.search(parsed.path):
        new_path = PAGE_PATH_RE.sub(f"/page/{page_number}", parsed.path)
    else:
        base_path = parsed.path.rstrip("/")
        new_path = f"{base_path}/page/{page_number}" if base_path else f"/page/{page_number}"
    return urlunparse(parsed._replace(path=new_path, fragment=""))


def parse_simple_js_object(js_object_text: str) -> dict[str, str]:
    body = normalize_text(js_object_text)
    if not body.startswith("{") or not body.endswith("}"):
        return {}

    parsed: dict[str, str] = {}
    for match in JS_OBJECT_PAIR_RE.finditer(js_object_text):
        key = normalize_text(match.group("key"))
        if not key:
            continue

        value = ""
        if match.group("string") is not None:
            value = match.group("string")
            value = value.replace("\\'", "'").replace('\\"', '"')
        elif match.group("number") is not None:
            value = match.group("number")
        elif match.group("boolean") is not None:
            value = match.group("boolean")

        parsed[key] = html_unescape(value)

    return parsed


def extract_ajax_form_tokens(html_text: str) -> tuple[str, str] | None:
    tokens: dict[str, str] = {}
    for token_name, _quote, token_value in FORM_TOKEN_RE.findall(html_text):
        tokens[token_name] = token_value

    tk = tokens.get("tk")
    tm = tokens.get("tm")
    if not tk or not tm:
        return None
    return tk, tm


def ajax_paginator_request_headers(seed_url: str) -> dict[str, str]:
    parsed = urlparse(seed_url)
    origin = urlunparse(parsed._replace(path="", params="", query="", fragment=""))
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": origin.rstrip("/"),
        "Referer": seed_url,
        "X-Requested-With": "XMLHttpRequest",
    }


def discover_ajax_paginator_configs(
    seed_url: str,
    seed_html: str,
) -> list[AjaxPaginatorConfig]:
    configs: list[AjaxPaginatorConfig] = []
    seen: set[tuple[str, tuple[tuple[str, str], ...], int, int, str]] = set()

    for script_body in INLINE_SCRIPT_RE.findall(seed_html):
        if ".jsPaginator(" not in script_body:
            continue

        assigned_objects = {
            variable_name: object_body
            for variable_name, object_body in JS_VARIABLE_OBJECT_RE.findall(script_body)
        }

        for match in JS_PAGINATOR_CALL_RE.finditer(script_body):
            params_expression = match.group(1).strip()
            options_expression = match.group(2).strip()
            total_text = match.group(3) or "0"

            if params_expression.startswith("{"):
                params = parse_simple_js_object(params_expression)
            else:
                params = parse_simple_js_object(assigned_objects.get(params_expression, ""))

            if not params:
                continue
            if not params.get("module") or not params.get("method"):
                continue

            options = parse_simple_js_object(options_expression)
            page_id = options.get("pageID") or "openAjax"

            try:
                limit = int(params.get("limit") or 0)
                total_results = int(total_text)
                next_offset = int(params.get("offset") or 0)
            except ValueError:
                continue

            if limit <= 0:
                continue

            request_params = tuple(
                (key, str(value))
                for key, value in params.items()
                if key not in {"ajaxType", "page_id", "tk", "tm"}
            )
            key = (
                urljoin(seed_url, "/index.php"),
                request_params,
                limit,
                total_results,
                page_id,
            )
            if key in seen:
                continue
            seen.add(key)
            configs.append(
                AjaxPaginatorConfig(
                    endpoint_url=urljoin(seed_url, "/index.php"),
                    params=request_params,
                    limit=limit,
                    total_results=total_results,
                    next_offset=next_offset,
                    page_id=page_id,
                )
            )

    return configs


def ajax_total_pages(config: AjaxPaginatorConfig) -> int:
    if config.limit <= 0:
        return 1
    if config.total_results <= 0:
        return 1
    return max(1, (config.total_results + config.limit - 1) // config.limit)


def ajax_offset_for_page(config: AjaxPaginatorConfig, page_number: int) -> int:
    if page_number <= 1:
        return 0

    base_offset = config.next_offset if config.next_offset > 0 else config.limit
    return base_offset + (page_number - 2) * config.limit


def fetch_ajax_paginator_payload(
    seed_url: str,
    config: AjaxPaginatorConfig,
    page_number: int,
    form_token: str,
    form_time: str,
    include_ajax_type: bool,
) -> dict[str, object]:
    form_items = [
        (key, value)
        for key, value in config.params
        if key != "offset"
    ]
    form_items.extend(
        [
            ("offset", str(ajax_offset_for_page(config, page_number))),
            ("page_id", config.page_id),
            ("tk", form_token),
            ("tm", form_time),
        ]
    )
    if include_ajax_type:
        form_items.append(("ajaxType", "paginate"))

    payload_text = fetch_text(
        config.endpoint_url,
        extra_headers=ajax_paginator_request_headers(seed_url),
        form_data=form_items,
    )
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("AJAX paginator response was not valid JSON.") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("AJAX paginator returned an unexpected payload.")
    return payload


def collect_directory_entries_with_ajax_paginator(
    seed_url: str,
    seed_html: str,
    seed_page: ParsedPage,
    strategy: ListingStrategy,
    directory_url: str,
    start_page: int | None,
    end_page: int | None,
    max_pages: int,
) -> list[DirectoryEntry] | None:
    configs = discover_ajax_paginator_configs(seed_url, seed_html)
    if not configs:
        return None

    tokens = extract_ajax_form_tokens(seed_html)
    if tokens is None:
        return None

    seed_entries = extract_directory_entries(seed_page, strategy, directory_url)
    if not seed_entries:
        return None
    seed_profile_urls = {profile_url for _, profile_url in seed_entries}

    requested_start = max(start_page or 1, 1)
    chosen_config: AjaxPaginatorConfig | None = None
    cached_page_entries: dict[int, list[tuple[str, str]]] = {}
    current_tokens = tokens
    ajax_loaded = False

    for config in configs:
        total_pages = ajax_total_pages(config)
        if total_pages <= 1:
            continue

        requested_end = min(end_page or total_pages, total_pages)
        requested_end = min(requested_end, requested_start + max_pages - 1)
        if requested_start > requested_end:
            raise ValueError("--start-page cannot be greater than --end-page.")

        if requested_end == 1:
            chosen_config = config
            break

        sample_page = requested_start if requested_start > 1 else 2
        try:
            payload = fetch_ajax_paginator_payload(
                seed_url=seed_url,
                config=config,
                page_number=sample_page,
                form_token=current_tokens[0],
                form_time=current_tokens[1],
                include_ajax_type=True,
            )
        except Exception:  # noqa: BLE001
            continue

        fragment_html = unquote(str(payload.get("data") or ""))
        if not fragment_html.strip():
            continue

        sample_page_entries = extract_directory_entries(
            parse_page(seed_url, fragment_html),
            strategy,
            directory_url,
        )
        sample_profile_urls = {profile_url for _, profile_url in sample_page_entries}
        if not sample_profile_urls:
            continue
        if sample_page != 1 and sample_profile_urls == seed_profile_urls:
            continue

        chosen_config = config
        cached_page_entries[sample_page] = sample_page_entries
        current_tokens = (
            str(payload.get("formToken") or current_tokens[0]),
            str(payload.get("formTime") or current_tokens[1]),
        )
        ajax_loaded = True
        break

    if chosen_config is None:
        return None

    total_pages = ajax_total_pages(chosen_config)
    requested_end = min(end_page or total_pages, total_pages)
    requested_end = min(requested_end, requested_start + max_pages - 1)
    if requested_start > requested_end:
        raise ValueError("--start-page cannot be greater than --end-page.")

    print(
        "Detected AJAX paginator. "
        f"Will fetch up to {requested_end - requested_start + 1} page(s) "
        f"out of {total_pages} total."
    )

    entries: list[DirectoryEntry] = []
    seen_profiles: set[str] = set()

    for page_number in range(requested_start, requested_end + 1):
        if page_number == 1:
            page_entries = seed_entries
        elif page_number in cached_page_entries:
            page_entries = cached_page_entries[page_number]
        else:
            payload = fetch_ajax_paginator_payload(
                seed_url=seed_url,
                config=chosen_config,
                page_number=page_number,
                form_token=current_tokens[0],
                form_time=current_tokens[1],
                include_ajax_type=not ajax_loaded,
            )
            ajax_loaded = True
            current_tokens = (
                str(payload.get("formToken") or current_tokens[0]),
                str(payload.get("formTime") or current_tokens[1]),
            )
            fragment_html = unquote(str(payload.get("data") or ""))
            page_entries = extract_directory_entries(
                parse_page(seed_url, fragment_html),
                strategy,
                directory_url,
            )

        added = 0
        for company_name, profile_url in page_entries:
            if profile_url in seen_profiles:
                continue
            seen_profiles.add(profile_url)
            entries.append(
                DirectoryEntry(
                    sort_index=len(entries),
                    directory_page=page_number,
                    company_name=company_name,
                    profile_url=profile_url,
                )
            )
            added += 1

        print(
            f"Collected directory page {page_number} "
            f"(ajax mode) ({len(page_entries)} matches, {len(entries)} total, {added} new)."
        )

    return entries


def discover_query_page_param(seed_url: str, page: ParsedPage) -> str | None:
    for key, _ in parse_qsl(urlparse(seed_url).query, keep_blank_values=True):
        if key.lower() in COMMON_PAGE_PARAMS:
            return key

    counts: dict[str, int] = defaultdict(int)
    for anchor in page.anchors:
        if not anchor.absolute_url or not same_site(anchor.absolute_url, seed_url):
            continue
        for key, value in parse_qsl(urlparse(anchor.absolute_url).query, keep_blank_values=True):
            if key.lower() in COMMON_PAGE_PARAMS and value.isdigit():
                counts[key] += 1

    if not counts:
        return None
    return max(counts.items(), key=lambda item: item[1])[0]


def discover_explicit_page_urls(
    seed_url: str,
    seed_page: ParsedPage,
    seed_html: str,
    strategy: ListingStrategy,
    start_page: int | None,
    end_page: int | None,
    page_loader: callable[[str], tuple[str, str, ParsedPage]] | None = None,
) -> tuple[list[tuple[int, str]], int, str] | None:
    if page_loader is None:
        page_loader = load_static_page

    total_pages = extract_total_pages(seed_html)
    if not total_pages or total_pages <= 1:
        return None

    requested_start = max(start_page or 1, 1)
    requested_end = min(end_page or total_pages, total_pages)
    if requested_start > requested_end:
        raise ValueError("--start-page cannot be greater than --end-page.")

    seed_entries = extract_directory_entries(seed_page, strategy, seed_url)
    seed_profile_urls = {profile_url for _, profile_url in seed_entries}

    builders: list[tuple[str, callable[[int], str]]] = []
    known_param = None
    for key, value in parse_qsl(urlparse(seed_url).query, keep_blank_values=True):
        if key.lower() in COMMON_PAGE_PARAMS:
            known_param = key
            if value.isdigit():
                break

    if known_param:
        builders.append((f"query:{known_param}", lambda page, param=known_param: build_query_page_url(seed_url, param, page)))

    for param in COMMON_PAGE_PARAMS:
        if param == known_param:
            continue
        builders.append((f"query:{param}", lambda page, param=param: build_query_page_url(seed_url, param, page)))

    builders.append(("path:/page/{n}", lambda page: build_path_page_url(seed_url, page)))

    for label, builder in builders:
        test_page = 2 if total_pages >= 2 else 1
        test_url = builder(test_page)
        if normalize_http_url(test_url) == normalize_http_url(seed_url) and test_page == 1:
            continue

        try:
            _, test_html, test_page_parsed = page_loader(test_url)
            test_entries = extract_directory_entries(test_page_parsed, strategy, seed_url)
        except Exception:  # noqa: BLE001
            continue

        test_profile_urls = {profile_url for _, profile_url in test_entries}
        if test_profile_urls and test_profile_urls != seed_profile_urls:
            urls = [
                (page_number, builder(page_number))
                for page_number in range(requested_start, requested_end + 1)
            ]
            return urls, total_pages, label

    return None


def discover_pagination_links(
    page: ParsedPage,
    seed_url: str,
    current_url: str,
) -> list[str]:
    fingerprint = page_series_fingerprint(seed_url)
    candidates: list[str] = []

    for anchor in page.anchors:
        target_url = anchor.absolute_url
        if not target_url or target_url == current_url:
            continue
        if not same_site(target_url, seed_url):
            continue
        if page_series_fingerprint(target_url) != fingerprint:
            continue

        text = anchor.display_text.lower()
        page_number = extract_page_number_from_url(target_url)
        if page_number is not None:
            candidates.append(target_url)
            continue

        if text in PAGINATION_WORDS or text.isdigit():
            candidates.append(target_url)
            continue

    deduped: list[str] = []
    seen: set[str] = set()
    for url in sorted(
        candidates,
        key=lambda item: (extract_page_number_from_url(item) or 10**9, item),
    ):
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def collect_directory_entries_with_query_probing(
    seed_url: str,
    seed_page: ParsedPage,
    strategy: ListingStrategy,
    directory_url: str,
    param_name: str,
    start_page: int | None,
    end_page: int | None,
    max_pages: int,
    page_loader: callable[[str], tuple[str, str, ParsedPage]] | None = None,
) -> list[DirectoryEntry]:
    if page_loader is None:
        page_loader = load_static_page

    requested_start = max(start_page or 1, 1)
    requested_end = end_page or max_pages
    if requested_end < requested_start:
        raise ValueError("--start-page cannot be greater than --end-page.")

    entries: list[DirectoryEntry] = []
    seen_profiles: set[str] = set()
    seed_page_number = extract_page_number_from_url(seed_url)
    consecutive_stalls = 0

    for page_number in range(requested_start, requested_end + 1):
        page_url = build_query_page_url(seed_url, param_name, page_number)
        if seed_page_number == page_number or (
            seed_page_number is None
            and page_number == 1
            and normalize_http_url(page_url) == normalize_http_url(seed_url)
        ):
            page = seed_page
        else:
            _, _, page = page_loader(page_url)

        page_entries = extract_directory_entries(page, strategy, directory_url)
        added = 0
        for company_name, profile_url in page_entries:
            if profile_url in seen_profiles:
                continue
            seen_profiles.add(profile_url)
            entries.append(
                DirectoryEntry(
                    sort_index=len(entries),
                    directory_page=page_number,
                    company_name=company_name,
                    profile_url=profile_url,
                )
            )
            added += 1

        print(
            f"Collected directory page {page_number} "
            f"(query-probe mode) ({len(page_entries)} matches, {len(entries)} total, {added} new)."
        )

        if not page_entries or added == 0:
            consecutive_stalls += 1
        else:
            consecutive_stalls = 0

        if consecutive_stalls >= 2:
            break

    return entries


def collect_directory_entries_with_explicit_pages(
    page_urls: list[tuple[int, str]],
    strategy: ListingStrategy,
    directory_url: str,
    page_loader: callable[[str], tuple[str, str, ParsedPage]] | None = None,
) -> list[DirectoryEntry]:
    if page_loader is None:
        page_loader = load_static_page

    entries: list[DirectoryEntry] = []
    seen_profiles: set[str] = set()

    for position, (page_number, page_url) in enumerate(page_urls, start=1):
        _, _, page = page_loader(page_url)
        page_entries = extract_directory_entries(page, strategy, directory_url)

        added = 0
        for company_name, profile_url in page_entries:
            if profile_url in seen_profiles:
                continue
            seen_profiles.add(profile_url)
            entries.append(
                DirectoryEntry(
                    sort_index=len(entries),
                    directory_page=page_number,
                    company_name=company_name,
                    profile_url=profile_url,
                )
            )
            added += 1

        print(
            f"Collected directory page {page_number} "
            f"(position {position}/{len(page_urls)}) "
            f"({len(page_entries)} matches, {len(entries)} total, {added} new)."
        )

    return entries


def collect_directory_entries_with_bfs(
    seed_url: str,
    seed_page: ParsedPage,
    strategy: ListingStrategy,
    directory_url: str,
    max_pages: int,
    page_loader: callable[[str], tuple[str, str, ParsedPage]] | None = None,
) -> list[DirectoryEntry]:
    if page_loader is None:
        page_loader = load_static_page

    entries: list[DirectoryEntry] = []
    seen_profiles: set[str] = set()
    seen_pages: set[str] = set()
    queued_pages: set[str] = {seed_url}
    queue: deque[tuple[str, ParsedPage | None]] = deque([(seed_url, seed_page)])

    while queue and len(seen_pages) < max_pages:
        page_url, cached_page = queue.popleft()
        queued_pages.discard(page_url)
        if page_url in seen_pages:
            continue

        if cached_page is None:
            _, _, page = page_loader(page_url)
        else:
            page = cached_page

        seen_pages.add(page_url)
        page_number = extract_page_number_from_url(page_url) or len(seen_pages)
        page_entries = extract_directory_entries(page, strategy, directory_url)

        added = 0
        for company_name, profile_url in page_entries:
            if profile_url in seen_profiles:
                continue
            seen_profiles.add(profile_url)
            entries.append(
                DirectoryEntry(
                    sort_index=len(entries),
                    directory_page=page_number,
                    company_name=company_name,
                    profile_url=profile_url,
                )
            )
            added += 1

        print(
            f"Collected directory page {len(seen_pages)} "
            f"({len(page_entries)} matches, {len(entries)} total, {added} new)."
        )

        for next_page_url in discover_pagination_links(page, seed_url, page_url):
            if next_page_url in seen_pages or next_page_url in queued_pages:
                continue
            queue.append((next_page_url, None))
            queued_pages.add(next_page_url)

    return entries


def collect_entries_from_seed(
    seed_url: str,
    seed_html: str,
    seed_page: ParsedPage,
    sample_size: int,
    start_page: int | None,
    end_page: int | None,
    max_pages: int,
    page_loader: callable[[str], tuple[str, str, ParsedPage]] | None = None,
    profile_website_scraper: callable[[str], str] | None = None,
) -> tuple[list[DirectoryEntry], str]:
    if page_loader is None:
        page_loader = load_static_page
    if profile_website_scraper is None:
        profile_website_scraper = scrape_profile_website

    adapter_title = ""
    if is_mapyourshow_directory(seed_url, seed_html):
        print("Detected Map Your Show directory. Using platform adapter.")
        entries = collect_directory_entries_mapyourshow(
            seed_url=seed_url,
            start_page=start_page,
            end_page=end_page,
        )
        return entries, adapter_title

    if is_expofp_directory(seed_url, seed_html):
        print("Detected ExpoFP directory. Using platform adapter.")
        return collect_directory_entries_expofp(
            seed_url=seed_url,
            seed_html=seed_html,
        )

    landing_entries = collect_direct_landing_entries(
        seed_url=seed_url,
        seed_html=seed_html,
    )
    if landing_entries is not None:
        return landing_entries

    try:
        strategy, _seed_entries = choose_listing_strategy(
            seed_page=seed_page,
            directory_url=seed_url,
            sample_size=sample_size,
            profile_website_scraper=profile_website_scraper,
        )
    except RuntimeError:
        embedded_entries = collect_directory_entries_from_embedded_data(
            seed_url=seed_url,
            seed_html=seed_html,
        )
        if embedded_entries is None:
            raise

        entries, adapter_title = embedded_entries
        print(f"Recovered {len(entries)} entries from embedded application data.")
        return entries, adapter_title

    pagination_plan = discover_explicit_page_urls(
        seed_url=seed_url,
        seed_page=seed_page,
        seed_html=seed_html,
        strategy=strategy,
        start_page=start_page,
        end_page=end_page,
        page_loader=page_loader,
    )

    if pagination_plan is not None:
        page_urls, total_pages, pagination_label = pagination_plan
        print(
            f"Using explicit pagination strategy `{pagination_label}` "
            f"across {len(page_urls)} page(s) out of {total_pages} total."
        )
        entries = collect_directory_entries_with_explicit_pages(
            page_urls=page_urls,
            strategy=strategy,
            directory_url=seed_url,
            page_loader=page_loader,
        )
        return entries, adapter_title

    ajax_entries = collect_directory_entries_with_ajax_paginator(
        seed_url=seed_url,
        seed_html=seed_html,
        seed_page=seed_page,
        strategy=strategy,
        directory_url=seed_url,
        start_page=start_page,
        end_page=end_page,
        max_pages=max_pages,
    )
    if ajax_entries is not None:
        return ajax_entries, adapter_title

    query_param = discover_query_page_param(seed_url, seed_page)
    if query_param:
        print(
            f"Falling back to sequential query-page probing via `{query_param}`. "
            f"Will probe up to {end_page or max_pages} page(s)."
        )
        entries = collect_directory_entries_with_query_probing(
            seed_url=seed_url,
            seed_page=seed_page,
            strategy=strategy,
            directory_url=seed_url,
            param_name=query_param,
            start_page=start_page,
            end_page=end_page,
            max_pages=max_pages,
            page_loader=page_loader,
        )
        return entries, adapter_title

    print(
        "Falling back to pagination-link discovery. "
        f"Will crawl up to {max_pages} directory page(s)."
    )
    if start_page or end_page:
        print(
            "--start-page/--end-page were ignored because explicit numbered "
            "pagination could not be inferred.",
            file=sys.stderr,
        )

    entries = collect_directory_entries_with_bfs(
        seed_url=seed_url,
        seed_page=seed_page,
        strategy=strategy,
        directory_url=seed_url,
        max_pages=max_pages,
        page_loader=page_loader,
    )
    return entries, adapter_title


def scrape_profile_website(profile_url: str) -> str:
    profile_html = fetch_html(profile_url)
    embedded_url = extract_embedded_js_url(profile_html, MYS_WEBSITE_FIELDS)
    if embedded_url:
        return embedded_url
    profile_page = parse_page(profile_url, profile_html)
    return extract_company_website(profile_page, profile_url)


def scrape_profile_website_with_browser(
    profile_url: str,
    browser_renderer: BrowserRenderer,
) -> str:
    profile_html = fetch_html(profile_url)
    embedded_url = extract_embedded_js_url(profile_html, MYS_WEBSITE_FIELDS)
    if embedded_url:
        return embedded_url

    profile_page = parse_page(profile_url, profile_html)
    website_url = extract_company_website(profile_page, profile_url)
    if website_url:
        return website_url

    rendered_profile_url, rendered_html = browser_renderer.render(profile_url)
    embedded_url = extract_embedded_js_url(rendered_html, MYS_WEBSITE_FIELDS)
    if embedded_url:
        return embedded_url

    rendered_page = parse_page(rendered_profile_url, rendered_html)
    return extract_company_website(rendered_page, rendered_profile_url)


def collect_company_records(
    entries: list[DirectoryEntry],
    workers: int,
    browser_renderer: BrowserRenderer | None = None,
) -> tuple[list[CompanyRecord], int]:
    records: list[CompanyRecord] = []
    failures = 0
    pending_entries: list[DirectoryEntry] = []

    for entry in entries:
        parsed_profile = urlparse(entry.profile_url) if entry.profile_url else None
        has_fragment_only_reference = bool(parsed_profile and parsed_profile.fragment)

        if entry.website_url_hint or not entry.profile_url or has_fragment_only_reference:
            final_company_name = maybe_enrich_company_name(
                entry.company_name,
                entry.website_url_hint,
            )
            records.append(
                CompanyRecord(
                    sort_index=entry.sort_index,
                    directory_page=entry.directory_page,
                    company_name=final_company_name,
                    profile_url=entry.profile_url,
                    website_url=entry.website_url_hint,
                )
            )
        else:
            pending_entries.append(entry)

    completed_count = len(records)
    if completed_count and (completed_count == len(entries) or completed_count % 25 == 0):
        print(f"Scraped {completed_count}/{len(entries)} company profiles...")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_entry = {
            executor.submit(scrape_profile_website, entry.profile_url): entry
            for entry in pending_entries
        }

        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            try:
                website_url = future.result()
            except Exception as exc:  # noqa: BLE001
                failures += 1
                website_url = ""
                print(
                    f"Profile scrape failed for {entry.profile_url}: {exc}",
                    file=sys.stderr,
                )

            records.append(
                CompanyRecord(
                    sort_index=entry.sort_index,
                    directory_page=entry.directory_page,
                    company_name=entry.company_name,
                    profile_url=entry.profile_url,
                    website_url=website_url,
                )
            )

            completed_count += 1
            if completed_count == len(entries) or completed_count % 25 == 0:
                print(f"Scraped {completed_count}/{len(entries)} company profiles...")

    if browser_renderer is not None:
        pending_browser_indices = [
            index
            for index, record in enumerate(records)
            if not record.website_url and record.profile_url
        ]
        if pending_browser_indices:
            print(
                f"Attempting browser fallback for {len(pending_browser_indices)} blank profile URL(s)."
            )
        for position, index in enumerate(pending_browser_indices, start=1):
            record = records[index]
            try:
                browser_url = scrape_profile_website_with_browser(
                    record.profile_url,
                    browser_renderer=browser_renderer,
                )
            except Exception:  # noqa: BLE001
                browser_url = ""

            if browser_url:
                records[index] = CompanyRecord(
                    sort_index=record.sort_index,
                    directory_page=record.directory_page,
                    company_name=record.company_name,
                    profile_url=record.profile_url,
                    website_url=browser_url,
                )

            if position == len(pending_browser_indices) or position % 10 == 0:
                print(
                    "Browser-resolved "
                    f"{position}/{len(pending_browser_indices)} blank profile URL(s)..."
                )

    records.sort(key=lambda record: record.sort_index)
    return records, failures


def write_csv(output_path: Path, records: list[CompanyRecord]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "company_name",
                "website_url",
                "profile_url",
                "directory_page",
            ],
        )
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "company_name": record.company_name,
                    "website_url": record.website_url,
                    "profile_url": record.profile_url,
                    "directory_page": record.directory_page,
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Infer a conference directory structure from a seed URL and export "
            "company names plus website URLs to CSV."
        )
    )
    parser.add_argument(
        "directory_url",
        nargs="?",
        default=DEFAULT_DIRECTORY_URL,
        help=f"Directory seed URL. Default: {DEFAULT_DIRECTORY_URL}",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "CSV path to write. If omitted, the filename is auto-generated from "
            "the conference/site name, like `high_point_market.csv`."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Concurrent profile fetches. Default: {DEFAULT_WORKERS}",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=(
            "Maximum directory pages to crawl when pagination must be discovered "
            f"heuristically. Default: {DEFAULT_MAX_PAGES}"
        ),
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=(
            "How many sample profiles to use when validating the inferred listing "
            f"strategy. Default: {DEFAULT_SAMPLE_SIZE}"
        ),
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=None,
        help="Optional first page when explicit numbered pagination is detected.",
    )
    parser.add_argument(
        "--end-page",
        type=int,
        default=None,
        help="Optional last page when explicit numbered pagination is detected.",
    )
    parser.add_argument(
        "--browser-mode",
        choices=("off", "auto", "prefer"),
        default=DEFAULT_BROWSER_MODE,
        help=(
            "Browser fallback mode. `off` disables Playwright, `auto` only uses it "
            "when HTML scraping fails, and `prefer` renders through the browser "
            f"first. Default: {DEFAULT_BROWSER_MODE}."
        ),
    )
    parser.add_argument(
        "--browser-timeout-ms",
        type=int,
        default=DEFAULT_BROWSER_TIMEOUT_MS,
        help=(
            "Navigation timeout for Playwright browser fallback, in milliseconds. "
            f"Default: {DEFAULT_BROWSER_TIMEOUT_MS}."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.workers < 1:
        print("--workers must be 1 or greater.", file=sys.stderr)
        return 1
    if args.max_pages < 1:
        print("--max-pages must be 1 or greater.", file=sys.stderr)
        return 1
    if args.sample_size < 1:
        print("--sample-size must be 1 or greater.", file=sys.stderr)
        return 1
    if args.browser_timeout_ms < 1:
        print("--browser-timeout-ms must be 1 or greater.", file=sys.stderr)
        return 1

    seed_url = normalize_http_url(args.directory_url)
    if not seed_url:
        print("Please provide a valid http(s) directory URL.", file=sys.stderr)
        return 1

    browser_options = BrowserFallbackOptions(
        mode=args.browser_mode,
        timeout_ms=args.browser_timeout_ms,
    )
    browser_renderer: BrowserRenderer | None = None
    browser_install_hint = (
        "Install Playwright with `python3 -m pip install playwright` and "
        "`python3 -m playwright install chromium` to enable browser fallback."
    )
    if browser_options.enabled:
        if BrowserRenderer.is_available():
            browser_renderer = BrowserRenderer(timeout_ms=browser_options.timeout_ms)
        elif browser_options.prefer_browser:
            print(
                "Scrape failed: browser mode `prefer` requires Playwright. "
                f"{browser_install_hint}",
                file=sys.stderr,
            )
            return 1

    try:
        static_loader = load_static_page
        browser_loader = (
            (lambda url: load_browser_page(browser_renderer, url))
            if browser_renderer is not None
            else None
        )

        used_browser_fallback = False
        adapter_title = ""

        if browser_options.prefer_browser and browser_loader is not None:
            print("Browser mode `prefer` enabled. Rendering the seed page in a browser first.")
            seed_url, seed_html, seed_page = resolve_seed_page(seed_url, browser_loader)
            entries, adapter_title = collect_entries_from_seed(
                seed_url=seed_url,
                seed_html=seed_html,
                seed_page=seed_page,
                sample_size=args.sample_size,
                start_page=args.start_page,
                end_page=args.end_page,
                max_pages=args.max_pages,
                page_loader=browser_loader,
                profile_website_scraper=(
                    lambda profile_url: scrape_profile_website_with_browser(
                        profile_url,
                        browser_renderer=browser_renderer,
                    )
                ),
            )
            used_browser_fallback = True
        else:
            try:
                seed_url, seed_html, seed_page = resolve_seed_page(seed_url, static_loader)
                entries, adapter_title = collect_entries_from_seed(
                    seed_url=seed_url,
                    seed_html=seed_html,
                    seed_page=seed_page,
                    sample_size=args.sample_size,
                    start_page=args.start_page,
                    end_page=args.end_page,
                    max_pages=args.max_pages,
                )
            except Exception as static_exc:
                if browser_loader is None:
                    if browser_options.enabled:
                        raise RuntimeError(
                            f"{static_exc} Browser fallback is unavailable. {browser_install_hint}"
                        ) from static_exc
                    raise

                print(
                    "Static HTML discovery stalled; retrying with browser-rendered fallback."
                )
                seed_url, seed_html, seed_page = resolve_seed_page(seed_url, browser_loader)
                entries, adapter_title = collect_entries_from_seed(
                    seed_url=seed_url,
                    seed_html=seed_html,
                    seed_page=seed_page,
                    sample_size=args.sample_size,
                    start_page=args.start_page,
                    end_page=args.end_page,
                    max_pages=args.max_pages,
                    page_loader=browser_loader,
                    profile_website_scraper=(
                        lambda profile_url: scrape_profile_website_with_browser(
                            profile_url,
                            browser_renderer=browser_renderer,
                        )
                    ),
                )
                used_browser_fallback = True

        if not entries:
            raise RuntimeError(
                "No company profile links were collected. "
                "The directory may require JavaScript rendering."
            )

        if adapter_title:
            seed_page = retitle_page(seed_page, adapter_title)

        records, failures = collect_company_records(
            entries,
            args.workers,
            browser_renderer=browser_renderer if used_browser_fallback else None,
        )
        output_path = resolve_output_path(args.output, seed_url, seed_page)
        write_csv(output_path, records)
    except Exception as exc:  # noqa: BLE001
        print(f"Scrape failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if browser_renderer is not None:
            browser_renderer.close()

    print(f"Wrote {len(records)} companies to {output_path}.")
    if failures:
        print(
            f"Completed with {failures} profile failures. "
            "Those rows were written with blank website URLs.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
