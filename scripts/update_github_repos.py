#!/usr/bin/env python3
"""Update data/github-repos.json with deep GitHub scans + curated Awesome Starkzap inclusions."""

from __future__ import annotations

import datetime
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

PER_PAGE = 100
MAX_RESULTS = 1000
GITHUB_API_BASE = "https://api.github.com"
STARKZAP_REPO = "keep-starknet-strange/starkzap"
AWESOME_REPO = "keep-starknet-strange/awesome-starkzap"
AWESOME_SOURCE = "awesome_starkzap_curated"
AWESOME_BUILDERS_SOURCE = "awesome_starkzap_contributors"
STARKZAP_SOURCE = "starkzap_repo_curated"
STARKZAP_BUILDERS_SOURCE = "starkzap_repo_profiles"
CURATED_PROJECT_SOURCES = {AWESOME_SOURCE, STARKZAP_SOURCE}
MENTIONS_START = "2026-02-01"
DATA_FILE = Path("data/github-repos.json")
DISALLOWED_MENTION_QUERY = "starkzapp"

EXCLUDE_REPOS = {
    STARKZAP_REPO,
    "welttowelt/starkzap-stats",
    "starkience/starkzap-stats",
}
EXCLUDE_REPOS_LOWER = {repo.lower() for repo in EXCLUDE_REPOS}
BUILDER_LOGIN_REPLACEMENTS = {
    "abdelhamidbakhta": "welttowelt",
}

QUERY_SETS: List[Tuple[str, str]] = [
    ("repo_wide", "starkzap"),
    ("package_json", "starkzap path:package.json"),
    ("package_lock", "starkzap path:package-lock.json"),
    ("pnpm_lock", "starkzap path:pnpm-lock.yaml"),
    ("yarn_lock", "starkzap path:yarn.lock"),
    ("readme", "starkzap filename:README.md"),
    ("docs_path", "starkzap path:docs"),
    ("ts_code", "starkzap language:TypeScript"),
    ("js_code", "starkzap language:JavaScript"),
    ("python_code", "starkzap language:Python"),
    ("rust_code", "starkzap language:Rust"),
    ("go_code", "starkzap language:Go"),
    ("solidity_code", "starkzap language:Solidity"),
    ("json_files", "starkzap language:JSON"),
    ("yaml_files", "starkzap language:YAML"),
    ("markdown_files", "starkzap language:Markdown"),
]


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def utc_today_iso() -> str:
    return utc_now().strftime("%Y-%m-%d")


def is_iso_date(value: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value or ""))


def compact_text(text: str, limit: int = 120) -> str:
    compact = " ".join((text or "").split()).strip()
    if not compact:
        return ""
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit - 3].rstrip()}..."


def summary_from_readme(readme_text: str) -> str:
    for raw_line in (readme_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("#", "!", "[!", "<img", "<svg", "http://", "https://")):
            continue
        if re.match(r"^[\W_]+$", line):
            continue
        if len(line) < 20:
            continue
        return compact_text(line, 110)
    return ""


def build_summary(description: str, readme_text: str = "") -> str:
    from_description = compact_text(description, 110)
    if from_description:
        return from_description
    from_readme = summary_from_readme(readme_text)
    if from_readme:
        return from_readme
    return "Starkzap mention found in repository files."


class GithubClient:
    def __init__(self, token: str):
        self.headers_base = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def request(self, url: str, accept: Optional[str] = None, retries: int = 4) -> bytes:
        for attempt in range(retries):
            headers = dict(self.headers_base)
            if accept:
                headers["Accept"] = accept

            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=45) as resp:
                    return resp.read()
            except urllib.error.HTTPError as exc:
                retryable = exc.code in {403, 429, 502, 503, 504}
                if (not retryable) or attempt == retries - 1:
                    raise

                sleep_seconds = 1.5 * (2 ** attempt)
                reset = exc.headers.get("X-RateLimit-Reset")
                if reset:
                    try:
                        wait = max(1, int(reset) - int(time.time()) + 1)
                        sleep_seconds = max(sleep_seconds, min(wait, 60))
                    except ValueError:
                        pass
                time.sleep(sleep_seconds)
            except (urllib.error.URLError, TimeoutError):
                if attempt == retries - 1:
                    raise
                time.sleep(1.2 * (2 ** attempt))

    def request_json(self, url: str) -> dict:
        return json.loads(self.request(url).decode("utf-8"))

    def request_text(self, url: str) -> str:
        return self.request(url, accept="application/vnd.github.raw+json").decode("utf-8", errors="ignore")


def load_existing() -> dict:
    if not DATA_FILE.exists():
        return {}
    try:
        with DATA_FILE.open() as f:
            return json.load(f)
    except Exception:
        return {}


def extract_repo_full_names(markdown: str) -> Set[str]:
    # Prefer explicit [Repo](...) links from the Awesome table.
    repo_label_urls = re.findall(r"\[Repo\]\((https://github\.com/[^\)]+)\)", markdown, flags=re.IGNORECASE)
    if repo_label_urls:
        urls = set(repo_label_urls)
    else:
        # Fallback to any GitHub repo URLs when no explicit Repo links are found.
        urls = set(re.findall(r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[^\s)]*)?", markdown))
    full_names: Set[str] = set()

    for raw_url in urls:
        raw_url = raw_url.rstrip("\"'.,")
        parsed = urllib.parse.urlparse(raw_url)
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) < 2:
            continue
        owner, repo = parts[0], parts[1]
        if owner.lower() == "user-attachments":
            continue
        if owner.lower() in {"your-username", "example-user"}:
            continue
        if repo.lower() in {"your-repo", "your-repo-name"}:
            continue
        full_names.add(f"{owner}/{repo}")

    return full_names


def extract_profile_logins(markdown: str) -> Set[str]:
    # Profile URLs with exactly one path segment.
    logins = set(re.findall(r"https://github\.com/([A-Za-z0-9-]+)(?=[\"')\s]|$)", markdown))
    # Contributors table often includes commit links with ?author=<login>.
    logins.update(re.findall(r"[?&]author=([A-Za-z0-9-]+)", markdown))
    return logins


def main() -> None:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Missing GH_TOKEN or GITHUB_TOKEN")

    client = GithubClient(token)
    now = utc_now()
    today_iso = utc_today_iso()

    previous = load_existing()
    previous_repo_map = {
        (item.get("full_name") or "").lower(): item
        for item in previous.get("repos", [])
        if item.get("full_name")
    }
    previous_builder_map = {
        (item.get("login") or "").lower(): item
        for item in previous.get("builders", [])
        if item.get("login")
    }

    disallowed_repos: Set[str] = set()

    def collect_repo_names_for_query(query: str) -> Set[str]:
        found: Set[str] = set()
        page = 1
        while True:
            qs = urllib.parse.urlencode({"q": query, "per_page": PER_PAGE, "page": page})
            url = f"{GITHUB_API_BASE}/search/code?{qs}"
            data = client.request_json(url)
            items = data.get("items", [])
            for item in items:
                repo = item.get("repository") or {}
                full_name = (repo.get("full_name") or "").strip()
                if full_name:
                    found.add(full_name.lower())
            if len(items) < PER_PAGE:
                break
            if page * PER_PAGE >= MAX_RESULTS:
                break
            page += 1
            time.sleep(0.25)
        return found

    try:
        disallowed_repos = collect_repo_names_for_query(DISALLOWED_MENTION_QUERY)
        disallowed_repos -= EXCLUDE_REPOS_LOWER
        print(f"excluded typo repos ({DISALLOWED_MENTION_QUERY}): {len(disallowed_repos)}")
    except Exception as exc:
        print(f"Typo exclusion query failed ({DISALLOWED_MENTION_QUERY}): {exc}")
        disallowed_repos = set()

    repos: Dict[str, dict] = {}
    builders: Dict[str, dict] = {}

    def initial_first_seen(existing: dict) -> str:
        if is_iso_date(existing.get("first_seen_at", "")):
            return existing["first_seen_at"]
        if is_iso_date(existing.get("created_at", "")):
            return existing["created_at"]
        return today_iso

    def upsert_repo_by_name(full_name: str, source: str) -> Optional[dict]:
        if not full_name:
            return None

        key = full_name.lower()
        if key in EXCLUDE_REPOS_LOWER or key in disallowed_repos:
            return None
        existing = previous_repo_map.get(key, {})
        if key not in repos:
            owner = full_name.split("/")[0] if "/" in full_name else ""
            repos[key] = {
                "full_name": existing.get("full_name") or full_name,
                "url": existing.get("url") or f"https://github.com/{full_name}",
                "description": existing.get("description") or "",
                "summary": existing.get("summary") or "",
                "stars": int(existing.get("stars") or 0),
                "created_at": existing.get("created_at") or "",
                "first_seen_at": initial_first_seen(existing),
                "owner_avatar_url": existing.get("owner_avatar_url") or (f"https://github.com/{owner}.png?size=96" if owner else ""),
                "match_sources": list(existing.get("match_sources") or []),
            }

        entry = repos[key]
        if source not in entry["match_sources"]:
            entry["match_sources"].append(source)

        # Curated project sources should be represented in post-Feb 2026 stats.
        if source in CURATED_PROJECT_SOURCES and entry["first_seen_at"] < MENTIONS_START:
            entry["first_seen_at"] = today_iso
        return entry

    def upsert_repo_from_search(repo: dict, source: str) -> None:
        full_name = repo.get("full_name")
        if not full_name:
            return
        entry = upsert_repo_by_name(full_name, source)
        if not entry:
            return

        owner = repo.get("owner") or {}
        entry["full_name"] = full_name
        entry["url"] = repo.get("html_url") or entry["url"]
        entry["description"] = repo.get("description") or entry["description"] or ""
        entry["stars"] = repo.get("stargazers_count", entry["stars"])
        if owner.get("avatar_url"):
            entry["owner_avatar_url"] = owner["avatar_url"]

    def upsert_builder(login: str, source: str, avatar_url: str = "") -> None:
        login = (login or "").strip()
        if not login:
            return
        raw_key = login.lower()
        canonical_login = BUILDER_LOGIN_REPLACEMENTS.get(raw_key, login)
        key = canonical_login.lower()
        if key != raw_key:
            avatar_url = ""
        existing = previous_builder_map.get(key, {})
        if key not in builders:
            builders[key] = {
                "login": existing.get("login") or canonical_login,
                "url": existing.get("url") or f"https://github.com/{canonical_login}",
                "avatar_url": existing.get("avatar_url") or avatar_url or f"https://github.com/{canonical_login}.png?size=96",
                "sources": list(existing.get("sources") or []),
            }
        entry = builders[key]
        if avatar_url:
            entry["avatar_url"] = avatar_url
        if source not in entry["sources"]:
            entry["sources"].append(source)

    def run_query(query: str, source: str) -> None:
        page = 1
        while True:
            qs = urllib.parse.urlencode({"q": query, "per_page": PER_PAGE, "page": page})
            url = f"{GITHUB_API_BASE}/search/code?{qs}"
            data = client.request_json(url)
            items = data.get("items", [])

            for item in items:
                repo = item.get("repository") or {}
                upsert_repo_from_search(repo, source)

            if len(items) < PER_PAGE:
                break
            if page * PER_PAGE >= MAX_RESULTS:
                break
            page += 1
            time.sleep(0.25)

    # Deep code search pass.
    for source, query in QUERY_SETS:
        try:
            run_query(query, source)
            print(f"ok: {source}")
        except urllib.error.HTTPError as exc:
            print(f"Query failed ({source}): {exc}")
            continue

    # Curated pass: include projects and people listed in awesome-starkzap.
    try:
        awesome_readme = client.request_text(f"{GITHUB_API_BASE}/repos/{AWESOME_REPO}/readme")
        curated_repos = extract_repo_full_names(awesome_readme)
        curated_repos.discard(AWESOME_REPO)

        for full_name in curated_repos:
            upsert_repo_by_name(full_name, AWESOME_SOURCE)

        curated_profiles = extract_profile_logins(awesome_readme)
        for login in curated_profiles:
            upsert_builder(login, AWESOME_BUILDERS_SOURCE)

        print(f"ok: {AWESOME_SOURCE} ({len(curated_repos)} repos)")
        print(f"ok: {AWESOME_BUILDERS_SOURCE} ({len(curated_profiles)} builders)")
    except Exception as exc:
        print(f"Curated source failed ({AWESOME_REPO}): {exc}")

    # Curated pass: include projects and people linked from the main Starkzap repo.
    try:
        starkzap_readme = client.request_text(f"{GITHUB_API_BASE}/repos/{STARKZAP_REPO}/readme")
        starkzap_repos = extract_repo_full_names(starkzap_readme)
        starkzap_repos.discard(STARKZAP_REPO)

        for full_name in starkzap_repos:
            upsert_repo_by_name(full_name, STARKZAP_SOURCE)

        starkzap_profiles = extract_profile_logins(starkzap_readme)
        for login in starkzap_profiles:
            upsert_builder(login, STARKZAP_BUILDERS_SOURCE)

        print(f"ok: {STARKZAP_SOURCE} ({len(starkzap_repos)} repos)")
        print(f"ok: {STARKZAP_BUILDERS_SOURCE} ({len(starkzap_profiles)} builders)")
    except Exception as exc:
        print(f"Curated source failed ({STARKZAP_REPO}): {exc}")

    # Enrich repo details + summaries.
    resolved_repos: List[dict] = []
    for key, info in repos.items():
        full_name = info["full_name"]
        try:
            repo_data = client.request_json(f"{GITHUB_API_BASE}/repos/{full_name}")
            canonical_name = repo_data.get("full_name") or full_name
            owner = repo_data.get("owner") or {}
            created_at = (repo_data.get("created_at") or "")[:10]

            info["full_name"] = canonical_name
            info["url"] = repo_data.get("html_url") or info["url"]
            info["created_at"] = created_at
            info["stars"] = repo_data.get("stargazers_count", info.get("stars", 0))
            info["description"] = repo_data.get("description") or info.get("description") or ""
            info["owner_avatar_url"] = owner.get("avatar_url") or info.get("owner_avatar_url") or ""

            if not is_iso_date(info.get("first_seen_at", "")):
                info["first_seen_at"] = created_at if is_iso_date(created_at) else today_iso

            readme_text = ""
            if not info["description"]:
                try:
                    readme_text = client.request_text(f"{GITHUB_API_BASE}/repos/{canonical_name}/readme")
                except Exception:
                    readme_text = ""
            info["summary"] = build_summary(info["description"], readme_text)

            owner_login = owner.get("login")
            if owner_login:
                upsert_builder(owner_login, "repo_owner", avatar_url=info["owner_avatar_url"])

            resolved_repos.append(info)
        except Exception as exc:
            # Keep previously known entries if they already look valid.
            if is_iso_date(info.get("created_at", "")):
                if not is_iso_date(info.get("first_seen_at", "")):
                    info["first_seen_at"] = info["created_at"]
                info["summary"] = build_summary(info.get("description", ""), "")
                resolved_repos.append(info)
            elif set(info.get("match_sources") or []).intersection(CURATED_PROJECT_SOURCES):
                if not is_iso_date(info.get("first_seen_at", "")):
                    info["first_seen_at"] = today_iso
                info["summary"] = info.get("summary") or "Listed in curated Starkzap project sources."
                resolved_repos.append(info)
                print(f"Keeping curated repo with unresolved API record {full_name}: {exc}")
            else:
                print(f"Skipping unresolved repo {full_name}: {exc}")

    # Optionally include contributors from the main Starkzap repo.
    try:
        page = 1
        while True:
            url = f"{GITHUB_API_BASE}/repos/{STARKZAP_REPO}/contributors?per_page=100&page={page}"
            contributors = client.request_json(url)
            if not isinstance(contributors, list) or not contributors:
                break
            for item in contributors:
                login = item.get("login")
                avatar = item.get("avatar_url") or ""
                if login:
                    upsert_builder(login, "starkzap_core_contributors", avatar_url=avatar)
            if len(contributors) < 100:
                break
            page += 1
    except Exception as exc:
        print(f"Optional contributors fetch failed ({STARKZAP_REPO}): {exc}")

    for item in resolved_repos:
        item["match_sources"] = sorted(set(item.get("match_sources") or []))

    for item in builders.values():
        item["sources"] = sorted(set(item.get("sources") or []))

    repos_sorted = sorted(
        resolved_repos,
        key=lambda r: ((r.get("first_seen_at") or "9999-99-99"), (r.get("created_at") or "9999-99-99"), r["full_name"].lower()),
    )
    builders_sorted = sorted(builders.values(), key=lambda b: b["login"].lower())

    result = {
        "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": len(repos_sorted),
        "repos": repos_sorted,
        "builders": builders_sorted,
        "query_mode": "deep_repo_wide_mentions_with_curated_sources",
        "queries": {source: query for source, query in QUERY_SETS},
        "curated_sources": {
            AWESOME_SOURCE: f"https://github.com/{AWESOME_REPO}",
            AWESOME_BUILDERS_SOURCE: f"https://github.com/{AWESOME_REPO}#contributors",
            STARKZAP_SOURCE: f"https://github.com/{STARKZAP_REPO}",
            STARKZAP_BUILDERS_SOURCE: f"https://github.com/{STARKZAP_REPO}",
            "starkzap_core_contributors": f"https://github.com/{STARKZAP_REPO}",
        },
    }

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w") as f:
        json.dump(result, f, indent=2)

    print(f"Deep repo-wide starkzap mentions: {len(repos_sorted)} repos")
    print(f"Tracked builders: {len(builders_sorted)}")


if __name__ == "__main__":
    main()
