#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, json, os, pathlib, re, shutil, time
from urllib.parse import urljoin

from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

BRANCH_MAP_JSON = "filtered_branches_name_to_url.json"  # [file:1]

OUT_DIR = os.path.join("data", "out")
OUT_JSONL = os.path.join(OUT_DIR, "companies.jsonl")
DEBUG_DIR = "debug"

HEADLESS = True
ONLY_FIRST_N_BRANCHES = None

# short waits (you can keep them small, but don't make goto 1s)
GOTO_TIMEOUT_MS = 15_000
WAIT_DOM_MS = 2_000
WAIT_CARD_MS = 2_000

MAX_PAGES_PER_BRANCH = 4000
MAX_SECONDS_PER_BRANCH = 25 * 60

CARD_SELECTOR = "article.search-result-article"
DETAIL_LINK_SELECTOR = "a.title-link[href]"
PHONE_SELECTOR = 'a[itemprop="telephone"]'
EMAIL_SELECTOR = 'a[itemprop="email"]'
WEB_SELECTOR = 'a[itemprop="url"]'
STREET_SELECTOR = ".address .street"
PLACE_SELECTOR = ".address .place"

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    pathlib.Path(DEBUG_DIR).mkdir(exist_ok=True)

def append_jsonl(records):
    if not records:
        return
    with open(OUT_JSONL, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()

def snapshot_output(label: str):
    if not os.path.exists(OUT_JSONL):
        return
    ts = int(time.time())
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", label)[:80]
    dst = os.path.join(DEBUG_DIR, f"{ts}_{safe}.jsonl")
    shutil.copyfile(OUT_JSONL, dst)
    print(f"[debug] wrote {dst}", flush=True)

def clean_text(s):
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

async def dump_debug(page, label: str):
    ts = int(time.time())
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", label)[:80]
    html_path = os.path.join(DEBUG_DIR, f"{ts}_{safe}.html")
    png_path = os.path.join(DEBUG_DIR, f"{ts}_{safe}.png")

    html = await page.content()
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    try:
        await page.screenshot(path=png_path, full_page=True)
        print(f"[debug] wrote {html_path} and {png_path} (url={page.url})", flush=True)
    except Exception:
        print(f"[debug] wrote {html_path} (url={page.url})", flush=True)

    snapshot_output(label=safe)

async def extract_cards(page, branche: str, base_url: str):
    cards = page.locator(CARD_SELECTOR)
    n = await cards.count()
    out = []

    for i in range(n):
        card = cards.nth(i)

        name = None
        if await card.locator(f"{DETAIL_LINK_SELECTOR} h3").count():
            name = clean_text(await card.locator(f"{DETAIL_LINK_SELECTOR} h3").first.inner_text())

        href = await card.locator(DETAIL_LINK_SELECTOR).first.get_attribute("href") if await card.locator(DETAIL_LINK_SELECTOR).count() else None
        wko_detail_url = urljoin(base_url, href) if href else None

        phone = clean_text(await card.locator(PHONE_SELECTOR).first.inner_text()) if await card.locator(PHONE_SELECTOR).count() else None

        email = await card.locator(EMAIL_SELECTOR).first.get_attribute("href") if await card.locator(EMAIL_SELECTOR).count() else None
        if email and email.lower().startswith("mailto:"):
            email = email[7:]

        company_website = None
        if await card.locator(WEB_SELECTOR).count():
            company_website = clean_text(await card.locator(f"{WEB_SELECTOR} span").first.inner_text())

        street = clean_text(await card.locator(STREET_SELECTOR).first.inner_text()) if await card.locator(STREET_SELECTOR).count() else None
        zip_city = clean_text(await card.locator(PLACE_SELECTOR).first.inner_text()) if await card.locator(PLACE_SELECTOR).count() else None

        out.append({
            "branche": branche,
            "name": name,
            "wko_detail_url": wko_detail_url,
            "company_website": company_website,
            "email": email,
            "phone": phone,
            "street": street,
            "zip_city": zip_city,
            "source_list_url": page.url,
        })

    return out

async def get_next_url(page, base_url: str):
    # Your paste shows <link rel="next" href="...page2"> exists. [file:229]
    href = await page.locator("link[rel~='next']").first.get_attribute("href") if await page.locator("link[rel~='next']").count() else None
    return urljoin(base_url, href) if href else None

async def crawl_branch(context, branche: str, start_url: str):
    page = await context.new_page()
    t0 = time.time()
    seen_pages = set()
    all_rows = 0

    try:
        url = start_url
        for page_no in range(1, MAX_PAGES_PER_BRANCH + 1):
            if time.time() - t0 > MAX_SECONDS_PER_BRANCH:
                print(f"[{branche}] timeout branch", flush=True)
                return all_rows

            if not url or url in seen_pages:
                return all_rows
            seen_pages.add(url)

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
            except Exception:
                await dump_debug(page, f"{branche}_goto_failed_p{page_no}")
                return all_rows

            html0 = await page.content()
            if "Access Denied" in html0:
                await dump_debug(page, f"{branche}_access_denied_p{page_no}")
                return all_rows

            # short settle
            await page.wait_for_timeout(WAIT_DOM_MS)

            try:
                await page.wait_for_selector(CARD_SELECTOR, state="attached", timeout=WAIT_CARD_MS)
            except PWTimeoutError:
                await dump_debug(page, f"{branche}_no_cards_p{page_no}")
                return all_rows

            rows = await extract_cards(page, branche, base_url=url)
            append_jsonl(rows)  # continuous write
            all_rows += len(rows)

            nxt = await get_next_url(page, base_url=url)
            if not nxt:
                return all_rows
            url = nxt

    finally:
        await page.close()

async def main():
    ensure_dirs()

    with open(BRANCH_MAP_JSON, "r", encoding="utf-8") as f:
        branch_map = json.load(f)  # [file:1]

    items = list(branch_map.items())
    if ONLY_FIRST_N_BRANCHES is not None:
        items = items[:ONLY_FIRST_N_BRANCHES]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            locale="de-AT",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        await context.set_extra_http_headers({
            "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://firmen.wko.at/",
        })

        try:
            for idx, (branche, url) in tqdm(enumerate(items, start=1), total=len(items)):
                print(f"[{idx}/{len(items)}] {branche} -> {url}", flush=True)
                try:
                    wrote = await crawl_branch(context, branche, url)
                    print(f"[{idx}/{len(items)}] {branche}: wrote {wrote}", flush=True)
                except Exception as e:
                    print(f"[{idx}/{len(items)}] ERROR {branche}: {e!r}", flush=True)
                    # snapshot whatever we have
                    snapshot_output(f"{branche}_branch_error")
                    continue
        finally:
            await context.close()
            await browser.close()

    print("Done:", OUT_JSONL, flush=True)

if __name__ == "__main__":
    asyncio.run(main())
