#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, json, os, pathlib, re, shutil, time
from urllib.parse import urljoin

from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

BRANCH_MAP_JSON = "filtered_branches_name_to_url.json"

OUT_DIR = os.path.join("data", "out")
OUT_JSONL = os.path.join(OUT_DIR, "companies.jsonl")
DEBUG_DIR = "debug"

HEADLESS = True
ONLY_FIRST_N_BRANCHES = None

GOTO_TIMEOUT_MS = 15_000
WAIT_DOM_MS = 1_000
WAIT_CARD_MS = 6_000

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


async def _dismiss_overlays(page):
    """
    Best-effort: close cookie/consent overlays that can block clicks.
    Cookie overlays are a common reason why locator.click() times out. [web:207]
    """
    # Try common accept/close buttons
    candidates = [
        page.get_by_role("button", name=re.compile(r"(Alle akzeptieren|Akzeptieren|Zustimmen|OK)", re.I)),
        page.get_by_role("button", name=re.compile(r"(Schließen|Schliessen|Close)", re.I)),
        page.locator("button:has-text('Alle akzeptieren')"),
        page.locator("button:has-text('Akzeptieren')"),
        page.locator("button:has-text('Zustimmen')"),
    ]
    for loc in candidates:
        try:
            if await loc.first.is_visible(timeout=250):
                await loc.first.click(timeout=1500)
        except Exception:
            pass

    # Last resort: remove obvious backdrops/overlays if they still intercept pointer events
    try:
        await page.evaluate("""
          () => {
            const sels = [
              '.modal-backdrop', '.backdrop', '.overlay',
              '#cmp', '.cmp-overlay', '.cookie', '.cookie-banner',
              '[class*="consent"]', '[id*="consent"]'
            ];
            for (const s of sels) {
              document.querySelectorAll(s).forEach(e => {
                const style = window.getComputedStyle(e);
                // remove only big fixed layers
                if (style.position === 'fixed' && (parseInt(style.zIndex || '0', 10) >= 1000)) e.remove();
              });
            }
          }
        """)
    except Exception:
        pass


async def click_load_more_until_done(page, max_clicks=200):
    """
    Clicks "Mehr laden" repeatedly until it disappears or no progress.
    Uses locator actions (auto-wait + retries). [web:71]
    """
    load_more = page.get_by_role("button", name=re.compile(r"(Mehr laden|Weitere laden|Mehr Ergebnisse)", re.I))
    if await load_more.count() == 0:
        load_more = page.locator("button:has-text('Mehr laden')")

    cards = page.locator(CARD_SELECTOR)

    no_progress_rounds = 0
    for click_i in range(1, max_clicks + 1):
        await _dismiss_overlays(page)

        # If button not visible, done
        try:
            visible = await load_more.first.is_visible(timeout=800)
        except Exception:
            visible = False
        if not visible:
            return

        before = await cards.count()

        # Ensure in view then click
        try:
            await load_more.first.scroll_into_view_if_needed(timeout=3000)
            await load_more.first.click(timeout=10_000)
        except PWTimeoutError:
            # One forced attempt if actionability is blocked
            try:
                await load_more.first.click(timeout=6000, force=True)
            except Exception:
                await dump_debug(page, f"load_more_click_failed_{click_i}")
                return

        # Wait until more cards appear or button disappears
        try:
            await page.wait_for_function(
                """(sel, before) => {
                    const n = document.querySelectorAll(sel).length;
                    const btnExists = [...document.querySelectorAll('button')]
                      .some(b => (b.innerText || '').toLowerCase().includes('mehr laden'));
                    return n > before || !btnExists;
                }""",
                arg=[CARD_SELECTOR, before],
                timeout=12_000
            )
        except PWTimeoutError:
            pass

        after = await cards.count()
        if after <= before:
            no_progress_rounds += 1
            if no_progress_rounds >= 2:
                # stop to avoid infinite loop
                return
        else:
            no_progress_rounds = 0


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
    # WKO pages expose rel="next" (seen in your paste). [file:229]
    loc = page.locator("link[rel~='next']")
    href = await loc.first.get_attribute("href") if await loc.count() else None
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

            await page.wait_for_timeout(WAIT_DOM_MS)

            try:
                await page.wait_for_selector(CARD_SELECTOR, state="attached", timeout=WAIT_CARD_MS)
            except PWTimeoutError:
                await dump_debug(page, f"{branche}_no_cards_p{page_no}")
                return all_rows

            # IMPORTANT: click "Mehr laden" on THIS page before extracting
            await click_load_more_until_done(page, max_clicks=250)

            rows = await extract_cards(page, branche, base_url=url)
            append_jsonl(rows)
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
        branch_map = json.load(f)

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
                    snapshot_output(f"{branche}_branch_error")
                    continue
        finally:
            await context.close()
            await browser.close()

    print("Done:", OUT_JSONL, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
