"""
scripts/inspect_page.py
=======================
Inspecciona páginas de portales con el mismo navegador que los scrapers y
escribe en stdout lo necesario para ajustar selectores y URLs: URL final
(redirecciones), título, cuántos elementos cumple cada selector, enlaces que
cumplen un patrón (paginación, secciones de proyectos) y el HTML de una
tarjeta de ejemplo.

Sirve para depurar desde CI cuando no se puede abrir el portal a mano.

Uso:
    python scripts/inspect_page.py --file scripts/inspect_targets.txt
    python scripts/inspect_page.py "https://www.properati.com.co/s/bogota/apartamento/venta"

Formato de --file (una página por línea, # = comentario):
    URL ;; selector1, selector2 ;; regex_de_enlaces
Las dos últimas partes son opcionales.
"""

import argparse
import os
import re
import sys
from collections import Counter
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Selectores que usan hoy los scrapers: así se ve de inmediato cuál dejó de existir.
DEFAULT_SELECTORS = [
    "article.snippet",
    "div.listingBoxCard",
    "li.ui-search-layout__item",
    "div.property-card__container",
    "article.card",
]

# Clases de contenedores que se repiten mucho en la página: candidatas a tarjeta.
_CANDIDATES_JS = """
() => {
  const counts = {};
  for (const el of document.querySelectorAll('article, li, div, a')) {
    if (!el.className || typeof el.className !== 'string') continue;
    if (!el.querySelector('a[href]') && el.tagName !== 'A') continue;
    const text = el.innerText || '';
    if (!/\\$\\s?[\\d.]{5,}|COP|m²|m2/i.test(text)) continue;
    const key = el.tagName.toLowerCase() + '.' + el.className.trim().split(/\\s+/).slice(0, 2).join('.');
    counts[key] = (counts[key] || 0) + 1;
  }
  return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 12);
}
"""


def _parse_targets(path: Path):
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(";;")]
        url = parts[0]
        selectors = [s.strip() for s in parts[1].split(",") if s.strip()] if len(parts) > 1 else []
        link_regex = parts[2] if len(parts) > 2 and parts[2] else None
        yield url, selectors, link_regex


def inspect(page, url: str, selectors, link_regex) -> None:
    print("=" * 100)
    print(f"URL pedida : {url}")
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=45_000)
    except Exception as e:  # noqa: BLE001 — se reporta y se sigue con la siguiente URL
        print(f"ERROR al cargar: {e}")
        return
    page.wait_for_timeout(4000)
    for _ in range(6):
        page.mouse.wheel(0, 900)
        page.wait_for_timeout(400)

    print(f"URL final  : {page.url}")
    print(f"HTTP       : {response.status if response else '?'}")
    print(f"Título     : {page.title()!r}")

    print("-- Selectores")
    for sel in DEFAULT_SELECTORS + selectors:
        try:
            n = len(page.query_selector_all(sel))
        except Exception as e:  # noqa: BLE001
            n = f"selector inválido ({e})"
        print(f"   {sel:45s} -> {n}")

    print("-- Contenedores repetidos con precio/área (posibles tarjetas)")
    candidates = page.evaluate(_CANDIDATES_JS)
    for key, count in candidates:
        print(f"   {count:4d}  {key}")

    if link_regex:
        pattern = re.compile(link_regex, re.I)
        hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => [e.href, (e.innerText || '').trim()])")
        seen = []
        for href, text in hrefs:
            if pattern.search(href) and href not in [h for h, _ in seen]:
                seen.append((href, text[:40].replace("\n", " ")))
        print(f"-- Enlaces que cumplen /{link_regex}/ ({len(seen)})")
        for href, text in seen[:40]:
            print(f"   {href}   [{text}]")

    # Tarjeta completa de ejemplo: primero los selectores de los scrapers, si
    # no matchea ninguno el contenedor más repetido, y por último los extra.
    sample_options = [s for s in DEFAULT_SELECTORS if page.query_selector(s)]
    sample_options += [candidates[0][0]] if candidates else []
    sample_options += [s for s in selectors if page.query_selector(s)]
    sample_sel = sample_options[0] if sample_options else None
    if sample_sel:
        el = page.query_selector(sample_sel)
        if el:
            html = re.sub(r"\s+", " ", el.evaluate("e => e.outerHTML"))
            print(f"-- HTML de ejemplo ({sample_sel}), 2500 caracteres")
            print("   " + html[:2500])


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspecciona páginas de portales.")
    parser.add_argument("urls", nargs="*")
    parser.add_argument("--file", type=Path)
    args = parser.parse_args()

    targets = [(u, [], None) for u in args.urls]
    if args.file:
        targets += list(_parse_targets(args.file))
    if not targets:
        parser.error("Indica URLs o --file")

    proxy_url = os.getenv("PROXY_URL")
    stealth = Stealth(
        navigator_languages_override=("es-CO", "es", "en-US", "en"),
        navigator_platform_override="Win32",
        navigator_user_agent_override=UA,
    )
    with stealth.use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(
            headless=True,
            proxy={"server": proxy_url} if proxy_url else None,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=UA,
            locale="es-CO",
            timezone_id="America/Bogota",
        )
        page = context.new_page()
        for url, selectors, link_regex in targets:
            inspect(page, url, selectors, link_regex)
            sys.stdout.flush()
        browser.close()


if __name__ == "__main__":
    main()
