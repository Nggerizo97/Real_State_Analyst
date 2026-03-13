import sys
from playwright.sync_api import sync_playwright

def test_pagination():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        url = "https://www.fincaraiz.com.co/venta/apartamentos/bogota"
        print(f"Loading {url}")
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)
        
        pagination = page.query_selector("ul[class*='Pagination'], nav, .pagination, [aria-label*='pagin'], [class*='pagin']")
        next_button = page.query_selector("a[href*=pagina]:has-text('>')")
        
        with open("pagination_debug.txt", "w", encoding="utf-8") as f:
            if pagination:
                f.write("--- PAGINATION HTML ---\n")
                f.write(pagination.inner_html() + "\n\n")
            
            if next_button:
                f.write("--- NEXT BUTTON HTML ---\n")
                f.write(next_button.evaluate("el => el.outerHTML") + "\n")
            else:
                f.write("--- NEXT BUTTON NOT FOUND ---\n")
                # Let's try to grab any link that looks like pagination
                all_links = page.query_selector_all("a[href*=pagina]")
                f.write(f"Found {len(all_links)} links with 'pagina' in href:\n")
                for link in all_links:
                    f.write(link.evaluate("el => el.outerHTML") + "\n")
            
        browser.close()

if __name__ == "__main__":
    test_pagination()
