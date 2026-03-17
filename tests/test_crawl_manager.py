import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline.crawl_manager import CrawlManager


def test_crawl_manager_sitemap_nav_and_forced_paths():
    base = "https://example-ortho.com"
    sitemap_xml = """
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://example-ortho.com/services/braces</loc></url>
      <url><loc>https://example-ortho.com/services/invisalign</loc></url>
      <url><loc>https://example-ortho.com/contact</loc></url>
      <url><loc>https://example-ortho.com/schedule</loc></url>
      <url><loc>https://example-ortho.com/services/retainers</loc></url>
    </urlset>
    """
    page_map = {
        base: """
            <html><body>
                <a href="/services/braces">Braces</a>
                <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://example-ortho.com/sitemap.xml": sitemap_xml,
        "https://example-ortho.com/services/braces": "<html><body><h1>Braces</h1><p>braces braces braces</p></body></html>",
        "https://example-ortho.com/services/invisalign": "<html><body><h1>Invisalign</h1><p>invisalign</p></body></html>",
        "https://example-ortho.com/services/retainers": "<html><body><h1>Retainers</h1></body></html>",
        "https://example-ortho.com/contact": "<html><body><form></form></body></html>",
        "https://example-ortho.com/schedule": "<html><body>Schedule</body></html>",
    }

    cm = CrawlManager(base, fetch_fn=lambda url: page_map.get(url))
    out = cm.crawl(practice_type="orthodontist")

    assert out["pages_crawled"] >= 5
    assert out["confidence"] in {"medium", "high"}
    assert out["js_detected"] is False
    assert out["sitemap_found"] is True


def test_crawl_manager_js_detection_reduces_confidence():
    base = "https://js-site.example"
    home = """
    <html><body>
      <div id="root"></div>
      <script src="/static/chunk.js"></script>
      <script src="/static/main.js"></script>
      <script>window.__DATA__={};</script>
    </body></html>
    """
    cm = CrawlManager(base, fetch_fn=lambda url: home if url == base else None)
    out = cm.crawl(practice_type="general_dentist")
    assert out["js_detected"] is True
    assert out["confidence"] == "low"


def test_crawl_manager_prioritizes_forced_service_paths_under_tight_page_cap():
    base = "https://crowded.example"
    # Sitemap is intentionally crowded with generic pages.
    sitemap_urls = "\n".join(
        f"<url><loc>{base}/page-{i}</loc></url>" for i in range(1, 40)
    )
    sitemap_xml = f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{sitemap_urls}</urlset>'

    page_map = {
        base: "<html><body><a href='/about'>About</a></body></html>",
        f"{base}/sitemap.xml": sitemap_xml,
        f"{base}/invisalign": "<html><body><h1>Invisalign</h1><p>clear aligners</p></body></html>",
        f"{base}/page-1": "<html><body>p1</body></html>",
        f"{base}/page-2": "<html><body>p2</body></html>",
        f"{base}/page-3": "<html><body>p3</body></html>",
    }

    cm = CrawlManager(base, fetch_fn=lambda url: page_map.get(url))
    cm.max_pages = 4  # homepage + 3 pages; prioritize forced service URLs
    out = cm.crawl(practice_type="general_dentist")

    crawled = set((out.get("pages") or {}).keys())
    assert f"{base}/invisalign" in crawled


def test_crawl_manager_extracts_unquoted_and_data_links():
    base = "https://links.example"
    home = """
    <html><body>
      <a href=/invisalign>Invisalign</a>
      <div data-href="/dental-implants">Implants</div>
      <button data-url="/contact">Contact</button>
    </body></html>
    """
    page_map = {
        base: home,
        f"{base}/invisalign": "<html><body>i</body></html>",
        f"{base}/dental-implants": "<html><body>d</body></html>",
        f"{base}/contact": "<html><body>c</body></html>",
    }
    cm = CrawlManager(base, fetch_fn=lambda url: page_map.get(url))
    out = cm.crawl(practice_type="general_dentist")
    crawled = set((out.get("pages") or {}).keys())
    assert f"{base}/invisalign" in crawled
    assert f"{base}/dental-implants" in crawled
    assert f"{base}/contact" in crawled
