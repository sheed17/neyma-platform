import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline.internal_link_support import (  # noqa: E402
    CrawlConfig,
    build_internal_link_graph,
    compute_inbound_internal_link_support,
)


def test_inbound_counts_with_canonical_and_context_breakdown():
    base = "https://example.com"
    pages = [
        {
            "url": f"{base}/",
            "html": """
              <html><body>
                <nav><a href="/services/implants">Implants</a></nav>
              </body></html>
            """,
            "depth": 0,
        },
        {
            "url": f"{base}/about",
            "html": """
              <html><body>
                <footer><a href="/services/dental-implants/">Implants</a></footer>
              </body></html>
            """,
            "depth": 1,
        },
        {
            "url": f"{base}/blog/post-1",
            "html": """
              <html><body>
                <main><a href="/services/implants?utm_source=g#top">Implants</a></main>
              </body></html>
            """,
            "depth": 1,
        },
        {
            "url": f"{base}/services/implants",
            "html": """
              <html><head>
                <link rel="canonical" href="/services/dental-implants/" />
              </head><body><h1>Implants</h1></body></html>
            """,
            "depth": 1,
        },
        {
            "url": f"{base}/services/dental-implants/",
            "html": "<html><body><h1>Implants canonical</h1></body></html>",
            "depth": 1,
        },
    ]
    graph = build_internal_link_graph(base, config=CrawlConfig(mode="fast_html"), pre_crawled_pages=pages)
    out = compute_inbound_internal_link_support(f"{base}/services/implants/", graph)

    assert out["canonical_target_url"] == f"{base}/services/dental-implants"
    assert out["inbound"]["unique_pages_all"] == 3
    assert out["inbound"]["unique_pages_body_only"] == 1
    assert out["inbound"]["total_links_all"] == 3
    assert out["inbound"]["contexts"]["nav"] == 1
    assert out["inbound"]["contexts"]["footer"] == 1
    assert out["inbound"]["contexts"]["body"] == 1
    assert out["orphan"]["is_orphan_all"] is False
    assert out["orphan"]["is_orphan_body_only"] is False


def test_orphan_body_only_when_links_are_nav_footer_only():
    base = "https://example.com"
    pages = [
        {
            "url": f"{base}/",
            "html": '<html><body><nav><a href="/services/veneers">Veneers</a></nav></body></html>',
            "depth": 0,
        },
        {
            "url": f"{base}/contact",
            "html": '<html><body><footer><a href="/services/veneers/">Veneers</a></footer></body></html>',
            "depth": 1,
        },
        {"url": f"{base}/services/veneers", "html": "<html><body>Veneers</body></html>", "depth": 1},
    ]
    graph = build_internal_link_graph(base, config=CrawlConfig(mode="fast_html"), pre_crawled_pages=pages)
    out = compute_inbound_internal_link_support(f"{base}/services/veneers", graph)

    assert out["inbound"]["unique_pages_all"] == 2
    assert out["inbound"]["unique_pages_body_only"] == 0
    assert out["orphan"]["is_orphan_all"] is False
    assert out["orphan"]["is_orphan_body_only"] is True


def test_auto_mode_escalates_to_rendered_when_js_heavy():
    base = "https://js-example.com"
    base_slash = f"{base}/"
    rendered_home = """
      <html><body><main>
        <a href="/services/dental-implants">Implants</a>
      </main></body></html>
    """
    fast_pages = {
        base: '<html><body><div id="__next"></div><script>window.__NEXT_DATA__={}</script></body></html>',
        base_slash: '<html><body><div id="__next"></div><script>window.__NEXT_DATA__={}</script></body></html>',
        f"{base}/services/dental-implants": "<html><body>Implants</body></html>",
        f"{base}/sitemap.xml": "",
    }
    rendered_pages = {
        base: rendered_home,
        base_slash: rendered_home,
    }

    def fake_fetch(url: str, _timeout_ms: int):
        return fast_pages.get(url)

    def fake_render(url: str, _timeout_ms: int):
        return rendered_pages.get(url)

    graph = build_internal_link_graph(
        base,
        config=CrawlConfig(mode="auto", max_pages=10, max_depth=2, timeout_ms=8000),
        fetch_html_fn=fake_fetch,
        render_html_fn=fake_render,
    )
    out = compute_inbound_internal_link_support(f"{base}/services/dental-implants", graph)

    assert out["crawl_meta"]["used_rendered_mode"] is True
    assert out["inbound"]["unique_pages_body_only"] == 1


def test_sitemap_canonical_slash_consolidation():
    base = "https://wp-example.com"
    base_slash = f"{base}/"
    pages = {
        f"{base}/sitemap.xml": f"""
          <urlset>
            <url><loc>{base}/</loc></url>
            <url><loc>{base}/services/implants</loc></url>
            <url><loc>{base}/services/implants/</loc></url>
          </urlset>
        """,
        base: '<html><body><main><a href="/services/implants/">Implants</a></main></body></html>',
        base_slash: '<html><body><main><a href="/services/implants/">Implants</a></main></body></html>',
        f"{base}/services/implants": '<html><head><link rel="canonical" href="/services/implants/" /></head><body>Implants</body></html>',
        f"{base}/services/implants/": "<html><body>Implants Canonical</body></html>",
    }

    def fake_fetch(url: str, _timeout_ms: int):
        return pages.get(url)

    graph = build_internal_link_graph(
        base,
        config=CrawlConfig(mode="fast_html", max_pages=10, max_depth=2, timeout_ms=8000),
        fetch_html_fn=fake_fetch,
    )
    out = compute_inbound_internal_link_support(f"{base}/services/implants", graph)

    assert out["canonical_target_url"] == f"{base}/services/implants"
    assert out["inbound"]["unique_pages_body_only"] == 1
