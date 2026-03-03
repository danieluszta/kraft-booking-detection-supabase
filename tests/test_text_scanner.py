"""Unit tests for lib.text_scanner — regex pattern matching."""

import pytest
from pathlib import Path
from lib.text_scanner import load_patterns, scan_text, scan_domains

CONFIGS_DIR = Path(__file__).parent.parent / "configs"


@pytest.fixture(scope="module")
def patterns():
    return load_patterns(str(CONFIGS_DIR / "booking_fingerprints.yaml"))


# -------------------------------------------------------------------
# Pattern loading
# -------------------------------------------------------------------

class TestLoadPatterns:
    def test_loads_all_patterns(self, patterns):
        assert len(patterns) >= 25

    def test_each_pattern_has_required_keys(self, patterns):
        for p in patterns:
            assert "label" in p
            assert "regex" in p
            assert "category" in p

    def test_categories_are_known(self, patterns):
        known = {"booking_platform", "ecommerce", "payment_signal", "booking_signal"}
        for p in patterns:
            assert p["category"] in known, f"Unknown category: {p['category']} on {p['label']}"


# -------------------------------------------------------------------
# True positives — known booking platform embeds
# -------------------------------------------------------------------

class TestBookingPlatformDetection:
    def test_bokun_widget(self, patterns):
        html = '<script src="https://widget.bokun.io/latest/loader.js"></script>'
        hits = scan_text(html, patterns)
        labels = {h["label"] for h in hits}
        assert "bokun" in labels

    def test_bokun_dot_io(self, patterns):
        html = '<iframe src="https://bokun.io/checkout/abc"></iframe>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "bokun" for h in hits)

    def test_fareharbor_embed(self, patterns):
        html = '<script src="https://fareharbor.com/embeds/script/"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "fareharbor" for h in hits)

    def test_fareharbor_fhstart(self, patterns):
        html = '<script src="https://fhstart.com/widget.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "fareharbor" for h in hits)

    def test_rezdy_widget(self, patterns):
        html = '<script src="https://widgets.rezdy.com/catalog.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "rezdy" for h in hits)

    def test_checkfront(self, patterns):
        html = '<iframe src="https://bookings.checkfront.com/reserve/"></iframe>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "checkfront" for h in hits)

    def test_peek_pro(self, patterns):
        html = '<script src="https://js.peekpro.com/widget.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "peek_pro" for h in hits)

    def test_xola(self, patterns):
        html = '<script src="https://checkout.xola.com/index.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "xola" for h in hits)

    def test_regiondo(self, patterns):
        html = '<script src="https://www.regiondo.com/widget/"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "regiondo" for h in hits)

    def test_bookeo(self, patterns):
        html = '<iframe src="https://bookeo.com/mycompany"></iframe>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "bookeo" for h in hits)

    def test_ventrata(self, patterns):
        html = '<script src="https://booking.ventrata.com/widget.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "ventrata" for h in hits)

    def test_viator_widget(self, patterns):
        html = '<script src="https://widget.viator.com/loader.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "viator_widget" for h in hits)

    def test_getyourguide_widget(self, patterns):
        html = '<script src="https://widget.getyourguide.com/default/"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "getyourguide_widget" for h in hits)


# -------------------------------------------------------------------
# Ecommerce / payment signals
# -------------------------------------------------------------------

class TestEcommerceDetection:
    def test_woocommerce_class(self, patterns):
        html = '<div class="woocommerce"><a class="add_to_cart_button">Add</a></div>'
        hits = scan_text(html, patterns)
        labels = {h["label"] for h in hits}
        assert "woocommerce" in labels

    def test_shopify_cdn(self, patterns):
        html = '<script src="https://cdn.shopify.com/s/files/theme.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "shopify" for h in hits)

    def test_stripe_js(self, patterns):
        html = '<script src="https://js.stripe.com/v3/"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "stripe" for h in hits)

    def test_paypal_sdk(self, patterns):
        html = '<script src="https://www.paypal.com/sdk/js?client-id=abc"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "paypal" for h in hits)


# -------------------------------------------------------------------
# True negatives — should NOT trigger booking_platform/ecommerce/payment
# -------------------------------------------------------------------

class TestTrueNegatives:
    def _platform_hits(self, hits):
        return [h for h in hits if h["category"] in ("booking_platform", "ecommerce", "payment_signal")]

    def test_plain_html(self, patterns):
        html = "<html><body><h1>Hello World</h1></body></html>"
        assert self._platform_hits(scan_text(html, patterns)) == []

    def test_contact_form_only(self, patterns):
        html = """
        <html><body>
        <h1>Adventure Tours</h1>
        <p>Contact us: info@adventure.com</p>
        <a href="/contact">Send us a message</a>
        <p>Call to book: +1 555 1234</p>
        </body></html>
        """
        assert self._platform_hits(scan_text(html, patterns)) == []

    def test_blog_site(self, patterns):
        html = """
        <html><body>
        <h1>Travel Blog</h1>
        <p>10 best places to visit in 2024</p>
        <a href="/about">About the author</a>
        <a href="/blog">Latest posts</a>
        </body></html>
        """
        assert self._platform_hits(scan_text(html, patterns)) == []

    def test_enquire_only(self, patterns):
        html = """
        <html><body>
        <h1>Safari Tours</h1>
        <a href="/enquire">Enquire now</a>
        <a href="https://wa.me/123">WhatsApp us</a>
        </body></html>
        """
        assert self._platform_hits(scan_text(html, patterns)) == []

    def test_booking_dot_com_link_is_not_our_platform(self, patterns):
        """A link TO booking.com should not trigger bookeo detection."""
        html = '<a href="https://www.booking.com/hotel/us/myhotel">See on Booking.com</a>'
        hits = self._platform_hits(scan_text(html, patterns))
        # booking.com should not match bookeo regex
        bookeo_hits = [h for h in hits if h["label"] == "bookeo"]
        assert bookeo_hits == []


# -------------------------------------------------------------------
# Edge cases
# -------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_string(self, patterns):
        assert scan_text("", patterns) == []

    def test_case_insensitive(self, patterns):
        html = '<script src="https://WIDGET.BOKUN.IO/loader.js"></script>'
        hits = scan_text(html, patterns)
        assert any(h["label"] == "bokun" for h in hits)

    def test_multiple_platforms_detected(self, patterns):
        html = """
        <script src="https://js.stripe.com/v3/"></script>
        <div class="woocommerce">
        <a class="add_to_cart_button">Add to cart</a>
        </div>
        """
        hits = scan_text(html, patterns)
        labels = {h["label"] for h in hits}
        assert "stripe" in labels
        assert "woocommerce" in labels

    def test_match_count_accurate(self, patterns):
        html = "book now book now book now"
        hits = scan_text(html, patterns)
        cta_hit = next(h for h in hits if h["label"] == "book_now_cta")
        assert cta_hit["match_count"] == 3

    def test_matched_text_truncated(self, patterns):
        html = "book now " * 100
        hits = scan_text(html, patterns)
        cta_hit = next(h for h in hits if h["label"] == "book_now_cta")
        assert len(cta_hit["matched_text"]) <= 200


# -------------------------------------------------------------------
# Batch scanning
# -------------------------------------------------------------------

class TestScanDomains:
    def test_returns_all_domains(self, patterns):
        domains = {
            "has-booking.com": '<script src="https://widget.bokun.io/"></script>',
            "no-booking.com": "<html><body>Hello</body></html>",
        }
        results = scan_domains(domains, patterns)
        assert "has-booking.com" in results
        assert "no-booking.com" in results

    def test_empty_text_returns_empty_hits(self, patterns):
        results = scan_domains({"empty.com": ""}, patterns)
        assert results["empty.com"] == []

    def test_none_text_returns_empty_hits(self, patterns):
        results = scan_domains({"none.com": None}, patterns)
        assert results["none.com"] == []
