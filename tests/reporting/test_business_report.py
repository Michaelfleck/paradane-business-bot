import pytest

from project.reporting.business_report import generateBusinessReport
from project.reporting.renderer import render_list_block, render_template


class DummyResp:
    def __init__(self, data):
        self.data = data


class DummyClient:
    def __init__(self, business_row, pages_rows):
        self._business = business_row
        self._pages = pages_rows
        self._ctx = {}

    # businesses
    def table(self, name):
        self._ctx["table"] = name
        return self

    def select(self, fields):
        self._ctx["select"] = fields
        return self

    def eq(self, key, value):
        self._ctx[key] = value
        return self

    def single(self):
        assert self._ctx["table"] == "businesses"
        return self._business

    def execute(self):
        assert self._ctx["table"] == "business_pages"
        return DummyResp(self._pages)


def _tpl():
    # Minimal slice of the real template focusing on placeholders we validate
    return """<ul>
<li>The name of the business is <b>{BUSINESS_NAME}</b>.</li>
<li>The business is located at <b>{BUSINESS_ADDRESS}, {BUSINESS_CITY}, {BUSINESS_STATE}, {BUSINESS_COUNTRY}</b>.</li>
<li>The business coordinates are <b>{BUSINESS_COORDS_LAT}, {BUSINESS_COORDS_LONG}</b>.</li>
<li>The current status is <b>{BUSINESS_STATUS}</b>.</li>
<li>The listed category/categories are: <b>{BUSINESS_CATEGORIES}</b>.</li>
<li>The business opening hours are: <b>{BUSINESS_OPEN_DAYS}</b>.</li>
<li><b>Website:</b> <a href="{BUSINESS_WEBSITE_URL}">{BUSINESS_WEBSITE_URL}</a></li>
<li><b>Yelp:</b> <a href="{BUSINESS_WEBSITE_URL}">{BUSINESS_YELP_URL}</a></li>
<li><b>Google Place:</b> <a href="{BUSINESS_GOOGLE_PLACE_URL}">{BUSINESS_GOOGLE_PLACE_URL}</a></li>
<li><b>Email:</b> {BUSINESS_EMAILS}</li>
<li><b>Phone:</b> {BUSINESS_PHONE}</li>
<li><b>Web-Page:</b> <a href="{BUSINESS_CONTACT_PAGE[INDEX]_URL}">{BUSINESS_CONTACT_PAGE[INDEX]_URL}</a></li>
</ul>"""


def test_renderer_list_block_duplication_and_removal():
    tpl = _tpl()
    # Multiple items
    out = render_list_block(tpl, "BUSINESS_CONTACT_PAGE", ["https://a.com/contact", "https://b.com/contact"])
    assert out.count("<li><b>Web-Page:</b>") == 2
    assert "https://a.com/contact" in out and "https://b.com/contact" in out

    # No items -> line removed
    out2 = render_list_block(tpl, "BUSINESS_CONTACT_PAGE", [])
    assert '<li><b>Web-Page:</b>' not in out2


def test_generate_report_with_fallbacks_and_ordering(monkeypatch, tmp_path):
    # Prepare business data with missing coords to force geocode fallback and hours with overnight
    business_id = "biz123"
    business_row = {
        "id": business_id,
        "name": "Foo Bar",
        "url": "https://yelp.com/biz/foo-bar",
        "phone": "704-555-1234",
        "price": "$$",
        "reviews_count": 42,
        "rating": 4.5,
        "is_closed": False,
        "categories": [{"title": "Cafe"}, {"title": "Bakery"}, {"title": "Cafe"}],
        "business_hours": {
            "hours": [
                {"open": [{"day": 5, "start": "1700", "end": "0100", "is_overnight": True}]}
            ]
        },
        "attributes": {"business_temp_closed": False, "menu_url": "https://foo.example.com/menu"},
        "website": None,
        "coordinates": {},  # force fallback
        "geometry": {},     # force fallback, then geocode
        "location": {"city": "Charlotte", "state": "NC"},  # address1 missing to trigger parse from formatted
        "display_address": ["123 Main St", "Charlotte, NC 28202", "USA"],
        "google_enrichment": {"place_id": "PID123", "business_status": "OPERATIONAL", "user_ratings_total": 100},
    }
    pages_rows = [
        {"url": "https://foo.example.com/contact", "email": "info@foo.example.com", "page_type": "Contact"},
        {"url": "https://foo.example.com", "email": "contact@foo.example.org", "page_type": None},
        {"url": "https://foo.example.com/contact-us", "email": "sales@foo.example.com", "page_type": "Page"},
    ]

    dummy = DummyClient(business_row, pages_rows)

    # Monkeypatch supabase get_client
    import project.reporting.business_report as br
    br.get_client = lambda: dummy  # type: ignore

    # Monkeypatch geocode fallback to fixed coords
    import project.reporting.utils.address as addr_mod
    def fake_geocode(addr):
        return {"lat": 35.000000, "lng": -81.000000}
    addr_mod.geocodeAddressToCoords = fake_geocode  # type: ignore

    # Monkeypatch config for phone/defaults and static map key
    import project.reporting.config as cfg_mod
    def fake_cfg():
        class Cfg:
            GOOGLE_API_KEY = "fake-key"
            MAP_DEFAULT_SIZE = "600x400"
            MAP_DEFAULT_ZOOM = 15
            DEFAULT_PHONE_COUNTRY = "US"
        return Cfg()
    cfg_mod.get_report_config = fake_cfg  # type: ignore
    br.get_report_config = fake_cfg  # type: ignore

    # Monkeypatch phone normalization to deterministic value (to avoid real phonenumbers)
    import project.reporting.utils.phone as phone_mod
    phone_mod.normalizePhone = lambda p, c: "(704) 555-1234"  # type: ignore

    # Write a temporary copy of the template the generator reads
    tpl_path = tmp_path / "business-report.html"
    tpl_path.write_text(_tpl(), encoding="utf-8")

    # Monkeypatch path resolution inside generateBusinessReport to read our temp template
    import os
    orig_join = os.path.join
    def fake_join(a, b, c):
        # intercept project/template/business-report.html
        return str(tpl_path)
    monkeypatch.setattr(br.os.path, "join", lambda a, b, c: fake_join(a, b, c))

    html = generateBusinessReport(business_id)

    # Verify important placeholders replaced
    assert "Foo Bar" in html
    assert "Charlotte, NC" in html
    assert "35.000000, -81.000000" in html  # coords line in our test slice
    assert "Open" in html  # from OPERATIONAL
    assert "Cafe, Bakery" in html  # dedup preserving order
    assert "Saturday (5:00 PM - 1:00 AM)" in html  # overnight handling
    assert "https://foo.example.com" in html  # website root domain normalization
    assert "https://www.google.com/maps/place/?q=place_id:PID123" in html

    # Emails should be ordered with website-domain matches first (foo.example.com)
    # local reorder happens in business_report; underlying util orders by len then lexicographic
    assert "info@foo.example.com" in html
    assert "sales@foo.example.com" in html

    # Contact pages duplicated
    assert html.count("<li><b>Web-Page:</b>") >= 2


def test_render_template_basic_replacements():
    tpl = "Name: {BUSINESS_NAME}, Status: {BUSINESS_STATUS}"
    ctx = {"BUSINESS_NAME": "Foo", "BUSINESS_STATUS": "Open"}
    out = render_template(tpl, ctx)
    assert out == "Name: Foo, Status: Open"