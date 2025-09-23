import pytest

from project.reporting.utils.web import toRootDomain, buildGooglePlaceUrl, collectBusinessEmails, collectContactPages


class DummyResp:
    def __init__(self, data):
        self.data = data


class DummyClient:
    def __init__(self, pages):
        self._pages = pages

    def table(self, name):
        assert name == "business_pages"
        self._query = {"table": name}
        return self

    def select(self, fields):
        self._query["select"] = fields
        return self

    def eq(self, key, value):
        self._query[key] = value
        return self

    def execute(self):
        # Always return the preset pages
        return DummyResp(self._pages)


def test_toRootDomain_basic():
    assert toRootDomain("http://sub.example.com/path") == "https://example.com"
    assert toRootDomain("example.com") == "https://example.com"
    assert toRootDomain("https://www.gov.uk") == "https://gov.uk"


def test_buildGooglePlaceUrl():
    assert buildGooglePlaceUrl("abc123") == "https://www.google.com/maps/place/?q=place_id:abc123"
    assert buildGooglePlaceUrl(None) is None
    assert buildGooglePlaceUrl("") is None


def test_collectBusinessEmails_and_contact_pages(monkeypatch):
    # Prepare test data
    business_id = "biz1"
    pages = [
        {"url": "https://example.com/contact", "email": "Info@Example.com", "page_type": "Contact"},
        {"url": "https://example.com/about", "email": "admin@example.com", "page_type": "About"},
        {"url": "https://example.com/Contact-Us", "email": "sales@example.com", "page_type": "Page"},
        {"url": "https://example.com", "email": "info@example.org", "page_type": None},
        {"url": "https://example.com/contacts", "email": "contact@example.com", "page_type": "page"},
        {"url": "https://example.com/contact", "email": "info@example.com", "page_type": "Contact"},  # duplicate url, new email
    ]

    dummy = DummyClient(pages)

    # Monkeypatch supabase client
    import project.reporting.utils.web as web_mod
    def fake_get_client():
        return dummy
    web_mod.get_client = fake_get_client  # type: ignore

    emails = collectBusinessEmails(business_id)
    # Normalize, validate, dedupe, order by len then lexicographic
    assert emails == [
        "info@example.com",
        "admin@example.com",
        "sales@example.com",
        "contact@example.com",
        "info@example.org",
    ]

    contacts = collectContactPages(business_id)
    # Contact type bucket first, sorted, then contact-looking urls, then others
    assert contacts == [
        "https://example.com/contact",
        "https://example.com/contact",
        "https://example.com/Contact-Us",
        "https://example.com/contacts",
        "https://example.com",
        "https://example.com/about",
    ]