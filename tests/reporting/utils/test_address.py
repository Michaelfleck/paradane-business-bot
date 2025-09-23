import pytest

from project.reporting.utils.address import parseAddressFromDisplay, geocodeAddressToCoords


def test_parse_address_from_list_basic():
    display = ["123 Main St", "Charlotte, NC 28202", "USA"]
    parsed = parseAddressFromDisplay(display)
    assert parsed["address1"] == "123 Main St"
    assert parsed["city"] == "Charlotte"
    assert parsed["state"] == "NC"
    assert parsed["country"] in ("USA", "United States", "UNITED STATES", None)


def test_parse_address_from_string_basic():
    display = "123 Main St, Charlotte NC, USA"
    parsed = parseAddressFromDisplay(display)
    assert parsed["address1"] == "123 Main St"
    assert parsed["city"] == "Charlotte"
    assert parsed["state"] == "NC"
    assert parsed["country"] == "USA"


def test_parse_address_from_dict_fallbacks():
    display = {
        "location": {"city": "Charlotte", "state": "NC"},
        "formatted_address": "123 Main St, Charlotte, NC, USA",
    }
    parsed = parseAddressFromDisplay(display)
    assert parsed["address1"] == "123 Main St"
    assert parsed["city"] == "Charlotte"
    assert parsed["state"] == "NC"
    assert parsed["country"] == "USA"


def test_geocode_address_to_coords_monkeypatch(monkeypatch):
    # Mock GoogleClient path by monkeypatching the module attribute directly
    from project.reporting import utils as _  # ensure package importable

    class FakeClient:
        def __init__(self, key):
            self.client = self

        # emulate googlemaps.Client geocode()
        def geocode(self, line):
            assert "Charlotte" in line
            return [{"geometry": {"location": {"lat": 35.2271, "lng": -80.8431}}}]

    # Inject FakeClient into module
    import project.reporting.utils.address as addr_mod
    addr_mod.GoogleClient = FakeClient  # type: ignore

    address = {"address1": "123 Main St", "city": "Charlotte", "state": "NC", "country": "USA"}
    coords = geocodeAddressToCoords(address)
    assert coords["lat"] == pytest.approx(35.2271, rel=1e-6)
    assert coords["lng"] == pytest.approx(-80.8431, rel=1e-6)