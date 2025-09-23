import pytest

from project.reporting.utils.phone import normalizePhone


class DummyNum:
    def __init__(self, e164="+17045551234"):
        self.e164 = e164


class DummyLib:
    PhoneNumberFormat = type("PNF", (), {"E164": 0, "NATIONAL": 1})

    def __init__(self):
        self._last = None

    def parse(self, phone, region):
        # Very naive dummy parse; return object stored in self
        self._last = DummyNum("+17045551234")
        return self._last

    def is_possible_number(self, num):
        return True

    def is_valid_number(self, num):
        return True

    def format_number(self, num, fmt):
        if fmt == self.PhoneNumberFormat.NATIONAL:
            return "(704) 555-1234"
        return "+17045551234"


def test_normalize_us_national(monkeypatch):
    import project.reporting.utils.phone as phone_mod
    dummy = DummyLib()
    phone_mod.phonenumbers = dummy  # type: ignore
    phone_mod.PhoneNumberFormat = dummy.PhoneNumberFormat  # type: ignore

    assert normalizePhone("704-555-1234", "US") == "(704) 555-1234"


def test_normalize_e164_other_country(monkeypatch):
    import project.reporting.utils.phone as phone_mod
    dummy = DummyLib()
    phone_mod.phonenumbers = dummy  # type: ignore
    phone_mod.PhoneNumberFormat = dummy.PhoneNumberFormat  # type: ignore

    assert normalizePhone("+44 20 7946 0958", "GB") == "+17045551234"


def test_normalize_failure_when_library_missing(monkeypatch):
    import project.reporting.utils.phone as phone_mod
    phone_mod.phonenumbers = None  # type: ignore
    assert normalizePhone("704-555-1234", "US") is None