import pytest

from project.reporting.utils.hours import to12h, formatBusinessHours


def test_to12h_basic_and_edge_cases():
    assert to12h("0000") == "12:00 AM"   # midnight
    assert to12h("1200") == "12:00 PM"   # noon
    assert to12h("0900") == "9:00 AM"
    assert to12h("1700") == "5:00 PM"
    assert to12h("23:15") == "11:15 PM"
    # Graceful fallback for bad input
    assert to12h("bad") == "bad"


def test_format_business_hours_yelp_standard():
    hours = {
        "hours": [
            {
                "open": [
                    {"day": 0, "start": "0900", "end": "1700"},  # Monday
                    {"day": 1, "start": "1000", "end": "1800"},  # Tuesday
                ]
            }
        ]
    }
    out = formatBusinessHours(hours)
    assert "Monday (9:00 AM - 5:00 PM)" in out
    assert "Tuesday (10:00 AM - 6:00 PM)" in out
    # Others closed
    assert "Sunday (Closed)" in out


def test_format_business_hours_yelp_overnight():
    # Overnight by end < start
    hours = {
        "hours": [
            {
                "open": [
                    {"day": 5, "start": "1700", "end": "0100", "is_overnight": True},  # Saturday overnight
                ]
            }
        ]
    }
    out = formatBusinessHours(hours)
    assert "Saturday (5:00 PM - 1:00 AM)" in out


def test_format_business_hours_google_periods_and_weekday_text():
    # periods style
    opening_hours = {
        "periods": [
            {"open": {"day": 2, "time": "0900"}, "close": {"day": 2, "time": "2100"}},  # Wednesday
        ]
    }
    out = formatBusinessHours(opening_hours)
    assert "Wednesday (9:00 AM - 9:00 PM)" in out

    # weekday_text style
    opening_hours2 = {
        "weekday_text": [
            "Monday: 9:00 AM – 5:00 PM",
            "Tuesday: 9:00 AM – 5:00 PM",
            "Wednesday: 9:00 AM – 5:00 PM",
            "Thursday: 9:00 AM – 5:00 PM",
            "Friday: 9:00 AM – 5:00 PM",
            "Saturday: Closed",
            "Sunday: Closed",
        ]
    }
    out2 = formatBusinessHours(opening_hours2)
    assert "Monday (9:00 AM – 5:00 PM)" in out2
    assert "Saturday (Closed)" in out2