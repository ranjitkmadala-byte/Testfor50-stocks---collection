from datetime import date, timedelta

# NSE F&O trading holidays for calendar year 2026.
# Source: NSE/FAOP/71777 dated 2025-12-12.
NSE_FO_HOLIDAYS_2026 = {
    date(2026, 1, 26): "Republic Day",
    date(2026, 3, 3): "Holi",
    date(2026, 3, 26): "Shri Ram Navami",
    date(2026, 3, 31): "Shri Mahavir Jayanti",
    date(2026, 4, 3): "Good Friday",
    date(2026, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2026, 5, 1): "Maharashtra Day",
    date(2026, 5, 28): "Bakri Id",
    date(2026, 6, 26): "Muharram",
    date(2026, 9, 14): "Ganesh Chaturthi",
    date(2026, 10, 2): "Mahatma Gandhi Jayanti",
    date(2026, 10, 20): "Dussehra",
    date(2026, 11, 10): "Diwali-Balipratipada",
    date(2026, 11, 24): "Prakash Gurpurb Sri Guru Nanak Dev",
    date(2026, 12, 25): "Christmas",
}

HOLIDAYS_BY_YEAR = {2026: NSE_FO_HOLIDAYS_2026}


def holiday_name(day: date):
    return HOLIDAYS_BY_YEAR.get(day.year, {}).get(day)


def is_nse_trading_day(day: date) -> bool:
    return day.weekday() < 5 and holiday_name(day) is None


def next_nse_trading_day(day: date) -> date:
    d = day
    while not is_nse_trading_day(d):
        d += timedelta(days=1)
    return d
