#!/usr/bin/env python3
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId

# --- KONFIG ---
MONGODB_URI = "mongodb+srv://lewinskicoding:Marta6021023@vemataps.21klpmx.mongodb.net/?retryWrites=true&w=majority&appName=VematAps"
MONGODB_DB = "miloszapptest"

# ile dni do przodu generujemy zamówienia (np. 7 = poniedziałek za tydzień)
DAYS_OFFSET = 7

# mapowanie pythonowego weekday() -> nazwy pól w order_templates
WEEKDAY_FIELDS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

def js_to_py_weekday(js_day: int) -> int:
    """
    Zamiana numeru dnia tygodnia z JS (0=nd, 6=sb)
    na numer używany przez Pythona (0=pn, 6=nd).
    """
    return (js_day + 6) % 7


def compute_edit_until(template: dict, target_date) -> datetime | None:
    """
    Oblicza editUntil w oparciu o:
    - deadline: [dni_przed, godzina, (opcjonalnie minuta)]
    - deadDays: lista dni tygodnia (JS: 0=nd, 6=sb), które
      NIE są liczone jako dni do deadlinu.

    target_date: datetime.date – dzień realizacji zamówienia.
    """

    deadline = template.get("deadline") or []
    dead_days_js = template.get("deadDays") or []

    if not deadline:
        return None

    # ile dni roboczych wcześniej można odwołać
    days_before = int(deadline[0]) if len(deadline) > 0 else 0
    hour = int(deadline[1]) if len(deadline) > 1 else 0
    minute = int(deadline[2]) if len(deadline) > 2 else 0

    # zamiana deadDays z JS na indeksy Pythona
    dead_days_py = {js_to_py_weekday(int(d)) for d in dead_days_js}

    d = target_date
    remaining = days_before

    # cofamy się o "days_before" dni ROBOCZYCH (pomijając deadDays)
    while remaining > 0:
        d -= timedelta(days=1)
        if d.weekday() not in dead_days_py:
            remaining -= 1

    # deadline w wyliczonym dniu o podanej godzinie
    return datetime(d.year, d.month, d.day, hour, minute)

# --- RESOLVER PRODUKTÓW ---

def get_quantity_for_date(template: dict, target_date) -> int:
    """
    target_date: datetime.date
    Zwraca ilość z order_template na konkretny dzień tygodnia
    (korzysta z pól mon/tue/wed/...).
    """
    weekday_idx = target_date.weekday()  # Monday=0
    field_name = WEEKDAY_FIELDS[weekday_idx]
    return int(template.get(field_name, 0) or 0)


def generate_orders():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]

    orders_collection = db["orders"]
    templates_collection = db["order_templates"]
    menus_collection = db["menus"]
    product_sets_collection = db["product_sets"]
    today = datetime.utcnow().date()
    target_date = today + timedelta(days=DAYS_OFFSET)
    target_date_str = target_date.isoformat()
    templates = list(templates_collection.find({}))

    for template in templates:
        quantity = get_quantity_for_date(template, target_date)
        if quantity <= 0:
            continue

        edit_until = compute_edit_until(template, target_date)

        order_doc = {
            "templateId": template["_id"],
            "date": target_date_str,
            "quantity": quantity,
            "status": "pending",
            "createdAt": datetime.utcnow(),
        }
        if edit_until:
            order_doc["editUntil"] = edit_until

        orders_collection.insert_one(order_doc)
        print(f"Generated order for template {template['_id']} on {target_date_str} with quantity {quantity}")


if __name__ == "__main__":
    generate_orders()


