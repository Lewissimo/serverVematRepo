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


def get_quantity_for_date(template: dict, target_date) -> int:
    """
    target_date: datetime.date
    Zwraca ilość z order_template na konkretny dzień tygodnia
    (korzysta z pól mon/tue/wed/...).
    """
    weekday_idx = target_date.weekday()  # Monday=0
    field_name = WEEKDAY_FIELDS[weekday_idx]
    return int(template.get(field_name, 0) or 0)


def compute_edit_until(template: dict, target_date) -> datetime | None:
    """
    Oblicza editUntil na podstawie pól:
    - deadDays: [liczba_dni_przed]
    - deadline: [godzina, minuta]

    target_date: datetime.date (dzień zamówienia)
    """
    dead_days = template.get("deadDays") or []
    deadline = template.get("deadline") or []

    if not dead_days and not deadline:
        return None

    days_before = int(dead_days[0]) if dead_days else 0
    hour = int(deadline[0]) if len(deadline) > 0 else 0
    minute = int(deadline[1]) if len(deadline) > 1 else 0

    edit_date = target_date - timedelta(days=days_before)
    return datetime(edit_date.year, edit_date.month, edit_date.day, hour, minute)


def resolve_product_id_normal(template: dict) -> str | None:
    """Dla zwykłych template’ów (type != 'iconic') bierzemy po prostu pid."""
    return template.get("pid")


def resolve_product_id_iconic(template: dict, target_date_str: str, menus_collection) -> str | None:
    """
    Szuka produktu na podstawie:
    - template.iconicMenuId -> dokument z kolekcji menus
    - w menu.days znajdujemy rekord z date == target_date_str
    - w tym dniu szukamy w iconicLinks pozycji z iconicId == template.idd
    - zwracamy link.productId
    """
    iconic_product_id = template.get("iconicMenuId")
    if not iconic_product_id:
        return None
    menu_id = template.get("idd")
    if not menu_id:
        return None

    # menu_id może być stringiem lub ObjectId
    if isinstance(menu_id, str):
        try:
            menu_id = ObjectId(menu_id)
        except Exception:
            pass

    menu = menus_collection.find_one({"_id": menu_id})
    if not menu:
        return None

    day_entry = None
    for day in menu.get("days", []):
        if day.get("date") == target_date_str:
            day_entry = day
            break
    print(day_entry)
    if not day_entry:
        return None

    iconic_id = template.get("idd")
    if not iconic_id:
        return None

    for link in day_entry.get("iconicLinks", []):
        if link.get("iconicId") == iconic_id:
            product_id = link.get("productId")
            if product_id:
                return product_id

    return None


def resolve_product_id(template: dict, target_date_str: str, menus_collection) -> str | None:
    """
    Wybiera odpowiednią metodę w zależności od typu template’a.
    """
    if template.get("type") == "iconic":
        return resolve_product_id_iconic(template, target_date_str, menus_collection)
    else:
        return resolve_product_id_normal(template)


def generate_orders():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]

    orders_collection = db["orders"]
    templates_collection = db["order_templates"]
    menus_collection = db["menus"]

    # data, na którą generujemy zamówienia (dziś + DAYS_OFFSET)
    today = datetime.utcnow().date()
    target_date = today + timedelta(days=DAYS_OFFSET)
    target_date_str = target_date.isoformat()

    print(f"Generuję zamówienia na dzień: {target_date_str}")

    templates = list(templates_collection.find({}))

    # Zbierzemy dane w słowniku: uid -> {uid, date, items[], editUntil}
    orders_by_uid: dict[str, dict] = {}

    for tpl in templates:
        qty = get_quantity_for_date(tpl, target_date)
        if qty <= 0:
            continue  # ten template nie ma nic na ten dzień

        uid = tpl["uid"]

        product_id = resolve_product_id(tpl, target_date_str, menus_collection)
        if not product_id:
            print(f"[WARN] Nie znaleziono productId dla template {tpl['_id']} na {target_date_str}")
            continue

        item = {
            "productId": product_id,
            "quantity": qty,
            "templateId": tpl["_id"],
        }

        if uid not in orders_by_uid:
            orders_by_uid[uid] = {
                "uid": uid,
                "date": target_date_str,
                "items": [],
                "editUntil": None,
            }

        orders_by_uid[uid]["items"].append(item)

        # policz editUntil i weź najwcześniejszy dla danego usera
        edit_until_tpl = compute_edit_until(tpl, target_date)
        if edit_until_tpl:
            current = orders_by_uid[uid]["editUntil"]
            if current is None or edit_until_tpl < current:
                orders_by_uid[uid]["editUntil"] = edit_until_tpl

    now = datetime.utcnow()

    # zapis / upsert do kolekcji orders
    for uid, order_data in orders_by_uid.items():
        query = {"uid": uid, "date": order_data["date"]}
        update = {
            "$set": {
                "uid": uid,
                "date": order_data["date"],
                "items": order_data["items"],
                "status": "new",
                "editUntil": order_data["editUntil"],
                "updatedAt": now,
            },
            "$setOnInsert": {
                "createdAt": now,
            },
        }

        result = orders_collection.update_one(query, update, upsert=True)

        if result.upserted_id:
            print(f"[INSERT] order {result.upserted_id} dla usera {uid}")
        else:
            print(f"[UPDATE] order dla usera {uid}")

    client.close()
    print("Gotowe.")


if __name__ == "__main__":
    generate_orders()
