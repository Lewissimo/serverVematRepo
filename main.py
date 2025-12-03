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
      np. [2, 12] = 2 dni ROBOCZE wcześniej do godziny 12:00
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


def resolve_product_ids_normal(template: dict) -> list[str]:
    """Dla zwykłych template’ów (type != 'iconic') bierzemy po prostu pid jako jeden produkt."""
    pid = template.get("pid")
    return [pid] if pid else []


def _to_object_id(maybe_id):
    """Pomocniczo: zamień string na ObjectId, jeśli się da, w przeciwnym razie zwróć oryginał."""
    if isinstance(maybe_id, str):
        try:
            return ObjectId(maybe_id)
        except Exception:
            return maybe_id
    return maybe_id


def resolve_product_ids_iconic(
    template: dict,
    target_date_str: str,
    menus_collection,
    product_sets_collection,
) -> list[str]:
    """
    Szuka KONKRETNEGO produktu (jednego) na podstawie:
    - template.ppid         -> _id dynamicznego menu (kolekcja menus)
    - w menu.days           -> dzień z date == target_date_str
    - template.iconicMenuId -> iconicId w iconicLinks

    iconicLinks: [{ iconicId, productId }, ...]
    product_sets: mają productIds – jeden z nich może być podlinkowany
    z iconicProduct przez iconicLinks.productId.

    Zwraca listę z jednym productId (albo pustą listę, jeśli nie znaleziono).
    """

    # 1. dynamiczne menu
    menu_id = template.get("ppid")
    if not menu_id:
        return []

    menu_id = _to_object_id(menu_id)
    menu = menus_collection.find_one({"_id": menu_id})
    if not menu:
        return []

    # 2. znajdź odpowiedni dzień
    day_entry = None
    for day in menu.get("days", []):
        if day.get("date") == target_date_str:
            day_entry = day
            break

    if not day_entry:
        return []

    # 3. zbierz wszystkie produkty dostępne danego dnia
    products_for_day = set(day_entry.get("productIds", []))

    for psid in day_entry.get("productSetIds", []):
        ps_oid = _to_object_id(psid)
        product_set = product_sets_collection.find_one({"_id": ps_oid})
        if product_set:
            for pid in product_set.get("productIds", []):
                products_for_day.add(pid)

    # 4. dopasuj slot ikoniczny -> produkt
    iconic_id = template.get("iconicMenuId")
    if iconic_id:
        for link in day_entry.get("iconicLinks", []):
            if link.get("iconicId") == iconic_id:
                pid = link.get("productId")
                if pid:
                    # opcjonalnie: upewnij się, że ten produkt jest naprawdę na tym dniu menu
                    if not products_for_day or pid in products_for_day:
                        return [pid]

    # 5. fallback – jeśli nie ma linka, ale w template jest pid
    pid_from_template = template.get("pid")
    if pid_from_template and (not products_for_day or pid_from_template in products_for_day):
        return [pid_from_template]

    return []


def resolve_product_ids(
    template: dict,
    target_date_str: str,
    menus_collection,
    product_sets_collection,
) -> list[str]:
    """
    Wybiera odpowiednią metodę w zależności od typu template’a.
    Zwraca listę productId (może być 0, 1 lub wiele – ale w ikonicznym
    przypadku i tak będzie 1).
    """
    if template.get("type") == "iconic":
        return resolve_product_ids_iconic(
            template, target_date_str, menus_collection, product_sets_collection
        )
    else:
        return resolve_product_ids_normal(template)


# --- GŁÓWNA FUNKCJA ---


def generate_orders():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]

    orders_collection = db["orders"]
    templates_collection = db["order_templates"]
    menus_collection = db["menus"]
    product_sets_collection = db["product_sets"]

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

        product_ids = resolve_product_ids(
            tpl, target_date_str, menus_collection, product_sets_collection
        )
        if not product_ids:
            print(
                f"[WARN] Nie znaleziono productId(ów) dla template {tpl['_id']} na {target_date_str}"
            )
            continue

        if uid not in orders_by_uid:
            orders_by_uid[uid] = {
                "uid": uid,
                "date": target_date_str,
                "items": [],
                "editUntil": None,
            }

        # dodajemy każdy produkt (dla seta będzie też 1 productId – ten z iconicLinks)
        for pid in product_ids:
            item = {
                "productId": pid,
                "quantity": qty,
                "templateId": tpl["_id"],
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
