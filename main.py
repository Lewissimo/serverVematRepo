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
    Szuka produktu(ów) na podstawie:
    - template.ppid -> dokument z kolekcji menus (dynamiczne menu)
    - w menu.days znajdujemy rekord z date == target_date_str
    - w tym dniu szukamy w iconicLinks pozycji z iconicId == template.iconicMenuId
    - jeśli link ma productId -> zwracamy [productId]
    - jeśli link ma productSetId -> pobieramy product_set i zwracamy listę productId z tego seta
    """

    # ppid = id dynamicznego menu
    menu_id = template.get("ppid")
    if not menu_id:
        return []

    menu_id = _to_object_id(menu_id)
    menu = menus_collection.find_one({"_id": menu_id})
    if not menu:
        return []

    # znajdź odpowiedni dzień
    day_entry = None
    for day in menu.get("days", []):
        if day.get("date") == target_date_str:
            day_entry = day
            break

    if not day_entry:
        return []

    # iconicMenuId = id produktu dynamicznego (slotu ikonicznego) w tym menu
    iconic_id = template.get("iconicMenuId")
    if not iconic_id:
        return []

    # znajdź link dla danego slotu
    link = None
    for l in day_entry.get("iconicLinks", []):
        if l.get("iconicId") == iconic_id:
            link = l
            break

    if not link:
        return []

    # 1) prosty przypadek – podpięty bezpośrednio produkt
    product_id = link.get("productId")
    if product_id:
        return [product_id]

    # 2) przypadek seta – podpięty set, trzeba go rozwinąć na produkty
    product_set_id = (
        link.get("productSetId")
        or link.get("productSetID")
        or link.get("product_set_id")
    )
    if not product_set_id:
        return []

    product_set_id = _to_object_id(product_set_id)
    product_set = product_sets_collection.find_one({"_id": product_set_id})
    if not product_set:
        return []

    product_ids: list[str] = []
    # zakładam strukturę: product_set["elements"] = [{ "productId": ... }, ...]
    for el in product_set.get("elements", []):
        pid = el.get("productId")
        if pid:
            product_ids.append(pid)

    return product_ids


def resolve_product_ids(
    template: dict,
    target_date_str: str,
    menus_collection,
    product_sets_collection,
) -> list[str]:
    """
    Wybiera odpowiednią metodę w zależności od typu template’a.
    Zwraca listę productId (może być 0, 1 lub wiele – np. gdy set).
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
            print(f"[WARN] Nie znaleziono productId(ów) dla template {tpl['_id']} na {target_date_str}")
            continue

        if uid not in orders_by_uid:
            orders_by_uid[uid] = {
                "uid": uid,
                "date": target_date_str,
                "items": [],
                "editUntil": None,
            }

        # jeśli to set, dodajemy kilka produktów; każdy z tą samą ilością qty
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
