# main.py

from datetime import datetime, timedelta
from pymongo import MongoClient
import os
from flask import Request

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "miloszapptest"
TEMPLATES_COLL = "order_templates"
ORDERS_COLL = "orders"
MENUS_COLL = "menus"
DEFAULT_DAYS_AHEAD = 17


def create_orders_for_future_day(days_ahead: int) -> dict:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    templates = db[TEMPLATES_COLL]
    orders = db[ORDERS_COLL]
    menus = db[MENUS_COLL]

    target_date = (datetime.utcnow() + timedelta(days=days_ahead)).date()

    weekday_idx = target_date.weekday()  # 0=pon, 6=niedz
    weekday_fields = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    day_field = weekday_fields[weekday_idx]

    created = 0
    skipped = 0

    cursor = templates.find({day_field: {"$gt": 0}})

    from bson import ObjectId  # ważne: import wewnątrz jeśli używasz

    for tpl in cursor:
        qty = tpl.get(day_field, 0)
        if qty <= 0:
            skipped += 1
            continue

        order_doc = {
            "uid": tpl.get("uid"),
            "pid": tpl.get("pid"),
            "templateId": tpl.get("_id"),
            "quantity": qty,
            "orderDate": datetime(target_date.year, target_date.month, target_date.day),
            "createdAt": datetime.utcnow(),
            "source": "cyclic",
        }

        if tpl.get("type") == "iconic":
            menu_id_str = tpl.get("idd")
            if not menu_id_str:
                skipped += 1
                continue

            try:
                menu_id = ObjectId(menu_id_str)
            except Exception:
                skipped += 1
                continue

            menu_doc = menus.find_one({"_id": menu_id})
            if not menu_doc:
                skipped += 1
                continue

            order_doc["iconicMenuId"] = menu_doc["_id"]
            order_doc["productId"] = menu_doc.get("productId")
        else:
            order_doc["type"] = "normal"
            order_doc["productName"] = tpl.get("productName")
            order_doc["productCategory"] = tpl.get("productCategory")

        orders.insert_one(order_doc)
        created += 1

    client.close()

    return {
        "created": created,
        "skipped": skipped,
        "target_date": str(target_date),
    }


# 👉 TO jest “entrypoint” dla Google Cloud Function
def generate_orders(request: Request):
    """
    HTTP Cloud Function.
    Opcjonalny parametr query: ?days_ahead=2
    """
    # domyślnie z env albo stałej
    days_ahead = DEFAULT_DAYS_AHEAD

    # odczyt parametru z URL, np. /?days_ahead=2
    if request.args and "days_ahead" in request.args:
        try:
            days_ahead = int(request.args["days_ahead"])
        except ValueError:
            pass

    result = create_orders_for_future_day(days_ahead)

    return (
        {
            "status": "ok",
            "days_ahead": days_ahead,
            **result,
        },
        200,
    )

if __name__ == "__main__":
    # ile dni do przodu – na próbę możesz dać np. 1
    days_ahead = 17

    result = create_orders_for_future_day(days_ahead)
    print("Wynik testu:", result)
