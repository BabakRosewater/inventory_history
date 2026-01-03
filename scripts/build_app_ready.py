import csv, json, os, re
from datetime import datetime, timezone

def pick(row, *names):
  for n in names:
    v = row.get(n, "")
    if v is None: 
      continue
    v = str(v).strip()
    if v != "":
      return v
  return ""

def to_float(x):
  if x is None:
    return None
  s = str(x).strip()
  if not s:
    return None
  s = re.sub(r"[^\d.\-]", "", s)  # remove $ and commas
  try:
    return float(s)
  except:
    return None

def norm_state(s):
  s = (s or "").strip().upper()
  if s in ("NEW","N"):
    return "NEW"
  if s in ("USED","U","PREOWNED","PRE-OWNED","CPO"):
    return "USED"
  return s or ""

def load_json(path, default):
  if os.path.exists(path):
    with open(path, "r", encoding="utf-8") as f:
      return json.load(f)
  return default

def save_json(path, obj):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  with open(path, "w", encoding="utf-8") as f:
    json.dump(obj, f, indent=2, ensure_ascii=False)

def main():
  INP = os.environ.get("INP", "data/latest/MP16607.csv")
  OUT = os.environ.get("OUT", "data/latest/app_ready.csv")
  STATE = os.environ.get("STATE", "data/state/first_seen.json")
  META = os.environ.get("META", "data/latest/meta.json")
  DELTA = os.environ.get("DELTA", "data/latest/delta.json")

  now = datetime.now(timezone.utc)
  now_date = now.date()

  first_seen = load_json(STATE, {})  # key -> iso ts

  # load previous OUT (for delta)
  prev = {}
  if os.path.exists(OUT):
    with open(OUT, "r", encoding="utf-8", errors="ignore", newline="") as f:
      r = csv.DictReader(f)
      for row in r:
        k = row.get("key","").strip()
        if k:
          prev[k] = row

  rows_out = []
  keys_now = set()

  with open(INP, "r", encoding="utf-8", errors="ignore", newline="") as f:
    r = csv.DictReader(f)
    headers = r.fieldnames or []

    # choose primary key: vin if present, else vehicle_id
    key_field = "vin" if "vin" in headers else ("vehicle_id" if "vehicle_id" in headers else None)
    if not key_field:
      raise SystemExit("No vin or vehicle_id column found; can’t key rows.")

    for row in r:
      key = pick(row, key_field).upper()
      if not key:
        continue

      keys_now.add(key)

      if key not in first_seen:
        first_seen[key] = now.isoformat()

      try:
        fs_date = datetime.fromisoformat(first_seen[key].replace("Z","+00:00")).date()
      except:
        fs_date = now_date

      age_days = (now_date - fs_date).days

      year = pick(row, "year")
      model = pick(row, "model")
      trim = pick(row, "Trim", "trim")
      url = pick(row, "url")
      img = pick(row, "image[0].url", "image_url", "photo_url")

      # prices
      msrp = to_float(pick(row, "price", "msrp"))
      sale = to_float(pick(row, "sale_price", "sale_price_usd", "sale"))

      # if only one price exists, treat it as sale
      if sale is None and msrp is not None:
        sale = msrp

      discount = 0.0
      if (msrp is not None) and (sale is not None) and (msrp > sale):
        discount = round(msrp - sale, 2)

      state = norm_state(pick(row, "state_of_vehicle", "condition", "availability"))

      # stock fallback
      stock = pick(row, "stock", "stock_number", "stockNumber", "stock_no")
      if not stock:
        stock = pick(row, "vehicle_id") or key

      rows_out.append({
        "key": key,
        "vin": key if key_field == "vin" else pick(row, "vin"),
        "stock": stock,
        "year": year,
        "model": model,
        "trim": trim,
        "state_of_vehicle": state,
        "age_days": age_days,
        "sale_price_usd": "" if sale is None else round(sale, 2),
        "discount_usd": "" if discount <= 0 else discount,
        "url": url,
        "image_url": img,
        "first_seen_utc": first_seen[key],
      })

  # write OUT
  os.makedirs(os.path.dirname(OUT), exist_ok=True)
  fieldnames = list(rows_out[0].keys()) if rows_out else [
    "key","vin","stock","year","model","trim","state_of_vehicle","age_days",
    "sale_price_usd","discount_usd","url","image_url","first_seen_utc"
  ]
  with open(OUT, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows_out)

  # delta
  added = sorted(list(keys_now - set(prev.keys())))
  removed = sorted(list(set(prev.keys()) - keys_now))
  price_changed = []
  for k in (keys_now & set(prev.keys())):
    try:
      a = str(prev[k].get("sale_price_usd","")).strip()
      b = str(next(x for x in rows_out if x["key"]==k).get("sale_price_usd","")).strip()
      if a != b and a != "" and b != "":
        price_changed.append(k)
    except:
      pass

  save_json(DELTA, {
    "ts_utc": now.isoformat(),
    "added": added,
    "removed": removed,
    "price_changed": sorted(price_changed),
    "counts": {"now": len(keys_now), "prev": len(prev)}
  })

  save_json(META, {
    "ts_utc": now.isoformat(),
    "rows": len(rows_out),
    "source": INP,
    "out": OUT
  })

  save_json(STATE, first_seen)

if __name__ == "__main__":
  main()
