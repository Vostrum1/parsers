import time
import re
import random
import urllib3
import psycopg2
from bs4 import BeautifulSoup
from curl_cffi import requests
from db_config import DB_CONFIG

urllib3.disable_warnings()

APPLE_PRODUCTS = [
    "iPhone 15", "iPhone 16 Pro", "MacBook Air M3",
    "iPad Pro M4", "AirPods Pro 2", "Apple Watch Ultra 2"
]

PARSERS_CONFIG = {
    "Allo":     {"url": "https://allo.ua/ua/catalogsearch/result/?q={}", "base": "https://allo.ua"},
    "Rozetka":  {"url": "https://rozetka.com.ua/ua/search/?text={}", "base": "https://rozetka.com.ua"},
    "Comfy":    {"url": "https://comfy.ua/ua/search/?q={}", "base": "https://comfy.ua"},
    "Citrus":   {"url": "https://www.citrus.ua/search/?q={}", "base": "https://www.citrus.ua"},
    "Moyo":     {"url": "https://www.moyo.ua/search/?query={}", "base": "https://www.moyo.ua"},
    "Foxtrot":  {"url": "https://www.foxtrot.com.ua/uk/search?q={}", "base": "https://www.foxtrot.com.ua"},
    "Eldorado": {"url": "https://eldorado.ua/uk/search/?q={}", "base": "https://eldorado.ua"},
    "iStore":   {"url": "https://istore.ua/ua/search/?q={}", "base": "https://istore.ua"},
    "Epicentr": {"url": "https://epicentrk.ua/ua/search/?q={}", "base": "https://epicentrk.ua"},
    "Brain":    {"url": "https://brain.com.ua/search/?query={}", "base": "https://brain.com.ua"},
    "Stylus":   {"url": "https://stylus.ua/uk/search?q={}", "base": "https://stylus.ua"},
    "Telemart": {"url": "https://telemart.ua/ua/search/?q={}", "base": "https://telemart.ua"},
    "MTA":      {"url": "https://mta.ua/search?search={}", "base": "https://mta.ua"},
    "iSpace":   {"url": "https://ispace.ua/search?q={}", "base": "https://ispace.ua"},
    "Omega":    {"url": "https://omega.ua/search?q={}", "base": "https://omega.ua"},
    "iOn":      {"url": "https://ion.ua/search?q={}", "base": "https://ion.ua"},
    "Notebook": {"url": "https://notebook.ua/search?q={}", "base": "https://notebook.ua"},
    "Tehno":    {"url": "https://tehno.com/search?q={}", "base": "https://tehno.com"},
    "Hotline":  {"url": "https://hotline.ua/sr/?q={}", "base": "https://hotline.ua"},
    "Price":    {"url": "https://price.ua/search?search={}", "base": "https://price.ua"},
}

def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS apple_v3 (
            id SERIAL PRIMARY KEY,
            shop TEXT,
            product TEXT,
            price_uah BIGINT,
            price_text TEXT,
            url TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def extract_price(text):
    try:
        clean = re.sub(r'[^\d]', '', text)
        nums = re.findall(r'\d{4,7}', clean)
        return int(nums[0]) if nums else 0
    except:
        return 0

def fetch_advanced(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "uk-UA,uk;q=0.9",
        "Referer": "https://www.google.com/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        r = requests.get(url, headers=headers, impersonate="chrome120", timeout=25, verify=False)
        if r.status_code in [403, 503]:
            return None, "blocked"
        if len(r.text) < 500:
            return None, "empty"
        return r.text, "ok"
    except Exception as e:
        return None, str(e)

def parse_items(html, shop_name, product, base_url):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.find_all(["div","li","article"], class_=re.compile(r"product|card|item|goods|result", re.I)):
        try:
            price_tag = card.find(class_=re.compile(r"price|cost", re.I))
            link_tag = card.find("a", href=True)
            if not price_tag or not link_tag:
                continue
            price_uah = extract_price(price_tag.text)
            if price_uah < 1000:
                continue
            href = link_tag["href"]
            if not href.startswith("http"):
                href = base_url + href
            items.append({"shop":shop_name,"product":product,"price_uah":price_uah,"price_text":price_tag.text.strip(),"url":href})
        except:
            continue
    return items

def save_to_db(items):
    if not items:
        return 0
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    count = 0
    for item in items:
        try:
            cur.execute("""
                INSERT INTO apple_v3 (shop,product,price_uah,price_text,url)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (url) DO NOTHING
            """, (item["shop"],item["product"],item["price_uah"],item["price_text"],item["url"]))
            count += 1
        except:
            continue
    conn.commit()
    cur.close()
    conn.close()
    return count

def main():
    create_table()
    total = 0
    for product in APPLE_PRODUCTS:
        for shop, cfg in PARSERS_CONFIG.items():
            url = cfg["url"].format(product.replace(" ","+"))
            print(f"[{shop}] {product}...")
            html, status = fetch_advanced(url)
            if status == "ok":
                items = parse_items(html, shop, product, cfg["base"])
                saved = save_to_db(items)
                total += saved
                print(f"  OK: +{saved} | Всього: {total}")
            else:
                print(f"  ВІДМОВА: {status}")
            time.sleep(random.uniform(2.0, 4.0))
        print(f"--- {product} готово ---")
    print(f"\nВсього записів: {total}")

if __name__ == "__main__":
    main()
