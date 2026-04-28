import requests
from bs4 import BeautifulSoup
import psycopg2
from db_config import DB_CONFIG
import time
import re

APPLE_PRODUCTS = [
    "iPhone 12","iPhone 12 Pro","iPhone 12 Pro Max","iPhone 12 mini",
    "iPhone 13","iPhone 13 Pro","iPhone 13 Pro Max","iPhone 13 mini",
    "iPhone 14","iPhone 14 Pro","iPhone 14 Pro Max","iPhone 14 Plus",
    "iPhone 15","iPhone 15 Pro","iPhone 15 Pro Max","iPhone 15 Plus",
    "iPhone 16","iPhone 16 Pro","iPhone 16 Pro Max","iPhone 16 Plus",
    "MacBook Air M1","MacBook Air M2","MacBook Air M3",
    "MacBook Pro 14 M2","MacBook Pro 14 M3","MacBook Pro 16 M2","MacBook Pro 16 M3",
    "iPad Pro M4","iPad Air M2","iPad mini 6","iPad mini 7",
    "AirPods 3","AirPods 4","AirPods Pro 2","AirPods Max",
    "Apple Watch Series 8","Apple Watch Series 9","Apple Watch Series 10",
    "Apple Watch Ultra 2","HomePod mini","HomePod 2",
]

SHOPS = {
    "Rozetka":"https://rozetka.com.ua/ua/search/?text={}",
    "Comfy":"https://comfy.ua/ua/search/?q={}",
    "Allo":"https://allo.ua/ua/catalogsearch/result/?q={}",
    "Moyo":"https://www.moyo.ua/search/?query={}",
    "Citrus":"https://www.citrus.ua/search/?q={}",
    "iOn":"https://ion.ua/search?q={}",
    "Brain":"https://brain.com.ua/search/?query={}",
    "Foxtrot":"https://www.foxtrot.com.ua/uk/search?q={}",
    "Eldorado":"https://eldorado.ua/uk/search/?q={}",
    "iStore":"https://istore.ua/ua/search/?q={}",
    "Stylus":"https://stylus.ua/search/?q={}",
    "Telemart":"https://telemart.ua/ua/search/?q={}",
    "MTA":"https://mta.ua/search?q={}",
    "Omega":"https://omega.ua/search?q={}",
    "Notebook":"https://notebook.ua/search?q={}",
    "Tehno":"https://tehno.com/search?q={}",
    "iSpace":"https://ispace.ua/search?q={}",
    "ReStore":"https://re-store.ua/search/?q={}",
    "Epicentr":"https://epicentrk.ua/ua/search/?q={}",
    "iTalents":"https://italents.ua/search?q={}",
}

report = {"success":0,"cloudflare":[],"error":[]}

def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS apple_ukraine (
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
    print("Таблиця створена")

def extract_price(text):
    try:
        clean = text.replace(" ","").replace("\xa0","").replace(",","")
        numbers = re.findall(r"\d{4,7}", clean)
        return int(numbers[0]) if numbers else 0
    except:
        return 0

def save_items(items):
    if not items:
        return 0
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    count = 0
    for item in items:
        try:
            cur.execute("""
                INSERT INTO apple_ukraine (shop, product, price_uah, price_text, url)
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

def fetch(shop_name, url):
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 403 or "cloudflare" in r.text.lower() or "checking your browser" in r.text.lower():
            if shop_name not in report["cloudflare"]:
                report["cloudflare"].append(shop_name)
            return None
        if r.status_code != 200:
            if shop_name not in report["error"]:
                report["error"].append(shop_name)
            return None
        report["success"] += 1
        return r.text
    except:
        if shop_name not in report["error"]:
            report["error"].append(shop_name)
        return None

def parse_page(html, shop_name, product):
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for tag in soup.find_all(["div","li","article"], class_=re.compile(r"product|card|item|goods|result", re.I)):
        try:
            price_el = tag.find(class_=re.compile(r"price|cost", re.I))
            link_el = tag.find("a", href=True)
            if not price_el or not link_el:
                continue
            price_text = price_el.text.strip()
            price_uah = extract_price(price_text)
            if price_uah < 1000:
                continue
            href = link_el["href"]
            if not href.startswith("http"):
                base = "/".join(SHOPS[shop_name].format("").split("/")[:3])
                href = base + href
            items.append({"shop":shop_name,"product":product,"price_uah":price_uah,"price_text":price_text,"url":href})
        except:
            continue
    return items

def main():
    create_table()
    total = 0
    for product in APPLE_PRODUCTS:
        for shop_name, url_template in SHOPS.items():
            query = product.replace(" ","+")
            url = url_template.format(query)
            html = fetch(shop_name, url)
            items = parse_page(html, shop_name, product)
            saved = save_items(items)
            total += saved
            if saved > 0:
                print(f"OK {shop_name} | {product}: +{saved}")
            time.sleep(0.5)
        print(f"--- {product} | Всього: {total} ---")

    print(f"\n{'='*50}")
    print(f"ЗВІТ")
    print(f"Всього записів: {total}")
    print(f"Успішних запитів: {report['success']}")
    print(f"Cloudflare заблокував: {report['cloudflare']}")
    print(f"Інші помилки: {report['error']}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
