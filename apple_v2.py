import requests
from bs4 import BeautifulSoup
import psycopg2
from db_config import DB_CONFIG
import time
import re
import urllib3
urllib3.disable_warnings()

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

report = {"success":{},"blocked":[],"error":{}}

def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS apple_v2 (
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
        clean = re.sub(r'[^\d]','',text)
        nums = re.findall(r'\d{4,7}', clean)
        return int(nums[0]) if nums else 0
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
                INSERT INTO apple_v2 (shop,product,price_uah,price_text,url)
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

def fetch(url, verify=True):
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36","Accept-Language":"uk-UA,uk;q=0.9"}
    try:
        r = requests.get(url, headers=headers, timeout=15, verify=verify)
        if r.status_code in [403,503]:
            return None, "blocked"
        if len(r.text) < 500:
            return None, "empty"
        return r.text, "ok"
    except:
        return None, "error"

def parse_rozetka(html, product):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.find_all("li", class_=re.compile(r"goods-tile", re.I)):
        try:
            name = card.find("span", class_=re.compile(r"goods-tile__title"))
            price = card.find("span", class_=re.compile(r"goods-tile__price-value"))
            link = card.find("a", class_=re.compile(r"goods-tile__heading"))
            if not name or not price or not link:
                continue
            items.append({"shop":"Rozetka","product":product,"price_uah":extract_price(price.text),"price_text":price.text.strip(),"url":link["href"]})
        except:
            continue
    return items

def parse_allo(html, product):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.find_all("div", class_=re.compile(r"product-card", re.I)):
        try:
            name = card.find(class_=re.compile(r"product-card__title"))
            price = card.find(class_=re.compile(r"product-card__price|price__current"))
            link = card.find("a", href=True)
            if not price or not link:
                continue
            href = link["href"]
            if not href.startswith("http"):
                href = "https://allo.ua" + href
            items.append({"shop":"Allo","product":product,"price_uah":extract_price(price.text),"price_text":price.text.strip(),"url":href})
        except:
            continue
    return items

def parse_citrus(html, product):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.find_all("div", class_=re.compile(r"product|card", re.I)):
        try:
            price = card.find(class_=re.compile(r"price", re.I))
            link = card.find("a", href=True)
            if not price or not link:
                continue
            price_uah = extract_price(price.text)
            if price_uah < 1000:
                continue
            href = link["href"]
            if not href.startswith("http"):
                href = "https://www.citrus.ua" + href
            items.append({"shop":"Citrus","product":product,"price_uah":price_uah,"price_text":price.text.strip(),"url":href})
        except:
            continue
    return items

def parse_generic(html, product, shop_name, base_url):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.find_all(["div","li","article"], class_=re.compile(r"product|card|item|goods", re.I)):
        try:
            price = card.find(class_=re.compile(r"price|cost|грн", re.I))
            link = card.find("a", href=True)
            if not price or not link:
                continue
            price_uah = extract_price(price.text)
            if price_uah < 1000:
                continue
            href = link["href"]
            if not href.startswith("http"):
                href = base_url + href
            items.append({"shop":shop_name,"product":product,"price_uah":price_uah,"price_text":price.text.strip(),"url":href})
        except:
            continue
    return items

PARSERS = {
    "Rozetka": ("https://rozetka.com.ua/ua/search/?text={}", parse_rozetka, True),
    "Allo": ("https://allo.ua/ua/catalogsearch/result/?q={}", parse_allo, True),
    "Citrus": ("https://www.citrus.ua/search/?q={}", parse_citrus, True),
    "Foxtrot": ("https://www.foxtrot.com.ua/uk/search?q={}", lambda h,p: parse_generic(h,p,"Foxtrot","https://www.foxtrot.com.ua"), True),
    "Eldorado": ("https://eldorado.ua/uk/search/?q={}", lambda h,p: parse_generic(h,p,"Eldorado","https://eldorado.ua"), True),
    "iStore": ("https://istore.ua/ua/search/?q={}", lambda h,p: parse_generic(h,p,"iStore","https://istore.ua"), True),
    "Epicentr": ("https://epicentrk.ua/ua/search/?q={}", lambda h,p: parse_generic(h,p,"Epicentr","https://epicentrk.ua"), True),
    "iOn": ("https://ion.ua/search?q={}", lambda h,p: parse_generic(h,p,"iOn","https://ion.ua"), False),
}

def main():
    create_table()
    total = 0
    for product in APPLE_PRODUCTS:
        for shop_name, (url_tpl, parser, verify) in PARSERS.items():
            url = url_tpl.format(product.replace(" ","+"))
            html, status = fetch(url, verify=verify)
            if status == "blocked":
                if shop_name not in report["blocked"]:
                    report["blocked"].append(shop_name)
                continue
            if not html:
                report["error"][shop_name] = status
                continue
            items = parser(html, product)
            saved = save_items(items)
            total += saved
            if saved > 0:
                print(f"OK {shop_name} | {product}: +{saved} | Всього: {total}")
            time.sleep(0.5)
        print(f"--- {product} готово ---")

    print(f"\n{'='*50}")
    print(f"Всього записів: {total}")
    print(f"Заблоковані: {report['blocked']}")
    print(f"Помилки: {list(report['error'].keys())}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
