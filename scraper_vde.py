import re
import time
import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from requests.exceptions import RequestException
import ctypes

ctypes.windll.kernel32.SetThreadExecutionState(0x80000002)

BASE = "https://www.vde.com.mx"
CATALOGO_URL = BASE + "/productos.html"
HEADERS = {"User-Agent": "Mozilla/5.0"}

TOTAL_PAGINAS = 24
PRODUCTOS_POR_PAGINA = 250

CARPETA_BASE_IMAGENES = "imagenes_productos"
ARCHIVO_PROGRESO = "progreso_productos.json"

os.makedirs(CARPETA_BASE_IMAGENES, exist_ok=True)

# ----------------------------
# REQUEST SEGURO
# ----------------------------

def get_soup(url, reintentos=3):
    for intento in range(reintentos):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except RequestException:
            print(f"⚠️ Error en request. Reintentando ({intento+1}/{reintentos})...")
            time.sleep(10)
    raise Exception("Demasiados errores de conexión.")


# ----------------------------
# DESCARGA DE IMAGEN RESPETANDO RUTA ORIGINAL
# ----------------------------

def descargar_imagen(url):
    if not url:
        return ""

    parsed = urlparse(url)
    ruta_relativa = parsed.path.lstrip("/")  # media/catalog/product/...

    ruta_local = os.path.join(CARPETA_BASE_IMAGENES, ruta_relativa)
    carpeta_destino = os.path.dirname(ruta_local)

    os.makedirs(carpeta_destino, exist_ok=True)

    if os.path.exists(ruta_local):
        return ruta_local

    intentos = 0
    while intentos < 4:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                with open(ruta_local, "wb") as f:
                    f.write(r.content)
                return ruta_local
        except RequestException:
            print(f"⚠️ Error descargando imagen, intento {intentos+1}")

        intentos += 1

        if intentos < 3:
            time.sleep(10)
        elif intentos == 3:
            print("🧘 Esperando 30s antes del último intento...")
            time.sleep(30)
        else:
            print("❌ Imagen omitida:", url)
            return ""

# ----------------------------
# EXTRAER PRODUCTOS
# ----------------------------

def extraer_productos_de_pagina(page):
    url = f"{CATALOGO_URL}?p={page}&product_list_limit={PRODUCTOS_POR_PAGINA}"
    print(f"\n📄 Descargando página {page}...")
    soup = get_soup(url)

    productos = []
    items = soup.select("li.product-item")

    for p in items:
        nombre_tag = p.select_one("a.product-item-link")
        nombre = nombre_tag.get_text(strip=True) if nombre_tag else ""

        link_tag = p.select_one("a.product.photo.product-item-photo")
        url_prod = link_tag["href"] if link_tag else ""

        img_tag = p.select_one("img.product-image-photo")
        imagen = ""
        if img_tag:
            imagen = img_tag.get("data-amsrc") or img_tag.get("src") or ""
            if imagen.startswith("/"):
                imagen = BASE + imagen

        texto = p.get_text(" ", strip=True)
        sku_match = re.search(r"SKU:\s*([A-Z0-9\-]+)", texto)
        sku = sku_match.group(1) if sku_match else f"SIN-SKU-{page}-{len(productos)}"

        productos.append({
            "nombre": nombre,
            "sku": sku,
            "url": url_prod,
            "imagen": imagen,
            "descripcion_corta": texto[:300],
            "imagen_local": ""
        })

    print(f"  → {len(productos)} productos encontrados")
    return productos

# ----------------------------
# CARGAR O CREAR PROGRESO
# ----------------------------

if os.path.exists(ARCHIVO_PROGRESO):
    with open(ARCHIVO_PROGRESO, "r", encoding="utf-8") as f:
        productos = json.load(f)
    print(f"🔄 Reanudando desde progreso guardado ({len(productos)} productos)")
else:
    productos = []
    for p in range(1, TOTAL_PAGINAS + 1):
        productos.extend(extraer_productos_de_pagina(p))
        time.sleep(5)

    df_temp = pd.DataFrame(productos)
    df_temp.drop_duplicates(subset="url", inplace=True)
    productos = df_temp.to_dict("records")

    with open(ARCHIVO_PROGRESO, "w", encoding="utf-8") as f:
        json.dump(productos, f, ensure_ascii=False, indent=2)

print(f"🧾 Total productos: {len(productos)}")

# ----------------------------
# DESCARGA MASIVA DE IMÁGENES
# ----------------------------

for i, prod in enumerate(productos):

    if prod.get("imagen_local") and os.path.exists(prod["imagen_local"]):
        continue

    print(f"[{i+1}/{len(productos)}] Descargando imagen de {prod['sku']}")

    try:
        ruta = descargar_imagen(prod["imagen"])
        prod["imagen_local"] = ruta

        if i % 20 == 0:
            with open(ARCHIVO_PROGRESO, "w", encoding="utf-8") as f:
                json.dump(productos, f, ensure_ascii=False, indent=2)

        if i % 40 == 0 and i != 0:
            print("😴 Pausa larga para no saturar el servidor...")
            time.sleep(25)

        time.sleep(2)

    except Exception as e:
        print("🚨 Error grave, guardando progreso...")
        with open(ARCHIVO_PROGRESO, "w", encoding="utf-8") as f:
            json.dump(productos, f, ensure_ascii=False, indent=2)
        raise e

# ----------------------------
# EXPORTAR
# ----------------------------

df = pd.DataFrame(productos)
df.to_excel("productos_vde.xlsx", index=False)
df.to_json("productos_vde.json", orient="records", force_ascii=False, indent=2)

print("\n✅ DESCARGA COMPLETA")
print("📄 productos_vde.xlsx")
print("📄 productos_vde.json")
print("🖼 Imágenes guardadas respetando estructura original")
