import json
import time
import re
import pandas as pd
from bs4 import BeautifulSoup
import requests
from requests.exceptions import RequestException

# Configuración
BASE = "https://www.vde.com.mx"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
PRODUCTOS_POR_PAGINA = 250 # Optimizado para hacer menos peticiones

# --- REEMPLAZA ESTO CON TU LISTA COMPLETA DE CATEGORÍAS ---
categorias_input = [
    {"categoria": "Equipos sumergibles", "url": "https://www.vde.com.mx/productos.html?category_ids=4"},
    {"categoria": "Bombas de superficie", "url": "https://www.vde.com.mx/productos.html?category_ids=39"},
    {"categoria": "Arrancadores, tableros, accesorios y protecciones", "url": "https://www.vde.com.mx/productos.html?category_ids=1262"},
    {"categoria": "Generadores", "url": "https://www.vde.com.mx/productos.html?category_ids=1378"},
    {"categoria": "Tanques precargados", "url": "https://www.vde.com.mx/productos.html?category_ids=55"},
    {"categoria": "Equipos y accesorios para piscina, spa y fuentes", "url": "https://www.vde.com.mx/productos.html?category_ids=59"},
    {"categoria": "Motobombas sumergibles para aguas residuales", "url": "https://www.vde.com.mx/productos.html?category_ids=5"},
    {"categoria": "Equipos presurizadores", "url": "https://www.vde.com.mx/productos.html?category_ids=75"},
    {"categoria": "Energía renovable", "url": "https://www.vde.com.mx/productos.html?category_ids=79"},
    {"categoria": "Tratamiento de agua", "url": "https://www.vde.com.mx/productos.html?category_ids=86"},
    {"categoria": "Calentadores de agua", "url": "https://www.vde.com.mx/productos.html?category_ids=98"},
    {"categoria": "Refacciones", "url": "https://www.vde.com.mx/productos.html?category_ids=102"}
]

def get_soup(url, reintentos=3):
    for intento in range(reintentos):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:
            print(f"⚠️ Error: {e}. Reintentando ({intento+1}/{reintentos})...")
            time.sleep(5)
    return None

def extraer_skus_de_categoria(url_base):
    skus_encontrados = set()
    pagina = 1
    
    while True:
        # Forzamos el límite de 250 para ser eficientes
        url_paginada = f"{url_base}&p={pagina}&product_list_limit={PRODUCTOS_POR_PAGINA}"
        print(f"   🔍 Analizando página {pagina}...")
        
        soup = get_soup(url_paginada)
        if not soup: break
        
        items = soup.select("li.product-item")
        if not items: break
            
        for p in items:
            sku = None
            
            # 1. Intentar obtener el SKU de atributos de Magento (Más confiable)
            # Buscamos en el div de precio o botones que suelen tener data-product-sku
            element_with_sku = p.select_one('[data-product-sku]')
            if element_with_sku:
                sku = element_with_sku['data-product-sku']
            
            # 2. Si no, buscar en inputs ocultos o botones de "comparar"
            if not sku:
                compare_button = p.select_one('a.action.tocompare')
                if compare_button and 'data-post' in compare_button.attrs:
                    # El SKU suele venir dentro de un JSON en data-post
                    match = re.search(r'"product":"(\d+)"', compare_button['data-post'])
                    # Si no es el SKU alfanumérico, buscamos en el texto
            
            # 3. Recurso final: Texto visible (tu método original)
            if not sku:
                texto = p.get_text(" ", strip=True)
                sku_match = re.search(r"SKU:\s*([A-Z0-9\-]+)", texto)
                if sku_match:
                    sku = sku_match.group(1)

            if sku:
                skus_encontrados.add(sku.strip())
        
        # Verificamos si hay botón "Next"
        if not soup.select_one("a.action.next"):
            break
            
        pagina += 1
        time.sleep(1) # Pausa breve para no ser bloqueados
        
    return skus_encontrados

# --- INICIO DEL PROCESO ---

try:
    with open("productos_vde.json", "r", encoding="utf-8") as f:
        lista_productos = json.load(f)
except FileNotFoundError:
    print("❌ Error: No se encontró productos_vde.json")
    exit()

mapa_sku_categoria = {}

for cat in categorias_input:
    nombre_cat = cat["categoria"]
    print(f"\n📂 Categoría: {nombre_cat}")
    skus = extraer_skus_de_categoria(cat["url"])
    print(f"✅ Se encontraron {len(skus)} productos únicos en esta categoría.")
    
    for s in skus:
        if s in mapa_sku_categoria:
            mapa_sku_categoria[s] += f" | {nombre_cat}"
        else:
            mapa_sku_categoria[s] = nombre_cat

# Inyectar al JSON
print("\n🔄 Cruzando datos...")
total_actualizados = 0
for prod in lista_productos:
    sku_actual = prod.get("sku")
    if sku_actual in mapa_sku_categoria:
        prod["categoria"] = mapa_sku_categoria[sku_actual]
        total_actualizados += 1
    else:
        prod["categoria"] = "Sin Categoría"

# Guardar
with open("productos_vde_categorizados.json", "w", encoding="utf-8") as f:
    json.dump(lista_productos, f, ensure_ascii=False, indent=2)

df = pd.DataFrame(lista_productos)
df.to_excel("productos_vde_categorizados.xlsx", index=False)

print(f"\n✨ ¡Listo! Se asignó categoría a {total_actualizados} productos.")