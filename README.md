# SHAHARSOZ

**EN:** *SHAHARSOZ* means **"city builder / urban planner"** in Uzbek. This CLI is mainly for generating 3D city blockouts for architecture and urbanist purposes.

**UZ:** *SHAHARSOZ* so'zi o'zbek tilida **"shahar quruvchi / urbanist rejalovchi"** ma'nosini anglatadi. Ushbu CLI asosan arxitektura va urbanistika maqsadlari uchun 3D shahar blockout modellarini yaratish uchun mo'ljallangan.

## Collaboration Note / Hamkorlik Eslatmasi

**EN:** If you use this app, I would appreciate if you mention and credit me. This is optional, not mandatory.

**UZ:** Agar ushbu ilovadan foydalansangiz, meni eslatib, kredit berishingizdan xursand bo'laman. Bu ixtiyoriy, majburiy emas.

## Features / Imkoniyatlar

**EN:**
- OSM extraction: buildings, roads, railways, bridges, water, tree candidates
- DEM terrain mesh generation
- Sentinel-2 NDVI vegetation polygons
- Sentinel NDWI water polygons (merged with OSM water)
- 3D meshes: terrain/buildings/roads/railways/bridges/water/trees
- Textured terrain OBJ and textured blockout OBJ/MTL
- LOD and collision mesh export

**UZ:**
- OSM qatlamlari: bino, yo'l, temiryo'l, ko'prik, suv, daraxt manbalari
- DEM asosida relyef meshi
- Sentinel-2 NDVI asosida vegetatsiya poligonlari
- Sentinel NDWI asosida suv poligonlari (OSM suv bilan birlashtiriladi)
- 3D meshlar: terrain/bino/yo'l/temiryo'l/ko'prik/suv/daraxt
- Teksturalangan terrain OBJ va blockout OBJ/MTL
- LOD va collision mesh eksporti

## Install / O'rnatish

### 1) Requirements / Talablar

- macOS/Linux
- Python 3.12 (recommended / tavsiya etiladi)

### 2) Create virtual environment / Virtual muhit yaratish

```bash
cd /path/to/SHAHARSOZ
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

### 3) Install dependencies / Kutubxonalarni o'rnatish

```bash
pip install -r requirements.txt
```

## Quick Start / Tez Boshlash

```bash
python shaharsoz.py --lat 41.3111 --lon 69.2797 --radius 4 --output tashkent_model
```

## Version Control / Versiya Nazorati

**EN:**
- App version is stored in `VERSION` (example: `0.1.0`).
- Git repository is initialized (`main` branch) for clean publishing.

**UZ:**
- Ilova versiyasi `VERSION` faylida saqlanadi (masalan: `0.1.0`).
- Toza nashr qilish uchun Git repozitoriy (`main` branch) ishga tushirilgan.

## Input Files / Kiruvchi Fayllar

### AOI screenshot texture (optional) / Hudud skrinshot teksturasi (ixtiyoriy)

```text
input/<output_name>.(png|jpg|jpeg|webp|tif|tiff)
```

Example / Misol:

```text
input/tashkent_model.png
```

### Material texture pack / Material tekstura to'plami

```text
input/materials/
```

Expected stems / Kutiladigan nomlar:
- `asphalt`
- `cobblestone`
- `concrete`
- `grass`
- `railway`
- `roof`
- `stone`
- `wall_window`
- `water`
- `tree`

## Main CLI Options / Asosiy CLI Parametrlari

- `--lat`, `--lon`, `--radius`
- `--output`
- `--dem-resolution`
- `--basemap-resolution`
- `--wayback-layer-id`
- `--input-dir`
- `--material-dir`
- `--tree-density`
- `--max-trees`
- `--ndwi-threshold`
- `--no-input-texture`
- `--verbose`
- `--version`

## Output Structure / Chiqish Tuzilmasi

`output/<name>/`:
- `meshes/` (OBJ meshlar)
- `textures/` (terrain texture PNG)
- `materials/` (MTL ishlatadigan material rasmlar)
- `vectors/` (GeoJSON qatlamlar)
- `lod/` (LOD OBJlar)
- `collision/` (collision OBJ)

`data/<name>/`:
- `dem/`
- `imagery/`
- `masks/`
- `cache/`

## Expected Quality / Natija Sifati

**EN:** Good for architectural and urbanist blockout, previs, and gameplay prototyping. Not photogrammetry-level reconstruction.

**UZ:** Arxitektura/urbanistika blockout, previs va gameplay prototiplash uchun yaxshi. Fotogrammetriya darajasidagi aniq rekonstruksiya emas.

## License / Litsenziya

- Code / Kod: MIT (`LICENSE`)
- Data usage / Ma'lumotlar: third-party terms apply (`DATA_AND_LEGAL_NOTICE.md`)
