# SHAHARSOZ

**EN:** *SHAHARSOZ* means **"city builder / urban planner"** in Uzbek. SHAHARSOZ generates 3D city blockouts mainly for architecture and urbanist workflows.

**UZ:** *SHAHARSOZ* o'zbek tilida **"shahar quruvchi / urban rejalovchi"** degani. SHAHARSOZ asosan arxitektura va urbanistika uchun 3D shahar blockout yaratadi.

## Collaboration Note / Hamkorlik Eslatmasi

**EN:** If you use this app, I would appreciate if you mention and credit me. This is optional, not mandatory.

**UZ:** Ushbu ilovadan foydalansangiz, meni eslatib/kredit bersangiz xursand bo'laman. Bu ixtiyoriy, majburiy emas.

## Install / O'rnatish

```bash
cd /path/to/SHAHARSOZ
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

## Run / Ishga tushirish

```bash
cd /path/to/SHAHARSOZ
source .venv/bin/activate
python shaharsoz.py --lat 41.3111 --lon 69.2797 --radius 4 --output tashkent_model
```

Or launcher:

```bash
/path/to/SHAHARSOZ/bin/shaharsoz --lat 41.3111 --lon 69.2797 --radius 4 --output tashkent_model
```

## Output Structure / Chiqish Tuzilmasi

`output/<model>/`:
- `meshes/` (`blockout.obj`, `blockout_textured.obj`, `blockout_textured.mtl`, etc.)
- `textures/` (terrain textures)
- `materials/` (copied material images used by MTL)
- `vectors/` (GeoJSON)
- `lod/` (LOD meshes)
- `collision/` (collision mesh)

`data/<model>/`:
- `dem/`
- `imagery/` (`basemap_rgb_3857.tif`, Sentinel rasters)
- `masks/` (NDVI/NDWI + previews)
- `cache/`

## Important Flags / Muhim Parametrlar

- `--wayback-layer-id` (default `64001`, use `0` for current Esri World Imagery)
- `--sentinel-upscale` (default `2`, upscales Sentinel preview fallback)
- `--material-dir` (default `input/materials`)
- `--ndwi-threshold`
- `--tree-density`, `--max-trees`

## Troubleshooting / Muammolarni hal qilish

### 1) `zsh: command not found: python`

Use `python3` and activate venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -V
```

### 2) `ModuleNotFoundError: geopandas`

Dependencies are not installed in active interpreter:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) `blockout_textured.obj` / `.mtl` not found

They are in:

```text
output/<model>/meshes/blockout_textured.obj
output/<model>/meshes/blockout_textured.mtl
```

### 4) `basemap_rgb_3857.tif` missing

If Wayback/current basemap fails, SHAHARSOZ now falls back to Sentinel RGB and writes:

```text
data/<model>/imagery/basemap_rgb_3857.tif
```

### 5) `materials`, `textures`, `vectors`, `lod`, `collision` folders look empty

Most common reasons:
- Run failed before export (check terminal log for first ERROR).
- Wrong folder checked (`output/<model>/...` is required).
- No venv/dependencies active.
- Input materials missing: app still exports OBJ/MTL, but material images may be limited.

### 6) I did not put any materials in `input/materials`

That is allowed. App still exports textured OBJ/MTL. Best quality needs material pack with stems:
`asphalt, cobblestone, concrete, grass, railway, roof, stone, wall_window, water, tree`.

## Uninstall / O'chirish

To uninstall SHAHARSOZ from this computer:

```bash
rm -rf /path/to/SHAHARSOZ
```

If installed elsewhere, remove that folder path.

## Free up space safely / Joy bo'shatish (xavfsiz)

Safe to delete (won't break source code):

```bash
rm -rf output/*
rm -rf data/*
rm -rf cache/*
find . -name '__pycache__' -type d -prune -exec rm -rf {} +
```

Do **not** delete if you still need app code/config:
- `shaharsoz.py`
- `utils/`
- `requirements.txt`
- `README.md`
- `LICENSE`, `DATA_AND_LEGAL_NOTICE.md`, `VERSION`

## License / Litsenziya

- Code: MIT (`LICENSE`)
- Data usage rights vary by provider (`DATA_AND_LEGAL_NOTICE.md`)
