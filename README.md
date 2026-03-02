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

### From project folder (recommended)

```bash
cd /path/to/SHAHARSOZ
./shaharsoz --lat 41.3111 --lon 69.2797 --radius 4 --output tashkent_model
```

If you run `./shaharsoz` without arguments in terminal, SHAHARSOZ opens interactive prompts.

### Using Python directly

```bash
cd /path/to/SHAHARSOZ
source .venv/bin/activate
python shaharsoz.py --lat 41.3111 --lon 69.2797 --radius 4 --output tashkent_model
```

### Use `shaharsoz` globally (optional)

```bash
echo 'export PATH="/path/to/SHAHARSOZ/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
shaharsoz --lat 41.3111 --lon 69.2797 --radius 4 --output tashkent_model
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

- `--wayback-layer-id` (default `0` = current Esri World Imagery; set a Wayback id to target snapshot)
- `--sentinel-upscale` (default `2`, upscales Sentinel preview fallback)
- `--material-dir` (default `input/materials`)
- `--ndwi-threshold`
- `--tree-density`, `--max-trees`
- `--terrain-osm-overlay` (optional: burns roads/buildings into terrain texture; off by default for cleaner satellite look)

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

### 4) Why does Wayback return 404?

This is common when a specific Wayback layer id is not available for your exact AOI/date/zoom.
SHAHARSOZ now automatically falls back:
1. Wayback requested size retries (4096 -> 2048 -> 1024)
2. Esri World Imagery retries
3. Sentinel fallback texture if Esri fails

If needed, force current Esri imagery (skip Wayback):

```bash
./shaharsoz ... --wayback-layer-id 0
```

### 5) `basemap_rgb_3857.tif` missing

It should be written to:

```text
data/<model>/imagery/basemap_rgb_3857.tif
```

If Esri fails, SHAHARSOZ creates it from Sentinel fallback.

### 6) `materials`, `textures`, `vectors`, `lod`, `collision` folders look empty

Most common reasons:
- Run failed before export (check first ERROR in logs).
- Wrong folder checked (`output/<model>/...`).
- No venv/dependencies active.

### 7) I did not put any materials in `input/materials`

Allowed. App still exports textured OBJ/MTL using available/fallback textures.
Better quality needs material files:
`asphalt, cobblestone, concrete, grass, railway, roof, stone, wall_window, water, tree`.

### 8) `terrain_textured.obj` looks black or untextured

Check:
1. `output/<model>/textures/terrain_texture.png` exists
2. `output/<model>/meshes/terrain_textured.mtl` has `map_Kd ../textures/terrain_texture.png`
3. Viewer supports relative MTL texture paths

Tip:
- Keep default mode for clean terrain texture.
- Use `--terrain-osm-overlay` only if you want roads/buildings burned into terrain texture.

## Uninstall / O'chirish

To uninstall SHAHARSOZ:

```bash
rm -rf /path/to/SHAHARSOZ
```

## Free up space safely / Joy bo'shatish (xavfsiz)

Safe to delete (won't break source code):

```bash
rm -rf output/*
rm -rf data/*
rm -rf cache/*
find . -name '__pycache__' -type d -prune -exec rm -rf {} +
```

Do **not** delete if you still need app code/config:
- `shaharsoz`
- `shaharsoz.py`
- `utils/`
- `requirements.txt`
- `README.md`
- `LICENSE`, `DATA_AND_LEGAL_NOTICE.md`, `VERSION`

## License / Litsenziya

- Code: SHAHARSOZ Non-Commercial License v1.0 (`LICENSE`)
- Data usage rights vary by provider (`DATA_AND_LEGAL_NOTICE.md`)
