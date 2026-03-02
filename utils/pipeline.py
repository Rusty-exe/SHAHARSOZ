#!/usr/bin/env python3
"""SHAHARSOZ pipeline: automated 3D city reconstruction from coordinates."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable, Tuple

import geopandas as gpd
import numpy as np
import osmnx as ox
import pyvista as pv
import rasterio
from rasterio import features
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.transform import from_bounds
from rasterio.warp import reproject
from shapely.geometry import Point, mapping, shape
from shapely.ops import triangulate

TARGET_CRS = "EPSG:3857"
DEM_TILE_ZOOM = 12
DEM_TILE_URL = "https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{z}/{x}/{y}.tif"
STAC_SEARCH_URL = "https://earth-search.aws.element84.com/v1/search"
USGS_3DEP_EXPORT_URL = "https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/exportImage"
ESRI_WORLD_IMAGERY_EXPORT_URL = "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export"
ESRI_WAYBACK_EXPORT_URL = "https://wayback.maptiles.arcgis.com/arcgis/rest/services/world_imagery/MapServer/export"
DEFAULT_BUILDING_LEVEL_HEIGHT_M = 3.0
DEM_NODATA_FALLBACK_THRESHOLD = -10000.0
DEFAULT_BRIDGE_CLEARANCE_M = 6.0
DEFAULT_BRIDGE_DECK_THICKNESS_M = 1.0


class ShaharsozError(RuntimeError):
    """Raised for recoverable SHAHARSOZ pipeline failures."""


def get_project_version() -> str:
    """Return SHAHARSOZ project version from VERSION file."""
    version_file = Path(__file__).resolve().parents[1] / "VERSION"
    if version_file.exists():
        return version_file.read_text(encoding="utf-8").strip() or "0.1.0"
    return "0.1.0"


def configure_logging(verbose: bool = False) -> None:
    """Configure root logger with production-friendly formatting."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _radius_geometry(lat: float, lon: float, radius_km: float) -> Tuple[Point, gpd.GeoSeries, gpd.GeoSeries]:
    """Create center point and circle in both WGS84 and Web Mercator."""
    center_wgs84 = Point(lon, lat)
    center_series = gpd.GeoSeries([center_wgs84], crs="EPSG:4326")
    center_3857 = center_series.to_crs(TARGET_CRS)
    circle_3857 = center_3857.buffer(radius_km * 1000.0)
    circle_wgs84 = circle_3857.to_crs("EPSG:4326")
    return center_wgs84, circle_wgs84, circle_3857


def _parse_height_m(height_value: object, levels_value: object) -> float:
    """Parse building height tags into meters with robust defaults."""
    if height_value is not None and not (isinstance(height_value, float) and math.isnan(height_value)):
        text = str(height_value).strip().lower()
        match = re.search(r"[-+]?\d*\.?\d+", text)
        if match:
            value = float(match.group(0))
            if "ft" in text or "feet" in text:
                return value * 0.3048
            return value

    if levels_value is not None and not (isinstance(levels_value, float) and math.isnan(levels_value)):
        match = re.search(r"\d+", str(levels_value))
        if match:
            return float(match.group(0)) * DEFAULT_BUILDING_LEVEL_HEIGHT_M

    return DEFAULT_BUILDING_LEVEL_HEIGHT_M


def _latlon_to_tile(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
    """Convert latitude/longitude to slippy tile index."""
    lat_rad = math.radians(lat)
    n = 2 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return xtile, ytile


def _tile_range(bounds_wgs84: Tuple[float, float, float, float], zoom: int) -> Iterable[Tuple[int, int]]:
    """Yield all slippy tiles intersecting a WGS84 bbox."""
    min_lon, min_lat, max_lon, max_lat = bounds_wgs84
    x_min, y_max = _latlon_to_tile(max_lat, min_lon, zoom)
    x_max, y_min = _latlon_to_tile(min_lat, max_lon, zoom)

    for x in range(min(x_min, x_max), max(x_min, x_max) + 1):
        for y in range(min(y_min, y_max), max(y_min, y_max) + 1):
            yield x, y


def _fetch_bytes(url: str, timeout: int = 90) -> bytes:
    """Download binary payload with explicit timeout and wrapped errors."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.read()
    except urllib.error.URLError as exc:
        raise ShaharsozError(f"Network request failed: {url} ({exc})") from exc


def _clip_raster_to_circle(src_path: Path, clip_geometry_3857, dst_path: Path) -> Path:
    """Clip raster to circular AOI geometry."""
    with rasterio.open(src_path) as src:
        clipped_data, clipped_transform = mask(src, [mapping(clip_geometry_3857)], crop=True)
        clipped_meta = src.meta.copy()

    clipped_meta.update(
        {
            "height": clipped_data.shape[1],
            "width": clipped_data.shape[2],
            "transform": clipped_transform,
            "count": clipped_data.shape[0],
        }
    )

    with rasterio.open(dst_path, "w", **clipped_meta) as dst:
        dst.write(clipped_data)
    return dst_path


def _arcgis_export_url(
    base_url: str,
    bounds_3857: Tuple[float, float, float, float],
    width: int,
    height: int,
    image_format: str,
    extra_params: dict | None = None,
) -> str:
    """Build ArcGIS export URL for georeferenced bbox request."""
    minx, miny, maxx, maxy = bounds_3857
    params = {
        "bbox": f"{minx},{miny},{maxx},{maxy}",
        "bboxSR": 3857,
        "imageSR": 3857,
        "size": f"{width},{height}",
        "format": image_format,
        "f": "image",
    }
    if extra_params:
        params.update(extra_params)
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def _download_arcgis_raster_to_geotiff(
    url: str,
    dst_path: Path,
    bounds_3857: Tuple[float, float, float, float],
    dtype: str,
    count: int,
    nodata,
) -> Path:
    """Download ArcGIS image export and write as georeferenced GeoTIFF."""
    payload = _fetch_bytes(url)
    with MemoryFile(payload) as memfile:
        with memfile.open() as src:
            data = src.read()
            if data.shape[0] < count:
                raise ShaharsozError(f"Downloaded raster has {data.shape[0]} bands, expected at least {count}")
            if count == 1:
                data = data[:1]
            else:
                data = data[:count]
            height, width = data.shape[1], data.shape[2]

    minx, miny, maxx, maxy = bounds_3857
    transform = from_bounds(minx, miny, maxx, maxy, width, height)
    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": count,
        "dtype": dtype,
        "crs": TARGET_CRS,
        "transform": transform,
        "nodata": nodata,
        "compress": "lzw",
    }

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(dst_path, "w", **profile) as dst:
        dst.write(data.astype(dtype))
    return dst_path


def _sanitize_gtiff_profile(profile: dict) -> dict:
    """Drop incompatible inherited creation options before GeoTIFF writes."""
    cleaned = dict(profile)
    for key in (
        "blockxsize",
        "blockysize",
        "tiled",
        "interleave",
        "photometric",
        "compress",
        "predictor",
        "num_threads",
    ):
        cleaned.pop(key, None)
    cleaned["driver"] = "GTiff"
    cleaned["compress"] = "lzw"
    return cleaned


def get_osm_data(
    lat: float, lon: float, radius_km: float
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Download and normalize OSM buildings, roads, waterways/waterbodies, and railways."""
    log = logging.getLogger("shaharsoz.osm")
    log.info("Downloading OSM features")

    buildings = ox.features_from_point((lat, lon), tags={"building": True}, dist=radius_km * 1000.0)
    roads = ox.features_from_point((lat, lon), tags={"highway": True}, dist=radius_km * 1000.0)
    railways = ox.features_from_point((lat, lon), tags={"railway": True}, dist=radius_km * 1000.0)
    water = ox.features_from_point(
        (lat, lon),
        tags={"waterway": True, "natural": "water", "landuse": "reservoir", "water": True},
        dist=radius_km * 1000.0,
    )

    if buildings.empty:
        log.warning("No building footprints found in radius")
        buildings = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    else:
        buildings = buildings.reset_index(drop=True)
        buildings = buildings[buildings.geometry.notna()].copy()
        buildings = buildings[buildings.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    if roads.empty:
        log.warning("No roads found in radius")
        roads = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    else:
        roads = roads.reset_index(drop=True)
        roads = roads[roads.geometry.notna()].copy()
        roads = roads[roads.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

    if railways.empty:
        log.warning("No railway features found in radius")
        railways = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    else:
        railways = railways.reset_index(drop=True)
        railways = railways[railways.geometry.notna()].copy()
        railways = railways[railways.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

    if water.empty:
        log.warning("No water features found in radius")
        water = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    else:
        water = water.reset_index(drop=True)
        water = water[water.geometry.notna()].copy()
        water = water[
            water.geometry.geom_type.isin(
                ["LineString", "MultiLineString", "Polygon", "MultiPolygon"]
            )
        ].copy()

    if buildings.crs is None:
        buildings = buildings.set_crs("EPSG:4326")
    if roads.crs is None:
        roads = roads.set_crs("EPSG:4326")
    if railways.crs is None:
        railways = railways.set_crs("EPSG:4326")
    if water.crs is None:
        water = water.set_crs("EPSG:4326")

    buildings = buildings.to_crs(TARGET_CRS)
    roads = roads.to_crs(TARGET_CRS)
    railways = railways.to_crs(TARGET_CRS)
    water = water.to_crs(TARGET_CRS)

    if "height_m" not in buildings.columns:
        buildings["height_m"] = buildings.apply(
            lambda row: _parse_height_m(row.get("height"), row.get("building:levels")), axis=1
        )

    log.info(
        "OSM download complete: %d buildings, %d road features, %d railway features, %d water features",
        len(buildings),
        len(roads),
        len(railways),
        len(water),
    )
    return buildings, roads, railways, water


def get_osm_tree_data(lat: float, lon: float, radius_km: float) -> gpd.GeoDataFrame:
    """Download OSM tree points and tree-covered areas (forest/park/wood)."""
    log = logging.getLogger("shaharsoz.trees")
    tags = {
        "natural": ["tree", "wood"],
        "landuse": ["forest"],
        "leisure": ["park"],
    }
    trees = ox.features_from_point((lat, lon), tags=tags, dist=radius_km * 1000.0)
    if trees.empty:
        log.warning("No OSM tree/forest features found in radius")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    trees = trees.reset_index(drop=True)
    trees = trees[trees.geometry.notna()].copy()
    trees = trees[
        trees.geometry.geom_type.isin(
            ["Point", "MultiPoint", "Polygon", "MultiPolygon"]
        )
    ].copy()
    if trees.crs is None:
        trees = trees.set_crs("EPSG:4326")
    trees = trees.to_crs(TARGET_CRS)
    log.info("OSM tree layer prepared: %d features", len(trees))
    return trees


def _get_usgs_3dep_dem(
    bounds_3857: Tuple[float, float, float, float],
    circle_geom_3857,
    data_dir: Path,
    dem_resolution_m: float,
) -> Path:
    """Try USGS 3DEP DEM (best available resolution where covered, mainly US)."""
    minx, miny, maxx, maxy = bounds_3857
    width = max(256, min(4096, int(math.ceil((maxx - minx) / dem_resolution_m))))
    height = max(256, min(4096, int(math.ceil((maxy - miny) / dem_resolution_m))))

    url = _arcgis_export_url(
        base_url=USGS_3DEP_EXPORT_URL,
        bounds_3857=bounds_3857,
        width=width,
        height=height,
        image_format="tiff",
        extra_params={
            "pixelType": "F32",
            "interpolation": "RSP_BilinearInterpolation",
            "compression": "LZ77",
        },
    )

    dem_bbox_path = data_dir / "dem_3dep_bbox.tif"
    _download_arcgis_raster_to_geotiff(
        url=url,
        dst_path=dem_bbox_path,
        bounds_3857=bounds_3857,
        dtype="float32",
        count=1,
        nodata=np.nan,
    )

    with rasterio.open(dem_bbox_path) as src:
        arr = _sanitize_dem_array(src.read(1), src.nodata)
        finite = np.isfinite(arr)
        if not finite.any():
            raise ShaharsozError("USGS 3DEP response contained no valid elevation samples")
        if float(np.nanstd(arr)) < 0.01:
            raise ShaharsozError("USGS 3DEP response appears uniform/invalid for this AOI")

    dem_clipped_path = data_dir / "dem_3857_clipped.tif"
    return _clip_raster_to_circle(dem_bbox_path, circle_geom_3857, dem_clipped_path)


def _get_global_dem_fallback(
    bounds_wgs84: Tuple[float, float, float, float],
    circle_geom_3857,
    data_dir: Path,
) -> Path:
    """Fallback DEM source using global elevation tile mosaic."""
    tile_indices = list(_tile_range(bounds_wgs84, DEM_TILE_ZOOM))
    if not tile_indices:
        raise ShaharsozError("Could not determine DEM tile coverage for query bounds")

    datasets = []
    try:
        for x, y in tile_indices:
            tile_url = DEM_TILE_URL.format(z=DEM_TILE_ZOOM, x=x, y=y)
            try:
                datasets.append(rasterio.open(tile_url))
            except rasterio.errors.RasterioIOError:
                continue

        if not datasets:
            raise ShaharsozError("No DEM tiles were available for the selected area")

        merged, merged_transform = merge(datasets)
        merged_data = merged[0]
        merged_meta = datasets[0].meta.copy()
        merged_meta.update(
            {
                "driver": "GTiff",
                "height": merged_data.shape[0],
                "width": merged_data.shape[1],
                "transform": merged_transform,
                "crs": datasets[0].crs,
                "count": 1,
                "dtype": "float32",
                "nodata": np.nan,
                "compress": "lzw",
            }
        )

        dem_merged_path = data_dir / "dem_merged.tif"
        with rasterio.open(dem_merged_path, "w", **merged_meta) as dst:
            dst.write(merged_data.astype(np.float32), 1)

        dem_clipped_path = data_dir / "dem_3857_clipped.tif"
        return _clip_raster_to_circle(dem_merged_path, circle_geom_3857, dem_clipped_path)
    finally:
        for ds in datasets:
            ds.close()


def get_dem_data(lat: float, lon: float, radius_km: float, data_dir: Path, dem_resolution_m: float = 2.0) -> Path:
    """Download DEM in EPSG:3857, preferring high-resolution sources when available."""
    log = logging.getLogger("shaharsoz.dem")
    _, circle_wgs84, circle_3857 = _radius_geometry(lat, lon, radius_km)
    bounds_wgs84 = tuple(circle_wgs84.total_bounds)
    bounds_3857 = tuple(circle_3857.total_bounds)
    circle_geom_3857 = circle_3857.iloc[0]

    try:
        dem_path = _get_usgs_3dep_dem(
            bounds_3857=bounds_3857,
            circle_geom_3857=circle_geom_3857,
            data_dir=data_dir,
            dem_resolution_m=dem_resolution_m,
        )
        log.info("DEM prepared from USGS 3DEP: %s", dem_path)
        return dem_path
    except Exception as exc:
        log.warning("USGS 3DEP unavailable for this AOI (%s). Falling back to global DEM tiles.", exc)

    dem_path = _get_global_dem_fallback(
        bounds_wgs84=bounds_wgs84,
        circle_geom_3857=circle_geom_3857,
        data_dir=data_dir,
    )
    log.info("DEM prepared from global fallback: %s", dem_path)
    return dem_path


def _stac_search_item(bounds_wgs84: Tuple[float, float, float, float]) -> dict:
    """Search Sentinel-2 item from AWS Earth Search STAC."""
    body = {
        "collections": ["sentinel-2-l2a"],
        "bbox": list(bounds_wgs84),
        "limit": 10,
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
        "query": {"eo:cloud_cover": {"lt": 30}},
    }

    req = urllib.request.Request(
        STAC_SEARCH_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise ShaharsozError(f"Sentinel STAC search failed: {exc}") from exc

    features_list = payload.get("features", [])
    if not features_list:
        raise ShaharsozError("No Sentinel-2 scenes found for the requested area")

    return features_list[0]


def _find_asset_href(item: dict, candidates: Iterable[str]) -> str:
    assets = item.get("assets", {})
    for key in candidates:
        if key in assets and "href" in assets[key]:
            return assets[key]["href"]
    raise ShaharsozError(f"Missing required Sentinel asset. Tried keys: {list(candidates)}")


def _reproject_to_grid(
    src_array: np.ndarray,
    src_transform,
    src_crs,
    dst_shape: Tuple[int, int],
    dst_transform,
    dst_crs: str,
    resampling: Resampling = Resampling.bilinear,
) -> np.ndarray:
    dst_array = np.empty(dst_shape, dtype=np.float32)
    reproject(
        source=src_array,
        destination=dst_array,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        resampling=resampling,
    )
    return dst_array


def _sanitize_dem_array(dem: np.ndarray, nodata: float | None) -> np.ndarray:
    """Normalize DEM nodata to NaN, handling explicit and implicit sentinels."""
    dem = dem.astype(np.float32, copy=True)
    if nodata is not None and np.isfinite(nodata):
        dem[np.isclose(dem, float(nodata))] = np.nan
    dem[dem <= DEM_NODATA_FALLBACK_THRESHOLD] = np.nan
    return dem


def _stretch_to_uint8(array: np.ndarray, low_pct: float = 2.0, high_pct: float = 98.0) -> np.ndarray:
    """Contrast-stretch float array to uint8 for visualization products."""
    out = np.zeros(array.shape, dtype=np.uint8)
    finite = np.isfinite(array)
    if not finite.any():
        return out
    vals = array[finite]
    lo = float(np.percentile(vals, low_pct))
    hi = float(np.percentile(vals, high_pct))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        out[finite] = 128
        return out
    scaled = (array - lo) / (hi - lo)
    scaled = np.clip(scaled, 0.0, 1.0)
    out[finite] = (scaled[finite] * 255.0).astype(np.uint8)
    return out


def get_sentinel_data(lat: float, lon: float, radius_km: float, data_dir: Path) -> Tuple[Path, Path]:
    """Download Sentinel-2 bands, clip/reproject, and save visual products."""
    log = logging.getLogger("shaharsoz.sentinel")
    _, circle_wgs84, circle_3857 = _radius_geometry(lat, lon, radius_km)
    bounds_wgs84 = tuple(circle_wgs84.total_bounds)
    bounds_3857 = tuple(circle_3857.total_bounds)

    log.info("Searching Sentinel-2 scene")
    item = _stac_search_item(bounds_wgs84)

    blue_href = _find_asset_href(item, ("B02", "blue"))
    green_href = _find_asset_href(item, ("B03", "green"))
    red_href = _find_asset_href(item, ("B04", "red"))
    nir_href = _find_asset_href(item, ("B08", "nir", "nir08"))

    minx, miny, maxx, maxy = bounds_3857
    resolution = 10.0
    width = max(1, int(math.ceil((maxx - minx) / resolution)))
    height = max(1, int(math.ceil((maxy - miny) / resolution)))
    dst_transform = from_bounds(minx, miny, maxx, maxy, width, height)

    circle_geometry_3857 = circle_3857.iloc[0]

    def _load_band(href: str) -> np.ndarray:
        with rasterio.open(href) as src:
            area_src = gpd.GeoSeries([circle_geometry_3857], crs=TARGET_CRS).to_crs(src.crs)
            clipped, clipped_transform = mask(src, [mapping(area_src.iloc[0])], crop=True)
            band = clipped[0].astype(np.float32)
            if src.nodata is not None:
                band[band == src.nodata] = np.nan
            return _reproject_to_grid(
                src_array=band,
                src_transform=clipped_transform,
                src_crs=src.crs,
                dst_shape=(height, width),
                dst_transform=dst_transform,
                dst_crs=TARGET_CRS,
            )

    blue = _load_band(blue_href)
    green = _load_band(green_href)
    red = _load_band(red_href)
    nir = _load_band(nir_href)

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": "float32",
        "crs": TARGET_CRS,
        "transform": dst_transform,
        "nodata": np.nan,
        "compress": "lzw",
    }

    blue_path = data_dir / "sentinel_b02_3857.tif"
    green_path = data_dir / "sentinel_b03_3857.tif"
    red_path = data_dir / "sentinel_b04_3857.tif"
    nir_path = data_dir / "sentinel_b08_3857.tif"

    with rasterio.open(blue_path, "w", **profile) as dst:
        dst.write(blue, 1)

    with rasterio.open(green_path, "w", **profile) as dst:
        dst.write(green, 1)

    with rasterio.open(red_path, "w", **profile) as dst:
        dst.write(red, 1)

    with rasterio.open(nir_path, "w", **profile) as dst:
        dst.write(nir, 1)

    rgb_profile = profile.copy()
    rgb_profile.update(count=3, dtype="uint8", nodata=0)
    rgb_path = data_dir / "sentinel_rgb_preview.tif"
    with rasterio.open(rgb_path, "w", **rgb_profile) as dst:
        dst.write(_stretch_to_uint8(red), 1)
        dst.write(_stretch_to_uint8(green), 2)
        dst.write(_stretch_to_uint8(blue), 3)

    log.info("Sentinel bands prepared: %s, %s, %s, %s", blue_path, green_path, red_path, nir_path)
    log.info("Sentinel RGB preview written: %s", rgb_path)
    return red_path, nir_path


def get_basemap_texture(
    lat: float,
    lon: float,
    radius_km: float,
    data_dir: Path,
    texture_resolution_m: float = 1.0,
    wayback_layer_id: int | None = 64001,
) -> Path:
    """Download high-resolution basemap RGB texture for visualization."""
    log = logging.getLogger("shaharsoz.basemap")
    _, _, circle_3857 = _radius_geometry(lat, lon, radius_km)
    bounds_3857 = tuple(circle_3857.total_bounds)
    minx, miny, maxx, maxy = bounds_3857

    width = max(512, min(4096, int(math.ceil((maxx - minx) / texture_resolution_m))))
    height = max(512, min(4096, int(math.ceil((maxy - miny) / texture_resolution_m))))

    basemap_path = data_dir / "basemap_rgb_3857.tif"
    if wayback_layer_id is not None:
        try:
            url = _arcgis_export_url(
                base_url=ESRI_WAYBACK_EXPORT_URL,
                bounds_3857=bounds_3857,
                width=width,
                height=height,
                image_format="jpg",
                extra_params={"transparent": "false", "layers": f"show:{int(wayback_layer_id)}"},
            )
            _download_arcgis_raster_to_geotiff(
                url=url,
                dst_path=basemap_path,
                bounds_3857=bounds_3857,
                dtype="uint8",
                count=3,
                nodata=0,
            )
            log.info("Basemap texture prepared from Esri Wayback layer %s: %s", wayback_layer_id, basemap_path)
            return basemap_path
        except Exception as exc:
            log.warning("Wayback layer %s unavailable (%s). Falling back to Esri World Imagery.", wayback_layer_id, exc)

    url = _arcgis_export_url(
        base_url=ESRI_WORLD_IMAGERY_EXPORT_URL,
        bounds_3857=bounds_3857,
        width=width,
        height=height,
        image_format="jpg",
        extra_params={"transparent": "false"},
    )
    _download_arcgis_raster_to_geotiff(
        url=url,
        dst_path=basemap_path,
        bounds_3857=bounds_3857,
        dtype="uint8",
        count=3,
        nodata=0,
    )
    log.info("Basemap texture prepared from Esri World Imagery: %s", basemap_path)
    return basemap_path


def _to_uint8_hwc(array_chw: np.ndarray) -> np.ndarray:
    """Normalize raster array to uint8 RGB in HWC layout."""
    arr = np.asarray(array_chw)
    if arr.ndim != 3:
        raise ShaharsozError("Expected 3D array for RGB conversion")
    if arr.shape[0] < 3:
        raise ShaharsozError("Input texture must have at least 3 channels")
    rgb = arr[:3].transpose(1, 2, 0)
    if rgb.dtype == np.uint8:
        return rgb
    rgb = rgb.astype(np.float32)
    finite = np.isfinite(rgb)
    if finite.any():
        lo = float(np.nanpercentile(rgb[finite], 2))
        hi = float(np.nanpercentile(rgb[finite], 98))
        if hi > lo:
            rgb = np.clip((rgb - lo) / (hi - lo), 0.0, 1.0) * 255.0
        else:
            rgb = np.clip(rgb, 0.0, 255.0)
    else:
        rgb[:] = 0.0
    return rgb.astype(np.uint8)


def _crop_to_map_region(rgb_hwc: np.ndarray) -> np.ndarray:
    """
    Crop likely UI borders from screenshots using per-row/col texture variance.
    Falls back to original image if no strong map window is detected.
    """
    gray = (
        rgb_hwc[..., 0].astype(np.float32) * 0.2989
        + rgb_hwc[..., 1].astype(np.float32) * 0.5870
        + rgb_hwc[..., 2].astype(np.float32) * 0.1140
    )
    row_var = gray.var(axis=1)
    col_var = gray.var(axis=0)

    row_thr = max(float(np.percentile(row_var, 20)) * 1.15, 8.0)
    col_thr = max(float(np.percentile(col_var, 20)) * 1.15, 8.0)
    valid_rows = np.where(row_var > row_thr)[0]
    valid_cols = np.where(col_var > col_thr)[0]

    if valid_rows.size < gray.shape[0] * 0.4 or valid_cols.size < gray.shape[1] * 0.4:
        return rgb_hwc

    pad_r = max(2, int(0.015 * gray.shape[0]))
    pad_c = max(2, int(0.015 * gray.shape[1]))
    r0 = max(0, int(valid_rows[0]) - pad_r)
    r1 = min(gray.shape[0], int(valid_rows[-1]) + pad_r + 1)
    c0 = max(0, int(valid_cols[0]) - pad_c)
    c1 = min(gray.shape[1], int(valid_cols[-1]) + pad_c + 1)
    cropped = rgb_hwc[r0:r1, c0:c1]
    if cropped.shape[0] < 128 or cropped.shape[1] < 128:
        return rgb_hwc
    return cropped


def _resize_nn_hwc(image: np.ndarray, out_h: int, out_w: int) -> np.ndarray:
    """Nearest-neighbor resize for HWC arrays without extra dependencies."""
    in_h, in_w = image.shape[:2]
    if in_h == out_h and in_w == out_w:
        return image.copy()
    y_idx = np.clip((np.arange(out_h) * (in_h / out_h)).astype(np.int32), 0, in_h - 1)
    x_idx = np.clip((np.arange(out_w) * (in_w / out_w)).astype(np.int32), 0, in_w - 1)
    return image[y_idx][:, x_idx]


def _warp_similarity_gray(gray: np.ndarray, out_shape: Tuple[int, int], angle_deg: float, scale: float) -> Tuple[np.ndarray, np.ndarray]:
    """Warp grayscale image by similarity transform into output frame."""
    out_h, out_w = out_shape
    in_h, in_w = gray.shape
    cy_out = (out_h - 1) * 0.5
    cx_out = (out_w - 1) * 0.5
    cy_in = (in_h - 1) * 0.5
    cx_in = (in_w - 1) * 0.5

    yy, xx = np.indices((out_h, out_w), dtype=np.float32)
    x0 = (xx - cx_out) / max(scale, 1e-6)
    y0 = (yy - cy_out) / max(scale, 1e-6)

    ang = math.radians(angle_deg)
    ca, sa = math.cos(ang), math.sin(ang)
    src_x = ca * x0 + sa * y0 + cx_in
    src_y = -sa * x0 + ca * y0 + cy_in

    valid = (src_x >= 0.0) & (src_x <= in_w - 1.0) & (src_y >= 0.0) & (src_y <= in_h - 1.0)
    xi = np.clip(np.round(src_x).astype(np.int32), 0, in_w - 1)
    yi = np.clip(np.round(src_y).astype(np.int32), 0, in_h - 1)

    out = np.zeros((out_h, out_w), dtype=np.float32)
    out[valid] = gray[yi[valid], xi[valid]]
    return out, valid


def _shift_2d(array: np.ndarray, dy: int, dx: int, fill_value: float = 0.0) -> np.ndarray:
    """Integer shift for 2D arrays."""
    out = np.full_like(array, fill_value)
    h, w = array.shape
    y0_src = max(0, -dy)
    y1_src = min(h, h - dy) if dy >= 0 else h
    x0_src = max(0, -dx)
    x1_src = min(w, w - dx) if dx >= 0 else w
    y0_dst = max(0, dy)
    y1_dst = y0_dst + (y1_src - y0_src)
    x0_dst = max(0, dx)
    x1_dst = x0_dst + (x1_src - x0_src)
    if y1_src > y0_src and x1_src > x0_src:
        out[y0_dst:y1_dst, x0_dst:x1_dst] = array[y0_src:y1_src, x0_src:x1_src]
    return out


def _phase_shift(ref: np.ndarray, mov: np.ndarray) -> Tuple[int, int, float]:
    """Estimate translation from mov to ref via phase correlation."""
    eps = 1e-8
    ref0 = ref - float(ref.mean())
    mov0 = mov - float(mov.mean())
    f_ref = np.fft.fft2(ref0)
    f_mov = np.fft.fft2(mov0)
    cps = f_ref * np.conj(f_mov)
    cps /= np.maximum(np.abs(cps), eps)
    corr = np.fft.ifft2(cps).real
    peak = np.unravel_index(np.argmax(corr), corr.shape)
    dy, dx = int(peak[0]), int(peak[1])
    h, w = corr.shape
    if dy > h // 2:
        dy -= h
    if dx > w // 2:
        dx -= w
    score = float(corr[peak])
    return dy, dx, score


def _align_texture_to_reference(input_rgb: np.ndarray, reference_rgb: np.ndarray) -> np.ndarray:
    """
    Align screenshot texture to reference imagery using rotation/scale/shift search.
    Returns RGB image in reference frame size.
    """
    ref_h, ref_w = reference_rgb.shape[:2]
    src = _resize_nn_hwc(input_rgb, ref_h, ref_w)

    ref_gray = (
        reference_rgb[..., 0].astype(np.float32) * 0.2989
        + reference_rgb[..., 1].astype(np.float32) * 0.5870
        + reference_rgb[..., 2].astype(np.float32) * 0.1140
    )
    src_gray = (
        src[..., 0].astype(np.float32) * 0.2989
        + src[..., 1].astype(np.float32) * 0.5870
        + src[..., 2].astype(np.float32) * 0.1140
    )

    best = {"score": -1e9, "angle": 0.0, "scale": 1.0, "dy": 0, "dx": 0}
    angles = list(np.arange(-12.0, 12.1, 2.0))
    scales = [0.80, 0.90, 1.00, 1.10, 1.20]
    for angle in angles:
        for scale in scales:
            warped, valid = _warp_similarity_gray(src_gray, (ref_h, ref_w), angle, scale)
            dy, dx, pc_score = _phase_shift(ref_gray, warped)
            shifted = _shift_2d(warped, dy, dx, fill_value=0.0)
            valid_shifted = _shift_2d(valid.astype(np.float32), dy, dx, fill_value=0.0) > 0.5
            overlap = int(valid_shifted.sum())
            if overlap < int(ref_h * ref_w * 0.2):
                continue
            a = ref_gray[valid_shifted]
            b = shifted[valid_shifted]
            a_std = float(np.std(a))
            b_std = float(np.std(b))
            if a_std < 1e-6 or b_std < 1e-6:
                continue
            corr = float(np.corrcoef(a, b)[0, 1])
            score = corr + 0.03 * pc_score + 0.000001 * overlap
            if score > best["score"]:
                best = {"score": score, "angle": angle, "scale": scale, "dy": int(dy), "dx": int(dx)}

    aligned = np.zeros((ref_h, ref_w, 3), dtype=np.uint8)
    for band in range(3):
        warped, _ = _warp_similarity_gray(src[..., band].astype(np.float32), (ref_h, ref_w), best["angle"], best["scale"])
        shifted = _shift_2d(warped, best["dy"], best["dx"], fill_value=0.0)
        aligned[..., band] = np.clip(shifted, 0, 255).astype(np.uint8)
    return aligned


def _download_reference_basemap_array(
    bounds_3857: Tuple[float, float, float, float],
    width: int,
    height: int,
    wayback_layer_id: int | None = 64001,
) -> np.ndarray:
    """Download reference RGB basemap as HWC uint8 array for alignment."""
    payload = None
    if wayback_layer_id is not None:
        try:
            url = _arcgis_export_url(
                base_url=ESRI_WAYBACK_EXPORT_URL,
                bounds_3857=bounds_3857,
                width=width,
                height=height,
                image_format="jpg",
                extra_params={"transparent": "false", "layers": f"show:{int(wayback_layer_id)}"},
            )
            payload = _fetch_bytes(url)
        except Exception:
            payload = None
    if payload is None:
        url = _arcgis_export_url(
            base_url=ESRI_WORLD_IMAGERY_EXPORT_URL,
            bounds_3857=bounds_3857,
            width=width,
            height=height,
            image_format="jpg",
            extra_params={"transparent": "false"},
        )
        payload = _fetch_bytes(url)

    with MemoryFile(payload) as memfile:
        with memfile.open() as src:
            arr = src.read()
    return _to_uint8_hwc(arr)


def _parse_coords_from_name(path: Path) -> Tuple[float, float, float | None] | None:
    """
    Parse coordinate naming convention from filename.
    Supported examples:
    - 41.3111_69.2797.png
    - 41.3111_69.2797_2km.jpg
    - lat41.3111_lon69.2797_radius2.0.jpeg
    """
    stem = path.stem.lower()
    values = re.findall(r"[-+]?\d*\.?\d+", stem)
    if len(values) < 2:
        return None
    try:
        lat = float(values[0])
        lon = float(values[1])
        radius = float(values[2]) if len(values) >= 3 else None
    except ValueError:
        return None
    return lat, lon, radius


def _find_input_texture(
    input_dir: Path,
    lat: float,
    lon: float,
    radius_km: float,
    model_name: str | None = None,
) -> Path | None:
    """Find closest coordinate-matching image in input directory."""
    if not input_dir.exists() or not input_dir.is_dir():
        return None

    if model_name:
        normalized = model_name.strip().lower()
        for path in sorted(input_dir.iterdir()):
            if not path.is_file():
                continue
            if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".webp"}:
                continue
            if path.stem.strip().lower() == normalized:
                return path

    candidates: list[Tuple[float, Path]] = []
    for path in input_dir.iterdir():
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".webp"}:
            continue
        parsed = _parse_coords_from_name(path)
        if parsed is None:
            continue
        img_lat, img_lon, img_radius = parsed
        score = abs(img_lat - lat) + abs(img_lon - lon)
        if img_radius is not None:
            score += abs(img_radius - radius_km) * 0.25
        candidates.append((score, path))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def get_input_texture(
    lat: float,
    lon: float,
    radius_km: float,
    input_dir: Path,
    data_dir: Path,
    model_name: str | None = None,
    wayback_layer_id: int | None = 64001,
) -> Path | None:
    """Load screenshot texture, auto-crop/align, then georeference to AOI bounds."""
    log = logging.getLogger("shaharsoz.input_texture")
    selected = _find_input_texture(
        input_dir=input_dir,
        lat=lat,
        lon=lon,
        radius_km=radius_km,
        model_name=model_name,
    )
    if selected is None:
        return None

    _, _, circle_3857 = _radius_geometry(lat, lon, radius_km)
    bounds_3857 = tuple(circle_3857.total_bounds)
    minx, miny, maxx, maxy = bounds_3857

    with rasterio.open(selected) as src:
        data = src.read()
        if data.shape[0] == 1:
            data = np.repeat(data, 3, axis=0)
        if data.shape[0] >= 3:
            data = data[:3]
        else:
            raise ShaharsozError(f"Unsupported screenshot band count in {selected}")

    input_rgb = _to_uint8_hwc(data)
    cropped = _crop_to_map_region(input_rgb)
    height, width = cropped.shape[0], cropped.shape[1]

    aligned = cropped
    try:
        reference = _download_reference_basemap_array(
            bounds_3857=bounds_3857,
            width=width,
            height=height,
            wayback_layer_id=wayback_layer_id,
        )
        aligned = _align_texture_to_reference(cropped, reference)
        log.info("Input texture aligned via rotation/scale/translation fit")
    except Exception as exc:
        log.warning("Input texture alignment skipped; using cropped texture only (%s)", exc)

    profile = {
        "driver": "GTiff",
        "height": aligned.shape[0],
        "width": aligned.shape[1],
        "count": 3,
        "dtype": "uint8",
        "crs": TARGET_CRS,
        "transform": from_bounds(minx, miny, maxx, maxy, aligned.shape[1], aligned.shape[0]),
        "nodata": 0,
        "compress": "lzw",
    }

    output = data_dir / "input_texture_3857.tif"
    with rasterio.open(output, "w", **profile) as dst:
        dst.write(aligned.transpose(2, 0, 1))

    log.info("Using input texture %s -> %s", selected, output)
    return output


def compute_ndvi(nir_path: Path, red_path: Path, data_dir: Path) -> gpd.GeoDataFrame:
    """Compute NDVI raster and return vegetation polygons (NDVI > 0.3)."""
    log = logging.getLogger("shaharsoz.ndvi")

    with rasterio.open(nir_path) as nir_src, rasterio.open(red_path) as red_src:
        nir = nir_src.read(1).astype(np.float32)
        red = red_src.read(1).astype(np.float32)

        if nir.shape != red.shape:
            raise ShaharsozError("Sentinel bands do not align; cannot compute NDVI")

        with np.errstate(divide="ignore", invalid="ignore"):
            ndvi = (nir - red) / (nir + red)

        ndvi[np.isinf(ndvi)] = np.nan

        profile = _sanitize_gtiff_profile(nir_src.profile.copy())
        profile.update(dtype="float32", nodata=np.nan, compress="lzw")

        ndvi_path = data_dir / "ndvi.tif"
        with rasterio.open(ndvi_path, "w", **profile) as dst:
            dst.write(ndvi, 1)

        vegetation_mask = np.where(ndvi > 0.3, 1, 0).astype(np.uint8)
        mask_profile = _sanitize_gtiff_profile(nir_src.profile.copy())
        mask_profile.update(dtype="uint8", nodata=0, compress="lzw")

        mask_path = data_dir / "vegetation_mask.tif"
        with rasterio.open(mask_path, "w", **mask_profile) as dst:
            dst.write(vegetation_mask, 1)

        ndvi_preview_profile = _sanitize_gtiff_profile(nir_src.profile.copy())
        ndvi_preview_profile.update(dtype="uint8", nodata=0, compress="lzw")
        ndvi_preview_path = data_dir / "ndvi_preview.tif"
        with rasterio.open(ndvi_preview_path, "w", **ndvi_preview_profile) as dst:
            ndvi_scaled = np.where(np.isfinite(ndvi), (np.clip((ndvi + 1.0) / 2.0, 0.0, 1.0) * 255.0), 0.0).astype(
                np.uint8
            )
            dst.write(ndvi_scaled, 1)

        mask_preview_path = data_dir / "vegetation_mask_preview.tif"
        with rasterio.open(mask_preview_path, "w", **ndvi_preview_profile) as dst:
            dst.write((vegetation_mask * 255).astype(np.uint8), 1)

        geoms = []
        for geom, val in features.shapes(vegetation_mask, mask=vegetation_mask == 1, transform=nir_src.transform):
            if val != 1:
                continue
            poly = shape(geom)
            if poly.area >= 25.0:
                geoms.append(poly)

    if not geoms:
        log.warning("No vegetation polygons detected with NDVI > 0.3")
        return gpd.GeoDataFrame(geometry=[], crs=TARGET_CRS)

    vegetation = gpd.GeoDataFrame(geometry=geoms, crs=TARGET_CRS)
    vegetation = vegetation.dissolve().explode(index_parts=False).reset_index(drop=True)
    vegetation = vegetation[vegetation.geometry.notna()].copy()
    vegetation = vegetation[vegetation.geometry.is_valid].copy()

    log.info("NDVI complete: %d vegetation polygons", len(vegetation))
    return vegetation


def compute_ndwi_water(green_path: Path, nir_path: Path, data_dir: Path, threshold: float = 0.12) -> gpd.GeoDataFrame:
    """Compute NDWI water mask from Sentinel bands and polygonize water areas."""
    log = logging.getLogger("shaharsoz.ndwi")

    with rasterio.open(green_path) as g_src, rasterio.open(nir_path) as nir_src:
        green = g_src.read(1).astype(np.float32)
        nir = nir_src.read(1).astype(np.float32)
        if green.shape != nir.shape:
            raise ShaharsozError("Green and NIR bands do not align for NDWI")

        with np.errstate(divide="ignore", invalid="ignore"):
            ndwi = (green - nir) / (green + nir)
        ndwi[np.isinf(ndwi)] = np.nan

        ndwi_profile = _sanitize_gtiff_profile(g_src.profile.copy())
        ndwi_profile.update(dtype="float32", nodata=np.nan, compress="lzw")
        ndwi_path = data_dir / "ndwi.tif"
        with rasterio.open(ndwi_path, "w", **ndwi_profile) as dst:
            dst.write(ndwi, 1)

        water_mask = np.where(ndwi > threshold, 1, 0).astype(np.uint8)
        mask_profile = _sanitize_gtiff_profile(g_src.profile.copy())
        mask_profile.update(dtype="uint8", nodata=0, compress="lzw")
        water_mask_path = data_dir / "water_mask.tif"
        with rasterio.open(water_mask_path, "w", **mask_profile) as dst:
            dst.write(water_mask, 1)

        water_preview_path = data_dir / "water_mask_preview.tif"
        with rasterio.open(water_preview_path, "w", **mask_profile) as dst:
            dst.write((water_mask * 255).astype(np.uint8), 1)

        geoms = []
        for geom, val in features.shapes(water_mask, mask=water_mask == 1, transform=g_src.transform):
            if val != 1:
                continue
            poly = shape(geom)
            if poly.area >= 50.0:
                geoms.append(poly)

    if not geoms:
        log.warning("No NDWI water polygons detected with threshold > %.3f", threshold)
        return gpd.GeoDataFrame(geometry=[], crs=TARGET_CRS)

    water = gpd.GeoDataFrame(geometry=geoms, crs=TARGET_CRS)
    water = water.dissolve().explode(index_parts=False).reset_index(drop=True)
    water = water[water.geometry.notna()].copy()
    water = water[water.geometry.is_valid].copy()
    log.info("NDWI water polygons: %d", len(water))
    return water


def _dem_xy_grids(transform, width: int, height: int) -> Tuple[np.ndarray, np.ndarray]:
    x_coords = transform.c + (np.arange(width) + 0.5) * transform.a
    y_coords = transform.f + (np.arange(height) + 0.5) * transform.e
    xx, yy = np.meshgrid(x_coords, y_coords)
    return xx.astype(np.float32), yy.astype(np.float32)


def create_terrain_mesh(dem_path: Path) -> pv.PolyData:
    """Create triangulated terrain mesh from DEM values."""
    log = logging.getLogger("shaharsoz.terrain")

    with rasterio.open(dem_path) as dem_src:
        dem = _sanitize_dem_array(dem_src.read(1), dem_src.nodata)

        finite = np.isfinite(dem)
        if not finite.any():
            raise ShaharsozError("DEM contains no finite elevation values")

        min_elev = float(np.nanmin(dem))
        dem[~finite] = min_elev
        xx, yy = _dem_xy_grids(dem_src.transform, dem_src.width, dem_src.height)

    grid = pv.StructuredGrid(xx, yy, dem)
    terrain = grid.extract_surface(algorithm="dataset_surface").triangulate().clean()
    log.info("Terrain mesh built: %d points, %d faces", terrain.n_points, terrain.n_cells)
    return terrain


def _sample_dem(dem: np.ndarray, transform, x: float, y: float) -> float:
    col_f, row_f = ~transform * (x, y)
    col = int(np.clip(round(col_f), 0, dem.shape[1] - 1))
    row = int(np.clip(round(row_f), 0, dem.shape[0] - 1))
    value = float(dem[row, col])
    if not math.isfinite(value):
        return float(np.nanmean(dem))
    return value


def _as_bool_tag(value: object) -> bool:
    """Interpret common OSM truthy values."""
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "viaduct"}


def _road_width_m(row: gpd.GeoSeries) -> float:
    """Approximate road/bridge width from OSM attributes."""
    lanes = row.get("lanes")
    if lanes is not None and not (isinstance(lanes, float) and math.isnan(lanes)):
        match = re.search(r"\d+", str(lanes))
        if match:
            return max(4.0, float(match.group(0)) * 3.5)

    highway = str(row.get("highway", "")).lower()
    width_by_type = {
        "motorway": 16.0,
        "trunk": 14.0,
        "primary": 12.0,
        "secondary": 10.0,
        "tertiary": 8.0,
        "residential": 7.0,
        "service": 5.0,
    }
    for key, value in width_by_type.items():
        if key in highway:
            return value
    return 6.0


def _extrude_polygon_prism(
    poly,
    base_z: float,
    top_z: float,
    vertices: list[Tuple[float, float, float]],
    faces: list[int],
) -> None:
    """Extrude polygon into triangular prism mesh sections."""

    def add_vertex(vertex: Tuple[float, float, float]) -> int:
        vertices.append(vertex)
        return len(vertices) - 1

    for tri in triangulate(poly):
        if tri.is_empty:
            continue
        if not tri.representative_point().within(poly):
            continue
        coords = list(tri.exterior.coords)[:-1]
        if len(coords) != 3:
            continue

        b0 = add_vertex((coords[0][0], coords[0][1], base_z))
        b1 = add_vertex((coords[1][0], coords[1][1], base_z))
        b2 = add_vertex((coords[2][0], coords[2][1], base_z))
        t0 = add_vertex((coords[0][0], coords[0][1], top_z))
        t1 = add_vertex((coords[1][0], coords[1][1], top_z))
        t2 = add_vertex((coords[2][0], coords[2][1], top_z))

        faces.extend([3, b0, b1, b2])
        faces.extend([3, t2, t1, t0])

        faces.extend([3, b0, b1, t1])
        faces.extend([3, b0, t1, t0])
        faces.extend([3, b1, b2, t2])
        faces.extend([3, b1, t2, t1])
        faces.extend([3, b2, b0, t0])
        faces.extend([3, b2, t0, t2])


def extrude_buildings(buildings: gpd.GeoDataFrame, dem_path: Path) -> pv.PolyData:
    """Extrude building footprints into a single clean triangular mesh."""
    log = logging.getLogger("shaharsoz.buildings")

    if buildings.empty:
        log.warning("No buildings to extrude")
        return pv.PolyData()

    with rasterio.open(dem_path) as dem_src:
        dem = _sanitize_dem_array(dem_src.read(1), dem_src.nodata)

        finite = np.isfinite(dem)
        if finite.any():
            dem[~finite] = float(np.nanmin(dem[finite]))
        else:
            dem[:] = 0.0

        dem_transform = dem_src.transform

    vertices: list[Tuple[float, float, float]] = []
    faces: list[int] = []

    building_count = 0
    for _, row in buildings.iterrows():
        geom = row.geometry
        height = float(row.get("height_m", DEFAULT_BUILDING_LEVEL_HEIGHT_M))

        polygons = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]

        for poly in polygons:
            if poly.is_empty or not poly.is_valid:
                continue

            base_z = _sample_dem(dem, dem_transform, poly.centroid.x, poly.centroid.y)
            top_z = base_z + max(1.0, height)

            _extrude_polygon_prism(poly=poly, base_z=base_z, top_z=top_z, vertices=vertices, faces=faces)

            building_count += 1

    if not vertices:
        log.warning("Building extrusion produced no geometry")
        return pv.PolyData()

    mesh = pv.PolyData(np.array(vertices, dtype=np.float32), np.array(faces, dtype=np.int64)).clean()
    log.info("Building mesh built: %d buildings, %d points, %d faces", building_count, mesh.n_points, mesh.n_cells)
    return mesh


def extrude_bridges(roads: gpd.GeoDataFrame, dem_path: Path) -> pv.PolyData:
    """Generate bridge deck meshes from OSM roads marked as bridges."""
    log = logging.getLogger("shaharsoz.bridges")
    if roads.empty:
        return pv.PolyData()

    with rasterio.open(dem_path) as dem_src:
        dem = _sanitize_dem_array(dem_src.read(1), dem_src.nodata)
        finite = np.isfinite(dem)
        if finite.any():
            dem[~finite] = float(np.nanmin(dem[finite]))
        else:
            dem[:] = 0.0
        dem_transform = dem_src.transform

    bridge_rows = roads[roads.apply(lambda r: _as_bool_tag(r.get("bridge")), axis=1)]
    if bridge_rows.empty:
        log.info("No OSM bridge-tagged roads found")
        return pv.PolyData()

    vertices: list[Tuple[float, float, float]] = []
    faces: list[int] = []
    bridge_count = 0

    for _, row in bridge_rows.iterrows():
        geom = row.geometry
        width_m = _road_width_m(row)
        if geom.is_empty:
            continue

        lines = list(geom.geoms) if geom.geom_type == "MultiLineString" else [geom]
        for line in lines:
            if line.is_empty or line.length < 2.0:
                continue

            deck_poly = line.buffer(width_m * 0.5, cap_style=2, join_style=2)
            if deck_poly.is_empty:
                continue

            base_z = _sample_dem(dem, dem_transform, line.centroid.x, line.centroid.y) + DEFAULT_BRIDGE_CLEARANCE_M
            top_z = base_z + DEFAULT_BRIDGE_DECK_THICKNESS_M

            polygons = list(deck_poly.geoms) if deck_poly.geom_type == "MultiPolygon" else [deck_poly]
            for poly in polygons:
                if poly.is_valid and not poly.is_empty:
                    _extrude_polygon_prism(poly=poly, base_z=base_z, top_z=top_z, vertices=vertices, faces=faces)
            bridge_count += 1

    if not vertices:
        return pv.PolyData()

    mesh = pv.PolyData(np.array(vertices, dtype=np.float32), np.array(faces, dtype=np.int64)).clean()
    log.info("Bridge mesh built: %d bridge segments, %d points, %d faces", bridge_count, mesh.n_points, mesh.n_cells)
    return mesh


def extrude_roads(roads: gpd.GeoDataFrame, dem_path: Path, thickness_m: float = 0.25) -> pv.PolyData:
    """Generate draped road meshes from OSM centerlines for game blockout."""
    log = logging.getLogger("shaharsoz.roads_mesh")
    if roads.empty:
        return pv.PolyData()

    with rasterio.open(dem_path) as dem_src:
        dem = _sanitize_dem_array(dem_src.read(1), dem_src.nodata)
        finite = np.isfinite(dem)
        if finite.any():
            dem[~finite] = float(np.nanmin(dem[finite]))
        else:
            dem[:] = 0.0
        dem_transform = dem_src.transform

    vertices: list[Tuple[float, float, float]] = []
    faces: list[int] = []
    road_count = 0

    non_bridge = roads[~roads.apply(lambda r: _as_bool_tag(r.get("bridge")), axis=1)]
    if non_bridge.empty:
        return pv.PolyData()

    for _, row in non_bridge.iterrows():
        geom = row.geometry
        if geom.is_empty:
            continue
        width_m = _road_width_m(row)
        lines = list(geom.geoms) if geom.geom_type == "MultiLineString" else [geom]
        for line in lines:
            if line.is_empty or line.length < 1.0:
                continue
            road_poly = line.buffer(max(2.0, width_m * 0.5), cap_style=2, join_style=2)
            if road_poly.is_empty:
                continue
            base_z = _sample_dem(dem, dem_transform, line.centroid.x, line.centroid.y) + 0.05
            top_z = base_z + max(0.05, thickness_m)
            polygons = list(road_poly.geoms) if road_poly.geom_type == "MultiPolygon" else [road_poly]
            for poly in polygons:
                if poly.is_valid and not poly.is_empty:
                    _extrude_polygon_prism(poly=poly, base_z=base_z, top_z=top_z, vertices=vertices, faces=faces)
            road_count += 1

    if not vertices:
        return pv.PolyData()

    mesh = pv.PolyData(np.array(vertices, dtype=np.float32), np.array(faces, dtype=np.int64)).clean()
    log.info("Road mesh built: %d road segments, %d points, %d faces", road_count, mesh.n_points, mesh.n_cells)
    return mesh


def _rail_width_m(row: gpd.GeoSeries) -> float:
    """Approximate rail corridor width in meters."""
    railway = str(row.get("railway", "")).lower()
    if railway in {"subway", "tram", "light_rail"}:
        return 4.0
    if railway in {"narrow_gauge"}:
        return 3.5
    return 5.0


def extrude_railways(railways: gpd.GeoDataFrame, dem_path: Path, thickness_m: float = 0.22) -> pv.PolyData:
    """Generate 3D railway meshes from OSM centerlines."""
    log = logging.getLogger("shaharsoz.rail_mesh")
    if railways.empty:
        return pv.PolyData()

    with rasterio.open(dem_path) as dem_src:
        dem = _sanitize_dem_array(dem_src.read(1), dem_src.nodata)
        finite = np.isfinite(dem)
        if finite.any():
            dem[~finite] = float(np.nanmin(dem[finite]))
        else:
            dem[:] = 0.0
        dem_transform = dem_src.transform

    vertices: list[Tuple[float, float, float]] = []
    faces: list[int] = []
    rail_count = 0

    for _, row in railways.iterrows():
        geom = row.geometry
        if geom.is_empty:
            continue
        width_m = _rail_width_m(row)
        lines = list(geom.geoms) if geom.geom_type == "MultiLineString" else [geom]
        for line in lines:
            if line.is_empty or line.length < 1.0:
                continue
            rail_poly = line.buffer(max(2.2, width_m * 0.5), cap_style=2, join_style=2)
            if rail_poly.is_empty:
                continue
            base_z = _sample_dem(dem, dem_transform, line.centroid.x, line.centroid.y) + 0.08
            top_z = base_z + max(0.05, thickness_m)
            polys = list(rail_poly.geoms) if rail_poly.geom_type == "MultiPolygon" else [rail_poly]
            for poly in polys:
                if poly.is_valid and not poly.is_empty:
                    _extrude_polygon_prism(poly=poly, base_z=base_z, top_z=top_z, vertices=vertices, faces=faces)
            rail_count += 1

    if not vertices:
        return pv.PolyData()
    mesh = pv.PolyData(np.array(vertices, dtype=np.float32), np.array(faces, dtype=np.int64)).clean()
    log.info("Rail mesh built: %d segments, %d points, %d faces", rail_count, mesh.n_points, mesh.n_cells)
    return mesh


def _water_width_m(row: gpd.GeoSeries) -> float:
    """Approximate waterway width."""
    width = row.get("width")
    if width is not None and not (isinstance(width, float) and math.isnan(width)):
        match = re.search(r"[-+]?\d*\.?\d+", str(width))
        if match:
            return max(2.0, float(match.group(0)))
    waterway = str(row.get("waterway", "")).lower()
    defaults = {
        "river": 20.0,
        "canal": 12.0,
        "stream": 5.0,
        "ditch": 3.0,
    }
    for key, value in defaults.items():
        if key in waterway:
            return value
    return 8.0


def extrude_water(water: gpd.GeoDataFrame, dem_path: Path, thickness_m: float = 0.15) -> pv.PolyData:
    """Generate water surface meshes from OSM waterways/waterbodies."""
    log = logging.getLogger("shaharsoz.water_mesh")
    if water.empty:
        return pv.PolyData()

    with rasterio.open(dem_path) as dem_src:
        dem = _sanitize_dem_array(dem_src.read(1), dem_src.nodata)
        finite = np.isfinite(dem)
        if finite.any():
            dem[~finite] = float(np.nanmin(dem[finite]))
        else:
            dem[:] = 0.0
        dem_transform = dem_src.transform

    vertices: list[Tuple[float, float, float]] = []
    faces: list[int] = []
    water_count = 0

    for _, row in water.iterrows():
        geom = row.geometry
        if geom.is_empty:
            continue

        if geom.geom_type in {"LineString", "MultiLineString"}:
            lines = list(geom.geoms) if geom.geom_type == "MultiLineString" else [geom]
            width_m = _water_width_m(row)
            polygons = [line.buffer(width_m * 0.5, cap_style=2, join_style=2) for line in lines if line.length >= 1.0]
        else:
            polygons = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]

        for poly in polygons:
            if poly.is_empty or not poly.is_valid:
                continue
            base = _sample_dem(dem, dem_transform, poly.centroid.x, poly.centroid.y) - 0.1
            top = base + max(0.05, thickness_m)
            _extrude_polygon_prism(poly=poly, base_z=base, top_z=top, vertices=vertices, faces=faces)
            water_count += 1

    if not vertices:
        return pv.PolyData()
    mesh = pv.PolyData(np.array(vertices, dtype=np.float32), np.array(faces, dtype=np.int64)).clean()
    log.info("Water mesh built: %d features, %d points, %d faces", water_count, mesh.n_points, mesh.n_cells)
    return mesh


def _sample_points_in_polygon(poly, n_points: int, rng: np.random.Generator, max_attempt_factor: int = 30) -> list[Point]:
    """Sample random points inside polygon by rejection sampling."""
    points: list[Point] = []
    if n_points <= 0 or poly.is_empty:
        return points
    minx, miny, maxx, maxy = poly.bounds
    attempts = 0
    max_attempts = max(n_points * max_attempt_factor, 100)
    while len(points) < n_points and attempts < max_attempts:
        x = float(rng.uniform(minx, maxx))
        y = float(rng.uniform(miny, maxy))
        p = Point(x, y)
        if poly.contains(p):
            points.append(p)
        attempts += 1
    return points


def generate_tree_mesh(
    osm_trees: gpd.GeoDataFrame,
    vegetation_polygons: gpd.GeoDataFrame,
    dem_path: Path,
    density_per_hectare: float = 20.0,
    max_trees: int = 8000,
) -> Tuple[pv.PolyData, gpd.GeoDataFrame]:
    """Generate lightweight tree meshes from OSM tree data + NDVI vegetation areas."""
    log = logging.getLogger("shaharsoz.trees")

    with rasterio.open(dem_path) as dem_src:
        dem = _sanitize_dem_array(dem_src.read(1), dem_src.nodata)
        finite = np.isfinite(dem)
        if finite.any():
            dem[~finite] = float(np.nanmin(dem[finite]))
        else:
            dem[:] = 0.0
        dem_transform = dem_src.transform

    rng = np.random.default_rng(42)
    tree_points: list[Point] = []

    if not osm_trees.empty:
        point_rows = osm_trees[osm_trees.geometry.geom_type.isin(["Point", "MultiPoint"])]
        for geom in point_rows.geometry:
            if geom.geom_type == "Point":
                tree_points.append(geom)
            else:
                tree_points.extend(list(geom.geoms))

        area_rows = osm_trees[osm_trees.geometry.geom_type.isin(["Polygon", "MultiPolygon"])]
        for geom in area_rows.geometry:
            polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
            for poly in polys:
                if poly.is_empty or not poly.is_valid:
                    continue
                n = int(max(0, round((poly.area / 10000.0) * density_per_hectare)))
                n = min(n, 1200)
                tree_points.extend(_sample_points_in_polygon(poly, n, rng))

    if not vegetation_polygons.empty:
        for geom in vegetation_polygons.geometry:
            polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
            for poly in polys:
                if poly.is_empty or not poly.is_valid:
                    continue
                # Lower density for NDVI-only polygons to avoid overpopulation.
                n = int(max(0, round((poly.area / 10000.0) * density_per_hectare * 0.3)))
                n = min(n, 1600)
                tree_points.extend(_sample_points_in_polygon(poly, n, rng))

    if not tree_points:
        return pv.PolyData(), gpd.GeoDataFrame(geometry=[], crs=TARGET_CRS)

    if len(tree_points) > max_trees:
        idx = rng.choice(len(tree_points), size=max_trees, replace=False)
        tree_points = [tree_points[int(i)] for i in idx]

    meshes: list[pv.PolyData] = []
    for p in tree_points:
        x, y = float(p.x), float(p.y)
        z = _sample_dem(dem, dem_transform, x, y)
        height = float(rng.uniform(4.0, 10.0))
        radius = float(rng.uniform(1.2, 2.4))
        trunk_h = height * 0.3
        trunk_r = max(0.12, radius * 0.12)

        trunk = pv.Cylinder(center=(x, y, z + trunk_h * 0.5), direction=(0, 0, 1), radius=trunk_r, height=trunk_h, resolution=8)
        canopy = pv.Cone(center=(x, y, z + trunk_h + (height - trunk_h) * 0.5), direction=(0, 0, 1), height=(height - trunk_h), radius=radius, resolution=8)
        meshes.append(trunk)
        meshes.append(canopy)

    tree_mesh = meshes[0]
    for m in meshes[1:]:
        tree_mesh = tree_mesh.merge(m, merge_points=False)
    tree_mesh = tree_mesh.extract_surface().triangulate().clean()
    tree_gdf = gpd.GeoDataFrame(geometry=tree_points, crs=TARGET_CRS)
    log.info("Tree mesh built: %d trees, %d points, %d faces", len(tree_points), tree_mesh.n_points, tree_mesh.n_cells)
    return tree_mesh, tree_gdf


def _blend_basemap_with_osm(
    basemap_path: Path,
    roads: gpd.GeoDataFrame,
    buildings: gpd.GeoDataFrame,
    out_png_path: Path,
) -> Path:
    """Bake an RGB texture by blending basemap with OSM road/building overlays."""
    with rasterio.open(basemap_path) as src:
        rgb = src.read([1, 2, 3]).astype(np.float32)
        transform = src.transform
        shape_hw = (src.height, src.width)

    roads_mask = np.zeros(shape_hw, dtype=np.uint8)
    if not roads.empty:
        widths = roads.apply(_road_width_m, axis=1).to_numpy()
        buffered = [geom.buffer(max(2.0, float(w) * 0.45), cap_style=2, join_style=2) for geom, w in zip(roads.geometry, widths)]
        roads_shapes = [(geom, 1) for geom in buffered if geom is not None and not geom.is_empty]
        if roads_shapes:
            roads_mask = features.rasterize(roads_shapes, out_shape=shape_hw, transform=transform, fill=0, dtype=np.uint8)

    bld_mask = np.zeros(shape_hw, dtype=np.uint8)
    if not buildings.empty:
        bld_shapes = [(geom, 1) for geom in buildings.geometry if geom is not None and not geom.is_empty]
        if bld_shapes:
            bld_mask = features.rasterize(bld_shapes, out_shape=shape_hw, transform=transform, fill=0, dtype=np.uint8)

    texture = rgb.copy()
    # Darken roads for readability in game engines.
    for band in range(3):
        texture[band][roads_mask == 1] *= 0.55
    # Slightly brighten rooftops to pop blockout massing.
    for band in range(3):
        texture[band][bld_mask == 1] = np.clip(texture[band][bld_mask == 1] * 1.15 + 12.0, 0, 255)

    out_png_path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "PNG",
        "height": shape_hw[0],
        "width": shape_hw[1],
        "count": 3,
        "dtype": "uint8",
    }
    with rasterio.open(out_png_path, "w", **profile) as dst:
        dst.write(np.clip(texture, 0, 255).astype(np.uint8))
    return out_png_path


def export_textured_terrain_obj(
    terrain_mesh: pv.PolyData,
    texture_png_path: Path,
    output_obj_path: Path,
) -> None:
    """Export terrain OBJ with UVs and MTL texture binding."""
    if terrain_mesh.n_points == 0:
        raise ShaharsozError("Cannot export textured terrain from empty mesh")

    points = np.asarray(terrain_mesh.points)
    faces_arr = terrain_mesh.faces.reshape((-1, 4))
    if faces_arr.size == 0:
        raise ShaharsozError("Terrain mesh has no faces for OBJ export")
    if not np.all(faces_arr[:, 0] == 3):
        terrain_mesh = terrain_mesh.triangulate()
        faces_arr = terrain_mesh.faces.reshape((-1, 4))

    minx, miny = float(points[:, 0].min()), float(points[:, 1].min())
    maxx, maxy = float(points[:, 0].max()), float(points[:, 1].max())
    dx = max(maxx - minx, 1e-6)
    dy = max(maxy - miny, 1e-6)
    u = (points[:, 0] - minx) / dx
    v = 1.0 - ((points[:, 1] - miny) / dy)

    output_obj_path.parent.mkdir(parents=True, exist_ok=True)
    mtl_path = output_obj_path.with_suffix(".mtl")
    tex_rel = texture_png_path.name
    with mtl_path.open("w", encoding="utf-8") as mtl:
        mtl.write("newmtl terrain_mat\n")
        mtl.write("Ka 1.000 1.000 1.000\n")
        mtl.write("Kd 1.000 1.000 1.000\n")
        mtl.write("Ks 0.000 0.000 0.000\n")
        mtl.write(f"map_Kd {tex_rel}\n")

    with output_obj_path.open("w", encoding="utf-8") as obj:
        obj.write(f"mtllib {mtl_path.name}\n")
        obj.write("o terrain_textured\n")
        for p in points:
            obj.write(f"v {p[0]:.6f} {p[1]:.6f} {p[2]:.6f}\n")
        for uu, vv in zip(u, v):
            obj.write(f"vt {uu:.6f} {vv:.6f}\n")
        obj.write("usemtl terrain_mat\n")
        for face in faces_arr[:, 1:4]:
            a, b, c = int(face[0]) + 1, int(face[1]) + 1, int(face[2]) + 1
            obj.write(f"f {a}/{a} {b}/{b} {c}/{c}\n")


def _find_material_texture(material_dir: Path, material_name: str) -> Path | None:
    """Locate material texture file by stem name with common image extensions."""
    if not material_dir.exists():
        return None
    for ext in (".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff"):
        candidate = material_dir / f"{material_name}{ext}"
        if candidate.exists():
            return candidate
    return None


def _face_projection_uv(points: np.ndarray, normal: np.ndarray, tile_m: float) -> np.ndarray:
    """Create UVs for 3 points using axis-projection chosen from face normal."""
    nx, ny, nz = abs(float(normal[0])), abs(float(normal[1])), abs(float(normal[2]))
    tile = max(tile_m, 0.25)
    if nz >= nx and nz >= ny:
        uv = np.stack((points[:, 0] / tile, points[:, 1] / tile), axis=1)
    elif nx >= ny:
        uv = np.stack((points[:, 1] / tile, points[:, 2] / tile), axis=1)
    else:
        uv = np.stack((points[:, 0] / tile, points[:, 2] / tile), axis=1)
    return uv.astype(np.float64)


def _iter_mesh_triangles(mesh: pv.PolyData) -> Iterable[np.ndarray]:
    """Yield triangle index arrays from a mesh."""
    if mesh.n_points == 0 or mesh.n_cells == 0:
        return
    tri = mesh.triangulate()
    faces_arr = tri.faces.reshape((-1, 4))
    for rec in faces_arr:
        if int(rec[0]) == 3:
            yield rec[1:4].astype(np.int64)


def export_textured_blockout_obj(
    components: dict[str, pv.PolyData],
    output_obj_path: Path,
    material_dir: Path,
    terrain_texture_path: Path | None = None,
    material_output_dir: Path | None = None,
) -> None:
    """
    Export a single textured blockout OBJ+MTL with per-component materials.

    Expected material filenames in material_dir:
    asphalt, cobblestone, concrete, grass, railway, roof, stone, wall_window, water, tree
    """
    material_order = [
        "terrain_sat",
        "asphalt",
        "cobblestone",
        "concrete",
        "grass",
        "railway",
        "roof",
        "stone",
        "wall_window",
        "water",
        "tree",
    ]
    tile_m = {
        "terrain_sat": 20.0,
        "asphalt": 4.0,
        "cobblestone": 2.0,
        "concrete": 5.0,
        "grass": 8.0,
        "railway": 3.0,
        "roof": 6.0,
        "stone": 2.0,
        "wall_window": 3.0,
        "water": 12.0,
        "tree": 2.0,
    }

    # Material assignment by component. Buildings are split by face orientation.
    component_material = {
        "terrain": "terrain_sat" if terrain_texture_path is not None else "grass",
        "roads": "asphalt",
        "bridges": "concrete",
        "water": "water",
        "railways": "railway",
        "trees": "tree",
    }

    output_obj_path.parent.mkdir(parents=True, exist_ok=True)
    if material_output_dir is None:
        material_output_dir = output_obj_path.parent
    material_output_dir.mkdir(parents=True, exist_ok=True)
    mtl_path = output_obj_path.with_suffix(".mtl")

    tex_paths: dict[str, Path | None] = {name: _find_material_texture(material_dir, name) for name in material_order}
    if terrain_texture_path is not None and terrain_texture_path.exists():
        tex_paths["terrain_sat"] = terrain_texture_path
    fallback = tex_paths.get("concrete")
    for name in material_order:
        if tex_paths[name] is None:
            tex_paths[name] = fallback

    with mtl_path.open("w", encoding="utf-8") as mtl:
        for name in material_order:
            mtl.write(f"newmtl {name}\n")
            mtl.write("Ka 1.000 1.000 1.000\n")
            mtl.write("Kd 1.000 1.000 1.000\n")
            mtl.write("Ks 0.000 0.000 0.000\n")
            tex = tex_paths[name]
            if tex is not None:
                tex_rel = Path(os.path.relpath(material_output_dir / tex.name, output_obj_path.parent)).as_posix()
                mtl.write(f"map_Kd {tex_rel}\n")
            mtl.write("\n")

    # Copy material textures into a dedicated materials directory.
    for _, tex in tex_paths.items():
        if tex is None:
            continue
        dst = material_output_dir / tex.name
        if not dst.exists():
            dst.write_bytes(tex.read_bytes())

    with output_obj_path.open("w", encoding="utf-8") as obj:
        obj.write(f"mtllib {mtl_path.name}\n")
        v_offset = 1
        vt_offset = 1

        for comp_name, mesh in components.items():
            if mesh is None or mesh.n_points == 0 or mesh.n_cells == 0:
                continue
            tri_mesh = mesh.triangulate()
            points = np.asarray(tri_mesh.points, dtype=np.float64)
            obj.write(f"o {comp_name}\n")
            for p in points:
                obj.write(f"v {p[0]:.6f} {p[1]:.6f} {p[2]:.6f}\n")

            if comp_name == "buildings":
                # Split by roof/wall based on face normal.
                roof_faces: list[tuple[np.ndarray, np.ndarray]] = []
                wall_faces: list[tuple[np.ndarray, np.ndarray]] = []
                floor_faces: list[tuple[np.ndarray, np.ndarray]] = []
                z_min = float(points[:, 2].min())
                z_max = float(points[:, 2].max())
                z_span = max(0.01, z_max - z_min)
                for ids in _iter_mesh_triangles(tri_mesh):
                    tri_pts = points[ids]
                    n = np.cross(tri_pts[1] - tri_pts[0], tri_pts[2] - tri_pts[0])
                    norm = float(np.linalg.norm(n))
                    if norm > 1e-9:
                        n = n / norm
                    else:
                        n = np.array([0.0, 0.0, 1.0], dtype=np.float64)
                    z_mean = float(np.mean(tri_pts[:, 2]))
                    z_rel = (z_mean - z_min) / z_span
                    if abs(float(n[2])) > 0.6 and z_rel >= 0.70:
                        roof_faces.append((ids, n))
                    elif abs(float(n[2])) > 0.6 and z_rel <= 0.20:
                        floor_faces.append((ids, n))
                    else:
                        wall_faces.append((ids, n))

                for mat_name, face_group in (("roof", roof_faces), ("wall_window", wall_faces), ("concrete", floor_faces)):
                    if not face_group:
                        continue
                    obj.write(f"usemtl {mat_name}\n")
                    for ids, n in face_group:
                        tri_pts = points[ids]
                        uv = _face_projection_uv(tri_pts, n, tile_m[mat_name])
                        obj.write(f"vt {uv[0,0]:.6f} {uv[0,1]:.6f}\n")
                        obj.write(f"vt {uv[1,0]:.6f} {uv[1,1]:.6f}\n")
                        obj.write(f"vt {uv[2,0]:.6f} {uv[2,1]:.6f}\n")
                        a, b, c = (int(ids[0]) + v_offset, int(ids[1]) + v_offset, int(ids[2]) + v_offset)
                        ta, tb, tc = vt_offset, vt_offset + 1, vt_offset + 2
                        obj.write(f"f {a}/{ta} {b}/{tb} {c}/{tc}\n")
                        vt_offset += 3
            else:
                mat_name = component_material.get(comp_name, "concrete")
                obj.write(f"usemtl {mat_name}\n")
                for ids in _iter_mesh_triangles(tri_mesh):
                    tri_pts = points[ids]
                    n = np.cross(tri_pts[1] - tri_pts[0], tri_pts[2] - tri_pts[0])
                    norm = float(np.linalg.norm(n))
                    if norm > 1e-9:
                        n = n / norm
                    else:
                        n = np.array([0.0, 0.0, 1.0], dtype=np.float64)
                    uv = _face_projection_uv(tri_pts, n, tile_m.get(mat_name, 4.0))
                    obj.write(f"vt {uv[0,0]:.6f} {uv[0,1]:.6f}\n")
                    obj.write(f"vt {uv[1,0]:.6f} {uv[1,1]:.6f}\n")
                    obj.write(f"vt {uv[2,0]:.6f} {uv[2,1]:.6f}\n")
                    a, b, c = (int(ids[0]) + v_offset, int(ids[1]) + v_offset, int(ids[2]) + v_offset)
                    ta, tb, tc = vt_offset, vt_offset + 1, vt_offset + 2
                    obj.write(f"f {a}/{ta} {b}/{tb} {c}/{tc}\n")
                    vt_offset += 3

            v_offset += points.shape[0]

def export_obj(mesh: pv.PolyData, output_path: Path) -> None:
    """Export a mesh as OBJ."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if mesh.n_points == 0:
        output_path.write_text("# empty mesh\n", encoding="utf-8")
        return
    mesh.save(str(output_path))


def _export_lods_and_collision(base_mesh: pv.PolyData, lod_dir: Path, collision_dir: Path) -> Tuple[Path, Path, Path]:
    """Export simplified collision + LOD meshes for game engine workflows."""
    collision_dir.mkdir(parents=True, exist_ok=True)
    lod_dir.mkdir(parents=True, exist_ok=True)
    collision_path = collision_dir / "collision.obj"
    lod1_path = lod_dir / "blockout_lod1.obj"
    lod2_path = lod_dir / "blockout_lod2.obj"

    mesh = base_mesh.extract_surface(algorithm="dataset_surface").triangulate().clean()
    collision = mesh.decimate_pro(0.92, preserve_topology=True).clean()
    lod1 = mesh.decimate_pro(0.50, preserve_topology=True).clean()
    lod2 = mesh.decimate_pro(0.75, preserve_topology=True).clean()

    export_obj(collision, collision_path)
    export_obj(lod1, lod1_path)
    export_obj(lod2, lod2_path)
    return collision_path, lod1_path, lod2_path


def _write_geojson(gdf: gpd.GeoDataFrame, output_path: Path) -> None:
    """Write GeoJSON, including valid empty FeatureCollection output."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if gdf.empty:
        empty_fc = {"type": "FeatureCollection", "features": []}
        output_path.write_text(json.dumps(empty_fc, indent=2), encoding="utf-8")
        return
    gdf[["geometry"]].to_file(output_path, driver="GeoJSON")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SHAHARSOZ: procedural GIS-driven 3D city blockout generator for architecture and urbanism"
    )
    parser.add_argument("--version", action="version", version=f"SHAHARSOZ {get_project_version()}")
    parser.add_argument("--lat", required=True, type=float, help="Latitude in decimal degrees")
    parser.add_argument("--lon", required=True, type=float, help="Longitude in decimal degrees")
    parser.add_argument("--radius", required=True, type=float, help="Radius in kilometers")
    parser.add_argument(
        "--output",
        required=True,
        type=str,
        help="Output folder name under ./output (example: tashkent_model)",
    )
    parser.add_argument(
        "--dem-resolution",
        type=float,
        default=2.0,
        help="Preferred DEM pixel size in meters for high-resolution sources (default: 2.0)",
    )
    parser.add_argument(
        "--basemap-resolution",
        type=float,
        default=1.0,
        help="Target basemap texture resolution in meters (default: 1.0, capped by provider)",
    )
    parser.add_argument(
        "--wayback-layer-id",
        type=int,
        default=64001,
        help="Esri Wayback layer id for imagery snapshot (default: 64001). Set 0 to use current World Imagery.",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="input",
        help="Folder with optional screenshot textures; file named <output>.* is used first (default: ./input)",
    )
    parser.add_argument(
        "--material-dir",
        type=str,
        default="input/materials",
        help="Folder containing tiling textures (asphalt, cobblestone, concrete, grass, railway, roof, stone, wall_window, water, tree)",
    )
    parser.add_argument(
        "--tree-density",
        type=float,
        default=20.0,
        help="Tree density per hectare for polygon-based spawning (default: 20)",
    )
    parser.add_argument(
        "--max-trees",
        type=int,
        default=8000,
        help="Maximum number of spawned trees (default: 8000)",
    )
    parser.add_argument(
        "--ndwi-threshold",
        type=float,
        default=0.12,
        help="NDWI threshold for Sentinel-derived water mask (default: 0.12)",
    )
    parser.add_argument(
        "--no-input-texture",
        action="store_true",
        help="Disable local screenshot texture lookup from --input-dir",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()
    configure_logging(args.verbose)

    root = Path(__file__).resolve().parents[1]
    data_dir = root / "data" / args.output
    output_dir = root / "output" / args.output
    input_dir = root / args.input_dir
    material_dir = root / args.material_dir

    data_dem_dir = data_dir / "dem"
    data_imagery_dir = data_dir / "imagery"
    data_masks_dir = data_dir / "masks"
    data_cache_dir = data_dir / "cache"

    output_meshes_dir = output_dir / "meshes"
    output_textures_dir = output_dir / "textures"
    output_materials_dir = output_dir / "materials"
    output_vectors_dir = output_dir / "vectors"
    output_lod_dir = output_dir / "lod"
    output_collision_dir = output_dir / "collision"

    for p in (
        data_dem_dir,
        data_imagery_dir,
        data_masks_dir,
        data_cache_dir,
        output_meshes_dir,
        output_textures_dir,
        output_materials_dir,
        output_vectors_dir,
        output_lod_dir,
        output_collision_dir,
    ):
        p.mkdir(parents=True, exist_ok=True)

    try:
        buildings, roads, railways, water = get_osm_data(args.lat, args.lon, args.radius)
        osm_trees = get_osm_tree_data(args.lat, args.lon, args.radius)
        dem_path = get_dem_data(args.lat, args.lon, args.radius, data_dem_dir, dem_resolution_m=args.dem_resolution)
        basemap_path = None
        if not args.no_input_texture:
            try:
                basemap_path = get_input_texture(
                    lat=args.lat,
                    lon=args.lon,
                    radius_km=args.radius,
                    input_dir=input_dir,
                    data_dir=data_imagery_dir,
                    model_name=args.output,
                    wayback_layer_id=None if args.wayback_layer_id == 0 else args.wayback_layer_id,
                )
            except Exception as exc:
                logging.getLogger("shaharsoz.input_texture").warning("Input texture skipped: %s", exc)

        if basemap_path is None:
            try:
                basemap_path = get_basemap_texture(
                    args.lat,
                    args.lon,
                    args.radius,
                    data_imagery_dir,
                    texture_resolution_m=args.basemap_resolution,
                    wayback_layer_id=None if args.wayback_layer_id == 0 else args.wayback_layer_id,
                )
            except Exception as exc:
                logging.getLogger("shaharsoz.basemap").warning("Basemap texture skipped: %s", exc)
        red_path, nir_path = get_sentinel_data(args.lat, args.lon, args.radius, data_imagery_dir)
        vegetation = compute_ndvi(nir_path=nir_path, red_path=red_path, data_dir=data_masks_dir)
        ndwi_water = compute_ndwi_water(
            green_path=data_imagery_dir / "sentinel_b03_3857.tif",
            nir_path=nir_path,
            data_dir=data_masks_dir,
            threshold=args.ndwi_threshold,
        )
        if not ndwi_water.empty:
            merged_water_geoms = list(water.geometry) + list(ndwi_water.geometry)
            water = gpd.GeoDataFrame(geometry=merged_water_geoms, crs=TARGET_CRS)
            water = water.dissolve().explode(index_parts=False).reset_index(drop=True)
            water = water[water.geometry.notna()].copy()
            water = water[water.geometry.is_valid].copy()

        terrain_mesh = create_terrain_mesh(dem_path)
        building_mesh = extrude_buildings(buildings, dem_path)
        road_mesh = extrude_roads(roads, dem_path)
        rail_mesh = extrude_railways(railways, dem_path)
        bridge_mesh = extrude_bridges(roads, dem_path)
        water_mesh = extrude_water(water, dem_path)
        trees_mesh, tree_points = generate_tree_mesh(
            osm_trees=osm_trees,
            vegetation_polygons=vegetation,
            dem_path=dem_path,
            density_per_hectare=args.tree_density,
            max_trees=args.max_trees,
        )

        terrain_obj = output_meshes_dir / "terrain.obj"
        terrain_textured_obj = output_meshes_dir / "terrain_textured.obj"
        buildings_obj = output_meshes_dir / "buildings.obj"
        roads_obj = output_meshes_dir / "roads.obj"
        railways_obj = output_meshes_dir / "railways.obj"
        bridges_obj = output_meshes_dir / "bridges.obj"
        water_obj = output_meshes_dir / "water.obj"
        trees_obj = output_meshes_dir / "trees.obj"
        blockout_obj = output_meshes_dir / "blockout.obj"
        blockout_textured_obj = output_meshes_dir / "blockout_textured.obj"
        roads_geojson = output_vectors_dir / "roads.geojson"
        railways_geojson = output_vectors_dir / "railways.geojson"
        water_geojson = output_vectors_dir / "water.geojson"
        trees_geojson = output_vectors_dir / "trees.geojson"
        vegetation_geojson = output_vectors_dir / "vegetation.geojson"

        export_obj(terrain_mesh, terrain_obj)
        export_obj(building_mesh, buildings_obj)
        export_obj(road_mesh, roads_obj)
        export_obj(rail_mesh, railways_obj)
        export_obj(bridge_mesh, bridges_obj)
        export_obj(water_mesh, water_obj)
        export_obj(trees_mesh, trees_obj)

        combined_mesh = terrain_mesh.copy()
        if building_mesh.n_points > 0:
            combined_mesh = combined_mesh.merge(building_mesh, merge_points=False)
        if road_mesh.n_points > 0:
            combined_mesh = combined_mesh.merge(road_mesh, merge_points=False)
        if rail_mesh.n_points > 0:
            combined_mesh = combined_mesh.merge(rail_mesh, merge_points=False)
        if bridge_mesh.n_points > 0:
            combined_mesh = combined_mesh.merge(bridge_mesh, merge_points=False)
        if water_mesh.n_points > 0:
            combined_mesh = combined_mesh.merge(water_mesh, merge_points=False)
        if trees_mesh.n_points > 0:
            combined_mesh = combined_mesh.merge(trees_mesh, merge_points=False)
        for arr_name in ("vtkOriginalPointIds", "vtkOriginalCellIds"):
            if arr_name in combined_mesh.point_data:
                combined_mesh.point_data.pop(arr_name)
            if arr_name in combined_mesh.cell_data:
                combined_mesh.cell_data.pop(arr_name)
        combined_mesh = (
            combined_mesh.extract_surface(
                pass_pointid=False,
                pass_cellid=False,
                algorithm="dataset_surface",
            )
            .triangulate()
            .clean()
        )
        export_obj(combined_mesh, blockout_obj)
        collision_obj, lod1_obj, lod2_obj = _export_lods_and_collision(
            combined_mesh,
            lod_dir=output_lod_dir,
            collision_dir=output_collision_dir,
        )

        terrain_texture_png: Path | None = None
        if basemap_path is not None and basemap_path.exists():
            texture_png = _blend_basemap_with_osm(
                basemap_path=basemap_path,
                roads=roads,
                buildings=buildings,
                out_png_path=output_textures_dir / "terrain_texture.png",
            )
            terrain_texture_png = texture_png
            export_textured_terrain_obj(
                terrain_mesh=terrain_mesh,
                texture_png_path=texture_png,
                output_obj_path=terrain_textured_obj,
            )
        elif (data_imagery_dir / "sentinel_rgb_preview.tif").exists():
            texture_png = _blend_basemap_with_osm(
                basemap_path=data_imagery_dir / "sentinel_rgb_preview.tif",
                roads=roads,
                buildings=buildings,
                out_png_path=output_textures_dir / "terrain_texture.png",
            )
            terrain_texture_png = texture_png
            export_textured_terrain_obj(
                terrain_mesh=terrain_mesh,
                texture_png_path=texture_png,
                output_obj_path=terrain_textured_obj,
            )

        export_textured_blockout_obj(
            components={
                "terrain": terrain_mesh,
                "buildings": building_mesh,
                "roads": road_mesh,
                "railways": rail_mesh,
                "bridges": bridge_mesh,
                "water": water_mesh,
                "trees": trees_mesh,
            },
            output_obj_path=blockout_textured_obj,
            material_dir=material_dir,
            terrain_texture_path=terrain_texture_png,
            material_output_dir=output_materials_dir,
        )

        _write_geojson(roads, roads_geojson)
        _write_geojson(railways, railways_geojson)
        _write_geojson(water, water_geojson)
        _write_geojson(tree_points, trees_geojson)
        _write_geojson(vegetation, vegetation_geojson)

        logging.getLogger("shaharsoz").info("Pipeline complete")
        logging.getLogger("shaharsoz").info("Terrain OBJ: %s", terrain_obj)
        logging.getLogger("shaharsoz").info("Terrain Textured OBJ: %s", terrain_textured_obj)
        logging.getLogger("shaharsoz").info("Buildings OBJ: %s", buildings_obj)
        logging.getLogger("shaharsoz").info("Roads OBJ: %s", roads_obj)
        logging.getLogger("shaharsoz").info("Railways OBJ: %s", railways_obj)
        logging.getLogger("shaharsoz").info("Bridges OBJ: %s", bridges_obj)
        logging.getLogger("shaharsoz").info("Water OBJ: %s", water_obj)
        logging.getLogger("shaharsoz").info("Trees OBJ: %s", trees_obj)
        logging.getLogger("shaharsoz").info("Blockout OBJ: %s", blockout_obj)
        logging.getLogger("shaharsoz").info("Collision OBJ: %s", collision_obj)
        logging.getLogger("shaharsoz").info("LOD1 OBJ: %s", lod1_obj)
        logging.getLogger("shaharsoz").info("LOD2 OBJ: %s", lod2_obj)
        logging.getLogger("shaharsoz").info("Blockout Textured OBJ: %s", blockout_textured_obj)
        logging.getLogger("shaharsoz").info("Roads GeoJSON: %s", roads_geojson)
        logging.getLogger("shaharsoz").info("Railways GeoJSON: %s", railways_geojson)
        logging.getLogger("shaharsoz").info("Water GeoJSON: %s", water_geojson)
        logging.getLogger("shaharsoz").info("Trees GeoJSON: %s", trees_geojson)
        logging.getLogger("shaharsoz").info("Vegetation GeoJSON: %s", vegetation_geojson)
        logging.getLogger("shaharsoz").info("Intermediate rasters saved in: %s", data_dir)

    except ShaharsozError as exc:
        logging.getLogger("shaharsoz").error("Pipeline failed: %s", exc)
        raise SystemExit(1) from exc
    except Exception as exc:  # pragma: no cover
        logging.getLogger("shaharsoz").exception("Unhandled failure: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
