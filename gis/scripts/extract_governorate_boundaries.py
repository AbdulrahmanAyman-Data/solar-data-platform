import geopandas as gpd
import pandas as pd
import json

NAME_TO_LOCATION_ID = {
    "Al Qahirah": "cairo",
    "Al Iskandariyah": "alexandria",
    "Bur Sa`id": "port-said",
    "As Suways": "suez",
    "Dumyat": "damietta",
    "Ad Daqahliyah": "dakahlia",
    "Ash Sharqiyah": "sharqia",
    "Al Qalyubiyah": "qalyubia",
    "Kafr ash Shaykh": "kafr-el-sheikh",
    "Al Gharbiyah": "gharbia",
    "Al Minufiyah": "monufia",
    "Al Buhayrah": "beheira",
    "Al Isma`iliyah": "ismailia",
    "Al Jizah": "giza",
    "Bani Suwayf": "beni-suef",
    "Al Fayyum": "faiyum",
    "Al Minya": "minya",
    "Asyut": "asyut",
    "Suhaj": "sohag",
    "Qina": "qena",
    "Al Uqsur": "luxor",
    "Aswan": "aswan",
    "Al Bahr al Ahmar": "red-sea",
    "Al Wadi al Jadid": "new-valley",
    "Matrouh": "matrouh",
    "Shamal Sina'": "north-sinai",
    "Janub Sina'": "south-sinai",
}

gdf = gpd.read_file("/opt/gis/data/raw/EGY_adm/EGY_adm1_fixed.gpkg")
gdf = gdf.to_crs(epsg=4326)
gdf["geometry"] = gdf["geometry"].simplify(0.001, preserve_topology=True)

rows = []
unmatched = []
for _, row in gdf.iterrows():
    loc_id = NAME_TO_LOCATION_ID.get(row["NAME_1"])
    if loc_id is None:
        unmatched.append(row["NAME_1"])
        continue
    rows.append({
        "location_id": loc_id,
        "geometry_wkt": row["geometry"].wkt,
        "geometry_geojson": json.dumps(row["geometry"].__geo_interface__),
    })

if unmatched:
    print("⚠️ أسامي ما اتلاقاش ليها location_id:", unmatched)

out = pd.DataFrame(rows)
assert len(out) == 27, f"المفروض 27 صف، طلع {len(out)}"

import os
os.makedirs("/opt/gis/data/processed", exist_ok=True)
out.to_parquet("/opt/gis/data/processed/governorate_boundaries.parquet", index=False)
print(f"✅ wrote {len(out)} row in /opt/gis/data/processed/governorate_boundaries.parquet")