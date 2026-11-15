#!/usr/bin/env python3
"""Inventory workspace FIRMS CSV/ZIP metadata without extracting archives."""
import csv, json, zipfile
from pathlib import Path

def main(root='.', output='outputs/input_inventory.json'):
    root=Path(root).resolve(); out=root/output; out.parent.mkdir(parents=True,exist_ok=True)
    csvs=[]; schema=set()
    for p in sorted(root.glob('*.csv')):
        with p.open('r',encoding='utf-8-sig',newline='') as f:
            reader=csv.reader(f); header=next(reader,[])
        schema.add(tuple(header))
        csvs.append({'name':p.name,'bytes':p.stat().st_size,'classification':'near-real-time point detections' if p.name.startswith('fire_nrt_') else 'finalized archive point detections' if p.name.startswith('fire_archive_') else 'other CSV','columns':header})
    zips=[]
    for p in sorted(root.glob('*.zip')):
        with zipfile.ZipFile(p) as z:
            members=[i for i in z.infolist() if not i.is_dir()]
            names=[i.filename for i in members]
            same_name=[n for n in names if (root/n).is_file()]
            country_year=any('/modis/' in '/'+n.lower() and 'all_countries' in p.name.lower() for n in names)
            zips.append({'name':p.name,'bytes':p.stat().st_size,'member_count':len(members),'uncompressed_bytes':sum(i.file_size for i in members),'kind':'country-year MODIS point CSV collection' if country_year else 'downloaded archive CSV copy' if any(n.startswith(('fire_archive_','fire_nrt_')) for n in names) else 'other','members_matching_existing_path':same_name,'sample_members':names[:3]})
    inventory={'workspace':str(root),'top_level_csv_count':len(csvs),'top_level_csvs':csvs,'distinct_csv_schemas':[list(s) for s in schema],'zip_count':len(zips),'archives':zips,'notes':['CSV headers and ZIP central directories inspected without extracting ZIPs.','Rows with latitude/longitude in country-year collections are point observations, not aggregate country statistics.','Matching member paths/names indicate potential copied datasets; byte-level hashes were not calculated.','MAP_KEY intentionally not inspected or included.']}
    out.write_text(json.dumps(inventory,indent=2)); print(json.dumps({'csv_count':len(csvs),'zip_count':len(zips),'schemas':[list(x) for x in schema],'inventory':str(out)},indent=2))
if __name__=='__main__': main()
