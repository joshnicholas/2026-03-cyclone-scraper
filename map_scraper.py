# %%
import pandas as pd

import requests
import datetime
import pytz
import re
import xml.etree.ElementTree as ET
import os
import pathlib
import json
import boto3
import ast

from sudulunu.helpers import pp, dumper

# %%
def syncData(jsonObject, path, filename):

    AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

    if 'AWS_SESSION_TOKEN' in os.environ:
        AWS_SESSION = os.environ['AWS_SESSION_TOKEN']

    print("Connecting to S3")
    bucket = 'gdn-cdn'

    if 'AWS_SESSION_TOKEN' in os.environ:
        session = boto3.Session(
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET,
            aws_session_token=AWS_SESSION
        )
    else:
        session = boto3.Session(
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET,
        )

    s3 = session.resource('s3')

    key = "{path}/{filename}".format(path=path, filename=filename)
    obj = s3.Object(bucket, key)
    obj.put(
        Body=jsonObject,
        CacheControl="max-age=30",
        ACL='public-read',
        ContentType="application/json"
    )

    print("JSON is updated")
    print("data", "https://interactive.guim.co.uk/{path}/{filename}".format(path=path, filename=filename))


# %%
today = datetime.datetime.now()
scrape_date_stemmo = today.astimezone(
    pytz.timezone("Australia/Brisbane")
).strftime('%Y%m%d%H')

# %%
pathos = pathlib.Path(__file__).parent
os.chdir(pathos)

# %%
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Referer": "https://www.google.com",
    "DNT": "1"
}

# %%
urlo = 'https://www.bom.gov.au/fwo/IDQ65002.xml'
r = requests.get(urlo, headers=headers)
root = ET.fromstring(r.text)

def parse_coord(value):
    value = value.strip().upper()
    number = float(value[:-1])
    direction = value[-1]

    if direction in {"S", "W"}:
        number *= -1

    return number

def text_at(path):
    node = root.find(path)
    return node.text.strip() if node is not None and node.text else None

forecast = root.find(".//forecast-period")

elements = {
    el.get("type"): (el.text.strip() if el.text else "")
    for el in root.findall(".//forecast-period/element[@type]")
}

wanted = {
    "issue-time-utc": text_at(".//issue-time-utc"),
    "warning_next_issue": text_at(".//warning-info/text[@type='warning_next_issue']"),
    "forecast_period_start_time_utc": forecast.get("start-time-utc") if forecast is not None else None,
    "forecast_period_end_time_utc": forecast.get("end-time-utc") if forecast is not None else None,
    "cyclone_name": elements.get("cyclone_name"),
    "analysis_time": elements.get("analysis_time"),
    "intensity": elements.get("intensity"),
    "location": elements.get("location"),
    "nearby_town_1": elements.get("nearby_town_1"),
    "nearby_town_2": elements.get("nearby_town_2"),
    "movement": elements.get("movement"),
}

forecast_points = {}
for key, value in elements.items():
    m = re.fullmatch(r"(date|category_name|latitude|longitude|position_acc_km)_plus(\d+)", key)
    if m:
        field, hour = m.groups()
        forecast_points.setdefault(hour, {})[field] = value

wanted["forecast_points"] = [
    {"plus_hours": int(hour), **data}
    for hour, data in sorted(forecast_points.items(), key=lambda x: int(x[0]))
]

for point in wanted["forecast_points"]:
    if "latitude" in point and point["latitude"]:
        point["latitude"] = parse_coord(point["latitude"])
    if "longitude" in point and point["longitude"]:
        point["longitude"] = parse_coord(point["longitude"])

wanted["hazards"] = []
for hazard in root.findall(".//hazard"):
    wkt_node = hazard.find("./text[@type='warning_area_polygons']")
    warning_areas_node = hazard.find("./text[@type='warning_areas']")

    wanted["hazards"].append({
        **hazard.attrib,
        "warning_areas": warning_areas_node.text.strip() if warning_areas_node is not None and warning_areas_node.text else None,
        "warning_area_polygons": wkt_node.text.strip() if wkt_node is not None and wkt_node.text else None
    })

print(wanted['forecast_points'])

# %%

points = pd.DataFrame.from_records(wanted['forecast_points'])
points['Current'] = True
points = points.loc[points['plus_hours'] == 0]
# print(points)

exclude = points['date'].unique().tolist()


iterrer = pathlib.Path('input/map_scrape')
fillos = list(iterrer.rglob("*.json"))

records = []

for fillo in fillos:
    with open(fillo, 'r') as f:
        jsony = json.loads(f.read())
        # print(jsony.keys()) 
        # print(jsony['forecast_points'])
        records.extend(jsony['forecast_points'])

# print(records)
old = pd.DataFrame.from_records(records)
old = old.loc[old['plus_hours'] == 0]
old = old.loc[~old['date'].isin(exclude)]

old = old.to_json(orient='records')

wanted['historic']= ast.literal_eval(old)

# print(old)


# %%

# %%

print(json.dumps(wanted, indent=2))

with open(f'input/map_scrape/{scrape_date_stemmo}.json', 'w') as f:
    json.dump(wanted, f, indent=4)

# %%
print(type(wanted))

# %%
jsony = json.dumps(wanted)
syncData(jsony, "26/03/19-oz-narelle-cyclone", "cyclone-warning-tracker-map.json")