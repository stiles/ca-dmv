import requests
import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup

# Headers setup
headers = {
    "accept": "*/*",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
}

now = pd.Timestamp.now().strftime("%Y-%m-%d %H:00:00")

archive_df = pd.read_json('https://stilesdata.com/ca-dmv/processed/archive/wait_times.json')

# Load field office URLs
df = pd.read_json("data/raw/dmv_locations.json")
field_office_urls = df["url"].to_list()

# List to store extracted wait times
times_dicts_list = []

# Loop through each URL and extract wait times
for url in field_office_urls:
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract appointment and non-appointment wait times safely
    try:
        appt_time_text = soup.find_all("span", class_="p medium")[0].text.strip()
        appt_time = (
            float(appt_time_text) if appt_time_text.lower() != "closed" else pd.NaT
        )
    except (IndexError, ValueError):
        appt_time = pd.NaT

    try:
        no_appt_time_text = soup.find_all("span", class_="p medium")[1].text.strip()
        no_appt_time = (
            float(no_appt_time_text)
            if no_appt_time_text.lower() != "closed"
            else pd.NaT
        )
    except (IndexError, ValueError):
        no_appt_time = pd.NaT

    times_dicts_list.append(
        {
            "location": url.strip().split("/")[5],
            "type": url.strip().split("/")[4],
            "appt_wait": appt_time,
            "no_appt_wait": no_appt_time,
            "captured": now,
        }
    )

# Optionally convert the list to a DataFrame or save it as needed
wait_times_df = pd.DataFrame(times_dicts_list)

wait_times_df.to_csv('data/processed/wait_times.csv', index=False)
wait_times_df.to_json('data/processed/wait_times.json', indent=4, orient='records')