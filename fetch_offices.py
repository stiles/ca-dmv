
import requests
import pandas as pd
import altair as alt
import geopandas as gpd
from bs4 import BeautifulSoup
from urllib.parse import quote

headers = {
    "accept": "*/*",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
}

city_zip_response = requests.get(
    "https://www.dmv.ca.gov/portal/wp-json/dmv/v1/cities", headers=headers
)
city_zip_list_all = city_zip_response.json()

# Construct URLs for field office searches
base_url = (
    "https://www.dmv.ca.gov/portal/locations/?q={}&c={}&z={}&services=field-office"
)
urls = [
    base_url.format(quote(city_zip), quote(city_zip.split()[0]), city_zip.split()[1])
    for city_zip in city_zip_list_all
]

# List to store all extracted details across all URLs
card_list = []

# Loop through each URL and extract data, including handling pagination
for original_url in urls: 
    page_number = 1
    no_more_pages = False  

    while not no_more_pages:
        # Construct the paginated URL properly
        if page_number == 1:
            url = original_url
        else:
            # Construct the URL for subsequent pages by inserting /page/{page_number}/ before the query parameters
            base, params = original_url.split("?")
            url = f"{base}page/{page_number}/?{params}"

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve {url} with status code {response.status_code}")
            break

        # Parse the HTML content
        html_content = BeautifulSoup(response.text, "html.parser")

        # Find all cards on the page
        cards = html_content.find_all(
            "li", class_="location-results__list-item search-card"
        )

        # If no cards are found or "No Locations found" is present, set the flag to stop pagination
        if not cards or any(
            "No Locations found" in card.get_text(strip=True) for card in cards
        ):
            # print(f"No locations found on page {page_number} for URL: {url}")
            no_more_pages = True
            break

        # Extract meaningful details from each card
        for card in cards:
            # Skip the card if it contains the "No Locations found" message
            if "No Locations found" in card.get_text(strip=True):
                continue

            # Extract name safely
            name_tag = card.find("h3", class_="search-card__title")
            name = name_tag.get_text(strip=True) if name_tag else None

            # Extract location type, excluding any nested tags
            location_type_tag = card.find("p", class_="search-card__type-label")
            location_type = (
                "".join(
                    element
                    for element in location_type_tag.contents
                    if isinstance(element, str)
                ).strip()
                if location_type_tag
                else None
            )

            # Extract services, if available, into a list
            services = (
                [
                    span.get_text(strip=True)
                    for span in location_type_tag.find_all(
                        "span", class_="kiosk-callout"
                    )
                ]
                if location_type_tag
                else []
            )

            # Extract detail URL
            detail_url = card.get("data-detail-url")

            # Extract latitude and longitude
            lat = card.get("data-lat")
            lng = card.get("data-lng")

            # Extract address components
            address_tag = card.find(itemprop="address")
            street_address = (
                address_tag.find(itemprop="streetAddress").get_text(strip=True)
                if address_tag and address_tag.find(itemprop="streetAddress")
                else None
            )
            locality = (
                address_tag.find(itemprop="addressLocality").get_text(strip=True)
                if address_tag and address_tag.find(itemprop="addressLocality")
                else None
            )
            region = (
                address_tag.find(itemprop="addressRegion").get_text(strip=True)
                if address_tag and address_tag.find(itemprop="addressRegion")
                else None
            )
            postal_code = (
                address_tag.find(itemprop="postalCode").get_text(strip=True)
                if address_tag and address_tag.find(itemprop="postalCode")
                else None
            )
            full_address = (
                f"{street_address}, {locality}, {region} {postal_code}".strip(", ")
            )

            # Extract opening hours
            opening_hours_meta = card.find("meta", itemprop="openingHours")
            opening_hours = (
                opening_hours_meta.get("content")
                if opening_hours_meta
                else "Hours not available"
            )

            # Append the extracted details to the list
            card_list.append(
                {
                    "name": name,
                    "type": location_type,
                    "latitude": lat,
                    "longitude": lng,
                    "url": detail_url,
                    "address": full_address,
                    "hours": opening_hours,
                    "services": services,
                }
            )

        # Increment page number for next iteration
        page_number += 1


        src = (
    pd.DataFrame(card_list)
    .drop_duplicates(subset="url")
    .rename(columns={"name": "place"})
)
        

src[["address", "drop", "city", "drop", "state", "drop"]] = src["address"].str.split(
    ",", expand=True
)

src[["state", "zip"]] = src["state"].str.strip().str.split(" ", expand=True)

df = src.drop(["type", "drop"], axis=1)

gdf = gpd.GeoDataFrame(
    df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
)

field_office_urls = gdf["url"].to_list()


ca_counties = gpd.read_file(
    "https://stilesdata.com/gis/usa_counties_esri_simple_mainland.json"
).query('state_name=="California"')

dmv_locations_gdf = (
    gpd.sjoin(gdf, ca_counties[["geometry", "name"]])
    .reset_index(drop=True)
    .drop(["index_right"], axis=1)
    .copy()
)

df.to_csv("data/raw/dmv_locations.csv", index=False)
df.to_json("data/raw/dmv_locations.json", indent=4, orient="records")

dmv_locations_gdf.to_file("data/geo/dmv_locations.geojson", driver="GeoJSON")