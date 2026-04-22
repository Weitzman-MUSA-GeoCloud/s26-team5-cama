
"""
HTTP Cloud Function to export property tile information from BigQuery to Cloud Storage.

This function queries the BigQuery dataset for property tile information and exports the results as a GeoJSON
file to a `musa5090s26-team5-temp_data` Cloud Storage bucket.

Usage:
    Deploy as a Cloud Function named "export-property-tile-info"
"""


import functions_framework
import json
from google.cloud import bigquery
from google.cloud import storage
import os
from dotenv import load_dotenv
import pathlib


@functions_framework.http
def export_property_tile_info(request):
    try: 
        temp_data_bucket = os.getenv("TEMP_DATA_BUCKET", "musa5090s26-team5-temp_data")

        # Call the SQL file to get the property tile information.
        DIR_NAME = pathlib.Path(__file__).parent
        sql = (DIR_NAME / "property_tile_info.sql").read_text()
        print("Executing property_tile_info.sql...")

        # Activate BigQuery client and run the query! 
        client = bigquery.Client()

        job = client.query(sql)
        results = job.result()
        print("Query executed successfully.")

        # Build GeoJSON FeatureCollection from query results.
        features = []
        for row in results:
            geometry = json.loads(row.geometry)
            # Exclude the geometry column from properties to avoid duplication in the GeoJSON.
            # Once the columns are read into a dictionary, we can filter out the geometry column and use the rest as properties.
            properties =  {key: value for (key, value) in row.items() if key != "geometry"}
            feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            }
            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "features": features,
            }
        print(f"Built FeatureCollection with {len(features)} features.")

        storage_client = storage.Client()
        bucket = storage_client.bucket(temp_data_bucket)
        blob = bucket.blob("property_tile_info.geojson")
        blob.upload_from_string(
            json.dumps(geojson),
            content_type="application/json",
        )
        print(f"Uploaded to gs://{temp_data_bucket}/property_tile_info.geojson")

        return ("Successfully exported property tile info GeoJSON.", 200) 

    except Exception as e:
        print(f"Error: {e}")
        return (f"Error exporting property tile info: {e}", 500)