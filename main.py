from flask import Flask, request

app = Flask(__name__)


@app.post("/")
def main():
    return f"Data is updated!"


"------------------------------------------------------------------------"

import os

import google.auth
import googleapiclient.discovery
from google.oauth2 import service_account
import google.cloud.bigquery as bq

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


def get_youtube_service(credentials_file=None):
    """
    Initializes and returns the YouTube Data API service object.

    Args:
        credentials_file: Optional path to a service account credentials JSON file.
                          If None, uses Application Default Credentials.
    """
    try:
        if credentials_file:
            # Load credentials from a specific JSON file.
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file
            )
        else:
            # Use Application Default Credentials (ADC).
            credentials, _ = google.auth.default()

        youtube = googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            credentials=credentials,
        )
        print("Successfully created YouTube API service.")
        return youtube
    except Exception as e:
        print(f"Error creating YouTube API service: {e}")
        return None


def get_video_ids_from_bigquery(project_id, query):
    """
    Retrieves video IDs from BigQuery based on the provided query.

    Args:
        project_id: The ID of the Google Cloud project.
        query: The SQL query to execute.

    Returns:
        A list of video IDs.
    """
    client = bq.Client(project=project_id)
    query_job = client.query(query)
    results = query_job.result()

    video_ids = []
    for row in results:
        if row.video_id is not None:  # Check for None
            video_ids.append(row.video_id)
    return video_ids


def get_playlist_ids_from_bigquery(project_id, query):
    """
    Retrieves playlist IDs from BigQuery based on the provided query.

    Args:
        project_id: The ID of the Google Cloud project.
        query: The SQL query to execute.

    Returns:
        A list of playlist IDs.
    """
    client = bq.Client(project=project_id)
    query_job = client.query(query)
    results = query_job.result()

    playlist_ids = []
    for row in results:
        if row.playlist_id is not None:  # Check for None
            playlist_ids.append(row.playlist_id)
    return playlist_ids


def get_vid_playlist_ids_from_bigquery(project_id, query):
    """
    Retrieves video IDs and playlist IDs from BigQuery based on the provided query.

    Args:
        project_id: The ID of the Google Cloud project.
        query: The SQL query to execute.

    Returns:
        A list of tuples (video_id, playlist_id).
    """
    client = bq.Client(project=project_id)
    query_job = client.query(query)
    results = query_job.result()

    ids = []
    for row in results:
        if row.video_id is not None:
            ids.append((row.video_id, row.playlist_id))

    return ids


def get_video_names(youtube, video_ids):
    """
    Retrieves video titles from a list of video IDs.

    Args:
        youtube: The YouTube API service object.
        video_ids: A list of YouTube video IDs.

    Returns:
        A dictionary where keys are video IDs and values are video titles.
        Returns an empty dictionary if there are errors or no videos found.
    """
    video_names = {}

    if not video_ids:
        print("No video IDs provided.")
        return video_names

    try:
        # The API allows up to 50 IDs per request.
        for i in range(0, len(video_ids), 50):
            chunk_ids = video_ids[i: i + 50]
            request = youtube.videos().list(
                part="snippet",
                id=",".join(chunk_ids),
            )
            response = request.execute()
            if "items" in response:
                for item in response["items"]:
                    video_id = item["id"]
                    video_title = item["snippet"]["title"]
                    video_names[video_id] = video_title
            else:
                print(f"No videos found for IDs: {chunk_ids}")
        return video_names

    except googleapiclient.errors.HttpError as e:
        print(f"An HTTP error occurred: {e}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}


def get_playlist_names(youtube, playlist_ids):
    """
    Retrieves playlist titles from a list of playlist IDs.

    Args:
        youtube: The YouTube API service object.
        playlist_ids: A list of YouTube playlist IDs.

    Returns:
        A dictionary where keys are playlist IDs and values are playlist titles.
        Returns an empty dictionary if there are errors.
    """
    playlist_names = {}
    if not playlist_ids:
        print("No playlist IDs provided.")
        return playlist_names

    try:
        # Similar to videos, handle up to 50 IDs per request
        for i in range(0, len(playlist_ids), 50):
            chunk_ids = playlist_ids[i: i + 50]
            request = youtube.playlists().list(
                part="snippet",
                id=",".join(chunk_ids),
            )
            response = request.execute()

            if "items" in response:
                for item in response["items"]:
                    playlist_id = item["id"]
                    playlist_title = item["snippet"]["title"]
                    playlist_names[playlist_id] = playlist_title
            else:
                print(f"No playlists found for IDs: {chunk_ids}")

        return playlist_names
    except googleapiclient.errors.HttpError as e:
        print(f"An HTTP error occurred: {e}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}


def upload_data_to_bigquery(project_id, dataset_id, table_id, data):
    """
    Uploads data to a BigQuery table, overwriting the existing table.

    Args:
        project_id: The ID of the Google Cloud project.
        dataset_id: The ID of the BigQuery dataset.
        table_id: The ID of the BigQuery table.
        data: A list of dictionaries representing the data to upload.
    """
    client = bq.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the schema
    schema = [
        bq.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bq.SchemaField("video_name", "STRING", mode="NULLABLE"),
        bq.SchemaField("playlist_id", "STRING", mode="REQUIRED"),
        bq.SchemaField("playlist_name", "STRING", mode="NULLABLE"),
    ]

    table = bq.Table(table_ref, schema=schema)

    # Delete the table if it exists
    try:
        client.delete_table(table, not_found_ok=True)
        print(f"Table {table_id} deleted.")
    except Exception as e:
        print(f"Error deleting table {table_id}: {e}")
        return

    # Create the table
    try:
        table = client.create_table(table)
        print(f"Table {table_id} created.")
    except Exception as e:
        print(f"Error creating table {table_id}: {e}")
        return

    errors = client.insert_rows_json(table, data)

    if errors == []:
        print(f"Data uploaded to table {table_id} successfully.")
    else:
        print(f"Error uploading data to table {table_id}: {errors}")


def main():
    """
    Main function to connect to YouTube API, retrieve video and playlist names,
    and upload the results to BigQuery.
    """
    # Option 1: Use Application Default Credentials (ADC)
    # youtube = get_youtube_service()

    # Option 2: Use a specific service account key file
    credentials_file = "/home/pieterdevries/key.json"  # Replace with the actual path if needed
    youtube = get_youtube_service(credentials_file)

    if not youtube:
        print("Failed to initialize YouTube API. Exiting.")
        return

    project_id = "looker-dcl-data"
    dataset_id = "pieteryoutube"
    table_id = "video_playlist_names"

    # 1. Get video IDs from both tables
    video_ids_from_playlists = get_video_ids_from_bigquery(
        project_id,
        """
        SELECT video_id
        FROM `looker-dcl-data.pieteryoutube.p_playlist_playback_location_a1_daily_first`
        GROUP BY 1
        """,
    )
    video_ids_from_channels = get_video_ids_from_bigquery(
        project_id,
        """
        SELECT video_id
        FROM `looker-dcl-data.pieteryoutube.p_channel_basic_a2_daily_first`
        GROUP BY 1
        """,
    )

    # Combine video_ids and remove duplicates
    all_video_ids = list(set(video_ids_from_playlists + video_ids_from_channels))

    # 2. Get playlist IDs
    playlist_ids = get_playlist_ids_from_bigquery(
        project_id,
        """
        SELECT playlist_id
        FROM `looker-dcl-data.pieteryoutube.p_playlist_playback_location_a1_daily_first`
        GROUP BY 1
        """,
    )

    # 3. Get video-playlist ID pairs (only non-NULL video IDs)
    vid_playlist_ids = get_vid_playlist_ids_from_bigquery(
        project_id,
        """
        SELECT video_id, playlist_id
        FROM `looker-dcl-data.pieteryoutube.p_playlist_playback_location_a1_daily_first`
        WHERE video_id IS NOT NULL
        GROUP BY 1, 2
        """,
    )

    # 4. Get video and playlist names from YouTube API
    video_names = get_video_names(youtube, all_video_ids)
    playlist_names = get_playlist_names(youtube, playlist_ids)

    # 5. Combine the data, ignoring "Popular uploads" playlists
    combined_data = []
    for video_id, playlist_id in vid_playlist_ids:
        
        # Check if the playlist name is "Popular uploads"
        playlist_name = playlist_names.get(playlist_id, "Playlist name not found")
        if playlist_name.lower() == "popular uploads":
            continue  # Skip this playlist

        video_name = video_names.get(video_id, "Video name not found")
        
        combined_data.append(
            {
                "video_id": video_id,
                "video_name": video_name,
                "playlist_id": playlist_id,
                "playlist_name": playlist_name,
            }
        )

    # 6. Upload to BigQuery
    upload_data_to_bigquery(project_id, dataset_id, table_id, combined_data)

if __name__ == "__main__":
    # Development only: run "python main.py" and open http://localhost:8080
    # When deploying to Cloud Run, a production-grade WSGI HTTP server,
    # such as Gunicorn, will serve the app.
    app.run(host="localhost", port=8080, debug=True)
