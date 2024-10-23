import os
import mysql.connector
from yt_dlp import YoutubeDL
from datetime import datetime

class DatabaseConnector:
    def __init__(self):
        self.host = 'xxx'
        self.user = 'xxx'
        self.password = 'xxx'
        self.database = 'xxx'
        self.charset = 'utf8mb4'
        
    def connect(self):
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset
        )

    def close_connection(self, conn, cursor):
        if cursor:
            cursor.close()
        if conn:
            conn.close()

class YouTubeDownloader:

    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.mysql_conn = self.db_connector.connect()
        self.mysql_cursor = self.mysql_conn.cursor()


    def download_videos(self, video_url,record_id):
        output_directory = '/data/dl_youtube/'

        # Options for downloading the best mp4 video
        video_options = {
            'outtmpl': os.path.join(output_directory, f'{record_id}.%(ext)s'),
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        }

        # Options for downloading the best webm audio
        audio_options = {
            'outtmpl': os.path.join(output_directory, f'{record_id}.%(ext)s'),
            'format': 'bestaudio[ext=m4a]/best[ext=webm]/best',
        }

        video_downloader = YoutubeDL(video_options)
        audio_downloader = YoutubeDL(audio_options)

        print(f"Downloading video from: {video_url}")

        try:
            # Download the video
            video_info = video_downloader.extract_info(video_url, download=True)

            # Download the audio
            audio_info = audio_downloader.extract_info(video_url, download=True)

            print(f"Download complete for: {video_url}\n")
            return video_info
        except Exception as e:
            print(f"Error downloading video with URL {video_url}: {str(e)}")
            return None


    def fetch_unprocessed_records(self, limit=1):
        try:
            self.mysql_cursor.execute("SELECT id, youtube_url FROM bot_youtube_urls WHERE status_flg = 0 LIMIT %s", (limit,))
            records = self.mysql_cursor.fetchall()
            return records

        except Exception as e:
            print(f"Error fetching unprocessed records from MySQL: {e}")

    def update_record_status(self, record_id, status):
        try:
            self.mysql_cursor.execute("UPDATE bot_youtube_urls SET status_flg = %s  WHERE id = %s", (status, record_id))
            
            self.mysql_conn.commit()

        except Exception as e:
            print(f"Error updating record status_flg in MySQL: {e}")

    def update_title_in_database(self, url, title):
        try:
            title_bytes = title.encode('utf-8')
            self.mysql_cursor.execute("UPDATE bot_youtube_urls SET video_title = %s WHERE youtube_url = %s", (title_bytes, url))
            self.mysql_conn.commit()

        except Exception as e:
            print(f"Error updating title in MySQL: {e}")

    def download_and_update_records(self):
        record = self.fetch_unprocessed_records(limit=1)

        if not record:
            print("No unprocessed records found.")
            return

        record_id, youtube_url = record[0]

        try:
            video_info = self.download_videos(youtube_url,record_id)

            if video_info:
                # Extract title from video_info
                title = video_info.get('title', 'Unknown Title')
                # Update title and status in the database
                self.update_title_in_database(youtube_url, title)
                self.update_record_status(record_id, 1)
                print(f"Download and update complete for video with id {record_id}")
            else:
                print(f"Error downloading video with id {record_id}: Video information is not available")
                self.update_record_status(record_id, 91)  # Mark as failed to download
        except Exception as e:
            print(f"Error downloading video with id {record_id}: {str(e)}")
            self.update_record_status(record_id, 91)  # Mark as failed to download


# Example usage
db_connector = DatabaseConnector()
youtube_downloader = YouTubeDownloader(db_connector)
youtube_downloader.download_and_update_records()
