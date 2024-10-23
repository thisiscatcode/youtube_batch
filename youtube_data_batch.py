import os
import mysql.connector
import requests
import time
from datetime import datetime, timedelta


api_key = ''

query = 'かわいい猫'  # Replace with your search query
max_results = 50  # Maximum results per request
total_results_to_fetch = 99999999  # Total results you want to fetch

one_year_ago = datetime.utcnow() - timedelta(days=183)
published_after = one_year_ago.isoformat("T") + "Z"

#relevance_language = 'en'
relevance_language = 'ja'

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

class YoutubeDataBatch:

    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.mysql_conn = self.db_connector.connect()
        self.mysql_cursor = self.mysql_conn.cursor()


    def fetch_video_details(self, query, api_key, max_results):
        video_details = []
        channel_ids = set()  # Use a set to avoid duplicates
        next_page_token = self.get_next_page_token(query)
           
        total_results = 0
      
        search_url = f'https://youtube.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults={max_results}&q={query}&relevanceLanguage={relevance_language}&publishedAfter={published_after}&key={api_key}'
        if next_page_token:
            search_url += f'&pageToken={next_page_token}'
        
        search_headers = {'Accept': 'application/json'}
        search_response = requests.get(search_url, headers=search_headers)
       
        if search_response.status_code == 200:
            search_data = search_response.json()
            total_results = int(search_data['pageInfo']['totalResults'])
            print(f"Total search results: {total_results}")
            next_page_token = search_data.get('nextPageToken')
            
            for item in search_data.get('items', []):
                if item['id']['kind'] == 'youtube#video':
                    video_id = item['id']['videoId']
                    video_title = item['snippet']['title']
                    channel_id = item['snippet']['channelId']
                    video_url = f'https://www.youtube.com/watch?v={video_id}'
                    video_details.append({
                        'video_url': video_url,
                        'video_title': video_title,
                        'channel_id': channel_id
                    })
                    channel_ids.add(channel_id)
                    # Save results to DB
                    self.insert_search_result(video_url, video_title, channel_id, query, total_results, next_page_token)
            
            
        else:
            print(f'Error fetching search data: {search_response.status_code}')
            print(search_response.text)
        
        return video_details, list(channel_ids)
        


    def fetch_channel_info(self,channel_ids, api_key):
        channel_info = {}
        # Process channel IDs in batches of 50 (YouTube API limit)
        for i in range(0, len(channel_ids), 50):
            batch = channel_ids[i:i+50]
            channel_ids_str = ','.join(batch)
            channels_url = f'https://youtube.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={channel_ids_str}&key={api_key}'
            channels_headers = {'Accept': 'application/json'}
            channels_response = requests.get(channels_url, headers=channels_headers)
            
            if channels_response.status_code == 200:
                channels_data = channels_response.json()
                for item in channels_data.get('items', []):
                    channel_id = item['id']
                    channel_info[channel_id] = {
                        'subscriber_count': item['statistics'].get('subscriberCount', 'N/A'),
                        'custom_url': item['snippet'].get('customUrl', 'N/A'),
                        'display_name': item['snippet'].get('title', 'N/A')
                    }
            else:
                print(f'Error fetching channel data: {channels_response.status_code}')
                print(channels_response.text)
        
        return channel_info



    def process_search_batches(self):
        video_details, channel_ids = self.fetch_video_details(query, api_key, max_results)

    def process_channel_batches(self):
        
        # Search DB for display_name and subscriber_count null record
        try:
            self.mysql_cursor.execute(
                "SELECT channel_id FROM bot_youtube_data WHERE display_name IS NULL AND subscriber_count IS NULL LIMIT 1"
            )
            channel_ids_from_db = [row[0] for row in self.mysql_cursor.fetchall()]

        except Exception as e:
            print(f"Error fetching records from MySQL: {e}")
            return False  # Return False on error or no records found

        if not channel_ids_from_db:
            return False  # Return False if no channels need updating

        channel_info = self.fetch_channel_info(channel_ids_from_db, api_key)
        
        for channel_id in channel_ids_from_db:
            display_name = channel_info.get(channel_id, {}).get('display_name', 'N/A')
            subscriber_count = channel_info.get(channel_id, {}).get('subscriber_count', 'N/A')
            
            try:
                self.mysql_cursor.execute(
                    "UPDATE bot_youtube_data SET display_name = %s, subscriber_count = %s, update_time_1 = NOW() WHERE channel_id = %s",
                    (display_name, subscriber_count, channel_id)
                )
                self.mysql_conn.commit()
                print(f"Processed channel: {display_name}")
            except Exception as e:
                print(f"Error updating record in MySQL: {e}")
        
        return True  # Return True indicating channels were processed
        
        
    def get_next_page_token(self, query):
        try:
            self.mysql_cursor.execute(
                "SELECT nextPageToken FROM bot_youtube_data WHERE search_words = %s ORDER BY id DESC LIMIT 1", 
                (query,)
            )
            result = self.mysql_cursor.fetchone()
            if result:
                return result[0]
            else:
                return None
        except Exception as e:
            print(f"Error fetching nextPageToken from MySQL: {e}")
            return None


    def fetch_unprocessed_records(self, limit=1):
        try:
            self.mysql_cursor.execute("SELECT id, channel_id FROM bot_youtube_data WHERE status_flg = 1 LIMIT %s", (limit,))
            records = self.mysql_cursor.fetchall()
            return records

        except Exception as e:
            print(f"Error fetching unprocessed records from MySQL: {e}")

    def update_channel_info(self, record_id, display_name, subscriber_count, status):
        try:
            self.mysql_cursor.execute(
                "UPDATE bot_youtube_data SET display_name = %s, subscriber_count = %s, status_flg = %s, update_time_1 = NOW() WHERE id = %s",
                (display_name, subscriber_count, status, record_id)
            )
            self.mysql_conn.commit()
        except Exception as e:
            print(f"Error updating record in MySQL: {e}")


    def update_record_status(self, record_id, status):
        try:
            self.mysql_cursor.execute("UPDATE bot_youtube_data SET status_flg = %s , update_time_1 = NOW() WHERE id = %s", (status, record_id))
            
            self.mysql_conn.commit()

        except Exception as e:
            print(f"Error updating record status_flg in MySQL: {e}")

    def insert_search_result(self, video_url, video_title, channel_id,query,total_results,next_page_token):
        try:
            self.mysql_cursor.execute(
                "INSERT IGNORE INTO bot_youtube_data (youtube_url, video_title, channel_id, search_words,total_results,nextPageToken,status_flg, created_at) VALUES (%s, %s, %s,%s, %s, %s,%s, NOW())",
                (video_url, video_title, channel_id,query,total_results,next_page_token,1)
            )
            self.mysql_conn.commit()
        except Exception as e:
            print(f"Error inserting search result in MySQL: {e}")


    def get_record_id(self, video_url):
        try:
            self.mysql_cursor.execute("SELECT id FROM bot_youtube_data WHERE youtube_url = %s", (video_url,))
            result = self.mysql_cursor.fetchone()
            if result:
                return result[0]
            else:
                return None
        except Exception as e:
            print(f"Error fetching record ID from MySQL: {e}")
            return None


# Example usage
db_connector = DatabaseConnector()
batch = YoutubeDataBatch(db_connector)

search_completed = False

while True:
    if not search_completed:
        batch.process_search_batches()
        search_completed = True
    else:
        if not batch.process_channel_batches():
            search_completed = False  # Reset to run search batch again

    time.sleep(10)  # Delay between batches

