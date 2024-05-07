import os
import requests
import logging
import concurrent.futures
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Load environment variables
dotenv_path = "C:/Users/taylo/Documents/Taylor/2024/CHD/scripts/.env"
load_dotenv(dotenv_path)

def get_db_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def create_tables():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hearing (
                "packageId" TEXT PRIMARY KEY,
                "lastModified" TIMESTAMP,
                "packageLink" TEXT,
                "docClass" TEXT,
                "title" TEXT,
                "congress" INTEGER,
                "dateIssued" DATE,
                "htmlLink" TEXT
            );
            CREATE TABLE IF NOT EXISTS transcript (
                "packageId" TEXT PRIMARY KEY,
                "transcript" TEXT,
                FOREIGN KEY ("packageId") REFERENCES hearing("packageId")
            );
        """)
        conn.commit()
    conn.close()


def insert_hearing_data(data):
    conn = get_db_connection()
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO hearing ("packageId", "lastModified", "packageLink", "docClass", "title", "congress", "dateIssued", "htmlLink")
            VALUES (%(packageId)s, %(lastModified)s, %(packageLink)s, %(docClass)s, %(title)s, %(congress)s, %(dateIssued)s, %(htmlLink)s);
        """, data)
        conn.commit()
    conn.close()


def insert_transcript_data(data):
    conn = get_db_connection()
    with conn.cursor() as cur:
        batch_size = 10
        num_batches = (len(data) + batch_size - 1) // batch_size
        for i in range(0, len(data), batch_size):
            batch_data = data[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO transcript ("packageId", "transcript")
                VALUES (%(packageId)s, %(transcript)s);
            """, batch_data)
            conn.commit()
            logging.info(f"Batch {i // batch_size + 1}/{num_batches} processed, {len(batch_data)} records inserted.")
    conn.close()
    logging.info("All transcript data has been successfully inserted into the database.")


def fetch_data(base_url, congress):
    api_key = os.environ['GOVINFO_API']
    params = {'pageSize': 100, 'collection': 'CHRG', 'congress': congress, 'offsetMark': '*', 'api_key': api_key}
    url = f"{base_url}?{'&'.join(f'{key}={value}' for key, value in params.items())}"
    while url:
        response = requests.get(url, headers={'Content-Type': 'application/json'}).json()
        data = [
            {
                'packageId': package.get('packageId'),
                'lastModified': package.get('lastModified'),
                'packageLink': package.get('packageLink'),
                'docClass': package.get('docClass'),
                'title': package.get('title'),
                'congress': package.get('congress'),
                'dateIssued': package.get('dateIssued'),
                'htmlLink': f"https://www.govinfo.gov/content/pkg/{package.get('packageId')}/html/{package.get('packageId')}.htm"
            }
            for package in response.get('packages', [])
        ]
        yield data
        url = response.get('nextPage')

def fetch_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup.get_text()
    except requests.RequestException as e:
        logging.exception(f"Error fetching {url}: {e}")
        return ""

def process_data(data):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(fetch_html, d['htmlLink']): d for d in data}
        for future in concurrent.futures.as_completed(futures):
            data_dict = futures[future]
            html_content = future.result()
            data_dict['transcript'] = html_content
            yield data_dict

if __name__ == "__main__":
    create_tables() 
    base_url = 'https://api.govinfo.gov/published/2008-12-31'
    for congress in range(111, 119):
        logging.info(f"Fetching data for Congress {congress}")
        for batch in fetch_data(base_url, congress):
            logging.info("Processing HTML content")
            processed_data = list(process_data(batch))
            logging.info("Inserting hearing data into database")
            insert_hearing_data(batch)
            logging.info("Inserting transcript data into database")
            insert_transcript_data(processed_data)
    logging.info("All data processed and saved to the database.")
