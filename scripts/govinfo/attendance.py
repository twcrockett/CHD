import os
import psycopg2
import psycopg2.pool
import requests
import xml.etree.ElementTree as ET
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the connection pool
conn_pool = psycopg2.pool.SimpleConnectionPool(1, 15, os.environ["DATABASE_URL"])

def get_db_connection():
    return conn_pool.getconn()

def release_db_connection(conn):
    conn_pool.putconn(conn)

def create_table():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS attendance (
            "packageId" VARCHAR(255) NOT NULL,
            "bioguideId" VARCHAR(255) NOT NULL,
            PRIMARY KEY ("packageId", "bioguideId"),
            FOREIGN KEY ("packageId") REFERENCES hearing("packageId"),
            FOREIGN KEY ("bioguideId") REFERENCES member("bioguideId")
        );
        """)
        conn.commit()
        logging.info("Table created or verified successfully.")
    finally:
        cursor.close()
        release_db_connection(conn)

def fetch_package_ids():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT "packageId" FROM hearing;')
        package_ids = [row[0] for row in cursor.fetchall()]
        logging.info(f"Fetched {len(package_ids)} package IDs from the database.")
    finally:
        cursor.close()
        release_db_connection(conn)
    return package_ids

def fetch_and_parse_xml(package_id):
    url = f"https://www.govinfo.gov/metadata/pkg/{package_id}/mods.xml"
    try:
        response = requests.get(url)
        response.raise_for_status()
        xml_data = response.text
        root = ET.fromstring(xml_data)
        ns = {'mods': 'http://www.loc.gov/mods/v3'}
        bioguide_ids = root.findall('.//mods:congMember[@bioGuideId]', namespaces=ns)
        bioguide_ids = [bg.attrib['bioGuideId'] for bg in bioguide_ids if 'bioGuideId' in bg.attrib]
        return (package_id, bioguide_ids)
    except requests.RequestException as e:
        logging.error(f"Failed to fetch or parse XML for package ID {package_id}: {e}")
        return (package_id, [])

def insert_bioguide_ids(data):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        insert_sql = """
        INSERT INTO attendance ("packageId", "bioguideId")
        VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """
        cursor.executemany(insert_sql, data)
        conn.commit()
        logging.info(f"Inserted {len(data)} BioGuide ID records.")
    finally:
        cursor.close()
        release_db_connection(conn)

def main():
    create_table()
    package_ids = fetch_package_ids()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_and_parse_xml, pid): pid for pid in package_ids}
        for future in as_completed(futures):
            package_id, bioguide_ids = future.result()
            if bioguide_ids:
                insert_bioguide_ids([(package_id, bid) for bid in bioguide_ids])

if __name__ == '__main__':
    main()
