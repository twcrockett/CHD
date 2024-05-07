import psycopg2
import psycopg2.pool
import pandas as pd
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv
import os
import pyarrow as arrow
import pyarrow.parquet as pq

# Load environment variables
dotenv_path = "C:/Users/taylo/Documents/Taylor/2024/CHD/scripts/.env"
load_dotenv(dotenv_path)

# Set up connection pool
pool = psycopg2.pool.SimpleConnectionPool(
    1, 20,
    os.environ["DATABASE_URL"]
)

# Get connection from the pool
conn = pool.getconn()
cursor = conn.cursor()

# SQL to join tables
sql_query = """
SELECT h.*, t.transcript FROM hearing h
JOIN transcript t ON h."packageId" = t."packageId"
"""

# Read data into DataFrame
transcripts_df = pd.read_sql_query(sql_query, conn)

# Define your text extraction function
def extract_text(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    trim_pattern = re.compile(r"The (\w+) met(.*?)\[Whereupon", re.DOTALL)
    trimmed_transcript = trim_pattern.search(soup.get_text(separator=' ', strip=True))
    return trimmed_transcript.group(0) if trimmed_transcript else "No match found"

# Apply the function
transcripts_df['transcript'] = transcripts_df['transcript'].apply(extract_text)

# Define regex for extracting dialogues
titles = [ "Mr", "Mrs", "Ms", "Dr", "Senator", "Chairwoman", "Chairman", "Chairperson",
           "Chair", "Representative", "Judge", "Professor", "President", "Secretary",
           "Speaker", "Ambassador", "Governor", "Director", "Commissioner", "Admiral", "General",
           "Colonel", "Major", "Captain", "Lieutenant", "Justice"]
titles_pattern = "|".join(re.escape(title) for title in titles)
pattern = re.compile(rf"\n\s{{4}}((?:{titles_pattern})\.?\s[A-Z]\w+)\.\s(.*?)(?=(?:\n\s{{4}}(?!{titles_pattern})|$))", re.DOTALL)

# Extract dialogues
all_dialogues = []
for i, row in transcripts_df.iterrows():
    dialogues = pd.DataFrame(pattern.findall(transcripts_df.loc[i, 'transcript']), columns=['speaker', 'dialogue'])
    dialogues['packageId'] = row['packageId']
    dialogues['dialogueId'] = dialogues.index
    all_dialogues.append(dialogues)

# Concatenate all dialogues into one DataFrame
df = pd.concat(all_dialogues, ignore_index=True)

pq.write_table(arrow.Table.from_pandas(df), "data/dialogues.parquet")
