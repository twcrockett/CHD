import psycopg2
from dotenv import load_dotenv

# Load environment variables
dotenv_path = "C:/Users/taylo/Documents/Taylor/2024/CHD/scripts/.env"
load_dotenv(dotenv_path)

# Connect to your CockroachDB instance
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

# Fetch table schema information
cur.execute("""
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM
    information_schema.columns
WHERE
    table_schema = 'public';
""")

tables = {}
for row in cur.fetchall():
    if row[0] not in tables:
        tables[row[0]] = []
    tables[row[0]].append({
        'column_name': row[1],
        'data_type': row[2],
        'is_nullable': row[3]
    })

# Format as DBML
dbml_output = []
for table, columns in tables.items():
    dbml_output.append(f"Table {table} {{")
    for col in columns:
        nullable = 'optional' if col['is_nullable'] == 'YES' else ''
        dbml_output.append(f"    {col['column_name']} {col['data_type']} {nullable}")
    dbml_output.append("}")

# Save or print your DBML
print("\n".join(dbml_output))

# Close the connection
cur.close()
conn.close()
