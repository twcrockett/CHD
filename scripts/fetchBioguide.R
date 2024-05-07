library(tidyverse)
library(DBI)
library(RPostgres)
library(dotenv)
library(jsonlite)
library(arrow)

dotenv::load_dot_env("C:/Users/taylo/Documents/Taylor/2024/CHD/scripts/.env")
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = Sys.getenv("DATABASE_NAME"),
                 host = Sys.getenv("DATABASE_HOST"),
                 port = Sys.getenv("DATABASE_PORT"),
                 user = Sys.getenv("DATABASE_USER"),
                 password = Sys.getenv("DATABASE_PASS"))

bg <- read_csv("data/bioguide.csv") %>%
  mutate(
    img_url = paste0("https://bioguide.congress.gov/bioguide/photo/", 
                     str_trunc(id, 1, ellipsis = ""), 
                     "/", id, ".jpg")
  ) %>%
  rename(bioguideId = id)

cgs <- bg %>%
  select(bioguideId, congresses) %>%
  mutate(congresses = map(congresses, ~fromJSON(.x, flatten = TRUE))
  ) %>%
  unnest(congresses) %>%
  mutate(
    partyChange = map_lgl(parties, ~length(.x) > 1),
    party = map(parties, ~.x[1])
  ) %>%
  select(-parties, -leadershipPositions)

dlg0 <- read_parquet("data/dialogues.parquet")
attend <- dbReadTable(con, "attendance")
attend_bg <- attend %>%
  left_join(bg %>% select(bioguideId, unaccentedFamilyName))
dlg <- dlg0 %>%
  mutate(Name = str_extract(speaker, "(?<=\\s)(\\b\\w+\\b)$")) %>%
  left_join(attend_bg, by = "packageId") %>%
  filter(!is.na(bioguideId) & unaccentedFamilyName == Name) %>%
  select(packageId, dialogueId, bioguideId)
dlgs <- dlg0 %>%
  left_join(dlg)

write_csv(dlgs, "data/dialogues.csv")

dbExecute(con, '
CREATE TABLE IF NOT EXISTS member (
    "bioguideId" TEXT PRIMARY KEY,
    "givenName" TEXT,
    "familyName" TEXT,
    "middleName" TEXT,
    "unaccentedGivenName" TEXT,
    "unaccentedFamilyName" TEXT,
    "unaccentedMiddleName" TEXT,
    "nickName" TEXT,
    "honorificPrefix" TEXT,
    "honorificSuffix" TEXT,
    "honorificTitle" TEXT,
    "birthYear" INTEGER,
    "deathYear" INTEGER,
    "congresses" TEXT,
    "img_url" TEXT
)')
dbWriteTable(con, "member", bg, append = TRUE)

dbExecute(con, '
CREATE TABLE IF NOT EXISTS congress (
    "congressNumber" INTEGER,
    "bioguideId" TEXT,
    "stateName" TEXT,
    "position" TEXT,
    "party" TEXT,
    "electionCirca" BOOLEAN,
    "electionDate" TEXT,
    "partyChange" BOOLEAN,
    PRIMARY KEY ("congressNumber", "bioguideId", "position"),
    FOREIGN KEY ("bioguideId") REFERENCES member ("bioguideId")
)')
dbWriteTable(con, "congress", cgs, append = TRUE)



dbExecute(con, '
CREATE TABLE IF NOT EXISTS congress (
    "congressNumber" INTEGER,
    "bioguideId" TEXT,
    "stateName" TEXT,
    "position" TEXT,
    "party" TEXT,
    "electionCirca" BOOLEAN,
    "electionDate" TEXT,
    "partyChange" BOOLEAN,
    PRIMARY KEY ("congressNumber", "bioguideId", "position"),
    FOREIGN KEY ("bioguideId") REFERENCES member ("bioguideId")
)')
dbWriteTable(con, "congress", cgs, append = TRUE)

dbExecute(con, '
CREATE TABLE IF NOT EXISTS dialogue (
    "bioguideId" TEXT,
    "speaker" TEXT,
    "dialogue" TEXT,
    "dialogueId" INT,
    "packageId" TEXT,
    PRIMARY KEY ("packageId", "dialogueId"),
    FOREIGN KEY ("packageId") REFERENCES hearing ("packageId"),
    FOREIGN KEY ("bioguideId") REFERENCES member ("bioguideId")
)')
batch_size <- 100

# Calculate number of batches
num_batches <- ceiling(nrow(p_dlgs) / batch_size)

# Loop through batches and write to the database
for (i in 1:num_batches) {
  start_row <- ((i - 1) * batch_size) + 1
  end_row <- min(i * batch_size, nrow(p_dlgs))
  batch_df <- p_dlgs[start_row:end_row, ]
  dbWriteTable(con, "dialogue", batch_df, append = TRUE)
}

dbReadTable(con, "dialogue")

dbExecute(con, "
IMPORT INTO dialogue
CSV DATA ('http://localhost:3000/dialogues.csv')
WITH skip = '1';
")

dbDisconnect(con)



