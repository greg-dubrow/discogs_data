

### pulls releases by year
library(httr)
library(jsonlite)
library(tidyverse)
library(tidylog)
library(gregeRs)

# load Discogs API credentials
discogs_key    <- Sys.getenv("DISCOGS_KEY")
discogs_secret <- Sys.getenv("DISCOGS_SECRET")
ua_string      <- "MyDiscogsApp/1.0 +https://example.com"

source("00_functions_new.R")

## need to pulls separate

## pull vinyl
# Step 1: Search filtered releases (e.g., only Vinyl LPs)
ukvinyl90_search_df <- search_releases("UK", 1990, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(ukvinyl90_search_df)

# Step 2: Pull full details

# run as background job
ukvinyl90_release_jsons <- fetch_release_details(ukvinyl90_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
uk_vinyl_1990 <- clean_all_releases(ukvinyl90_release_jsons)

glimpse(uk_vinyl_1990)

saveRDS(uk_vinyl_1990, "data/uk_vinyl_1990.rds")


## pull cds
# Step 1: Search filtered releases
ukcd90_search_df <- search_releases("UK", 1990, "CD",
  discogs_key, discogs_secret, ua_string)

glimpse(ukcd90_search_df)

# Step 2: Pull full details
# run as background job
ukcd90_release_jsons <- fetch_release_details(ukcd90_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
uk_cd_1990 <- clean_all_releases(ukcd90_release_jsons)

glimpse(uk_cd_1990)

saveRDS(uk_cd_1990, "data/uk_cd_1990.rds")


## pull cassette
# Step 1: Search filtered releases
ukcas90_search_df <- search_releases("UK", 1990, "Cassette",
  discogs_key, discogs_secret, ua_string)

glimpse(ukcas90_search_df)

# Step 2: Pull full details
# run as background job
ukcas90_release_jsons <- fetch_release_details(ukcas90_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
uk_cas_1990 <- clean_all_releases(ukcas90_release_jsons)

glimpse(uk_cas_1990)

saveRDS(uk_cas_1990, "data/uk_cas_1990.rds")




### get us releases
## pull vinyl
# Step 1: Search filtered releases (e.g., only Vinyl LPs)
usvinyl90_search_df <- search_releases("US", 1990, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(usvinyl90_search_df )

# Step 2: Pull full details

# run as background job
usvinyl90_release_jsons <- fetch_release_details(usvinyl90_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
us_vinyl_1990 <- clean_all_releases(usvinyl90_release_jsons)

glimpse(us_vinyl_1990)

saveRDS(us_vinyl_1990, "data/us_vinyl_1990.rds")


## pull CD
# Step 1: Search filtered releases
uscd90_search_df <- search_releases("US", 1990, "CD",
  discogs_key, discogs_secret, ua_string)

glimpse(uscd90_search_df )

# Step 2: Pull full details

# run as background job
uscd90_release_jsons <- fetch_release_details(uscd90_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
us_cd_1990 <- clean_all_releases(uscd90_release_jsons)

glimpse(us_cd_1990)

saveRDS(us_cd_1990, "data/us_cd_1990.rds")

## pull Cassette
# Step 1: Search filtered releases
uscas90_search_df <- search_releases("US", 1990, "Cassette",
  discogs_key, discogs_secret, ua_string)

glimpse(uscas90_search_df )

# Step 2: Pull full details

# run as background job
uscas90_release_jsons <- fetch_release_details(uscas90_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
us_cas_1990 <- clean_all_releases(uscas90_release_jsons)

glimpse(us_cas_1990)

saveRDS(us_cas_1990, "data/us_cas_1990.rds")


# combine files
uk_aoty_1990 <- uk_vinyl_1990 %>%
  rbind(uk_cd_1990) %>%
  rbind(uk_cas_1990) %>%
  mutate(non_music = ifelse(
    genre1 == "Non-Music" | genre2 == "Non-Music" | genre3 == "Non-Music",
    1, 0)) %>%
  mutate(non_music = ifelse(is.na(non_music), 0, non_music)) %>%
  filter(non_music == 0) %>%
  mutate(artist = str_remove(artist, "\\s*\\([0-9]+\\)$")) %>%
  mutate(artist = str_replace_all(artist, '"([^"]+)"', '\\1')) %>%
  mutate(artist = str_trim(artist)) %>%
  mutate(title = str_trim(title)) %>%
  arrange(artist, title, master_id, status) %>%
  distinct(artist, title, master_id, .keep_all = T) %>%
  distinct(artist, title, .keep_all = T)

glimpse(uk_aoty_1990)

uk_aoty_1990 %>%
  count(artist, title) %>%
  arrange(desc(n), artist, title)

us_aoty_1990 <- us_vinyl_1990 %>%
  rbind(us_cd_1990) %>%
  rbind(us_cas_1990) %>%
  mutate(non_music = ifelse(
    genre1 == "Non-Music" | genre2 == "Non-Music" | genre3 == "Non-Music",
    1, 0)) %>%
  mutate(non_music = ifelse(is.na(non_music), 0, non_music)) %>%
  filter(non_music == 0) %>%
  mutate(artist = str_remove(artist, "\\s*\\([0-9]+\\)$")) %>%
  mutate(artist = str_replace_all(artist, '"([^"]+)"', '\\1')) %>%
  mutate(artist = str_trim(artist)) %>%
  mutate(title = str_trim(title)) %>%
  arrange(artist, title, master_id, status) %>%
  distinct(artist, title, master_id, .keep_all = T) %>%
  distinct(artist, title, .keep_all = T)

glimpse(us_aoty_1990)

us_aoty_1990 %>%
  count(artist, title) %>%
  arrange(desc(n), artist, title)

aoty_1990 <- uk_aoty_1990 %>%
  rbind(us_aoty_1990) %>%
  arrange(artist, title, master_id, status, country) %>%
  distinct(artist, title, master_id, .keep_all = T) %>%
  distinct(artist, title, .keep_all = T)

aoty_1990 %>%
  count(artist, title) %>%
  arrange(desc(n), artist, title) %>%
  view()

aoty_1990 %>%
  count(artist) %>%
  arrange(desc(n), artist) %>%
  view()


aoty_1990 %>%
  count(genre1)
aoty_1990 %>%
  count(genre1, genre2) %>%
  filter(!is.na(genre2)) %>%
  arrange(desc(n))

aoty_1990 %>%
  count(genre1, genre2, genre3) %>%
  filter(!is.na(genre2)) %>%
  filter(!is.na(genre3)) %>%
  arrange(desc(n))
