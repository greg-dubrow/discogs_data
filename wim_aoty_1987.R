

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
ukvinyl87_search_df <- search_releases("UK", 1987, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(ukvinyl87_search_df)

# test_ids <- head(ukvinyl87_search_df$id, 100)

# Step 2: Pull full details

# run as background job
ukvinyl87_release_jsons <- fetch_release_details(ukvinyl87_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
uk_vinyl_1987 <- clean_all_releases(ukvinyl87_release_jsons)

glimpse(uk_vinyl_1987)

saveRDS(uk_vinyl_1987, "data/uk_vinyl_1987.rds")

uk_vinyl_1987 %>%
  count(mixed_at)

## pull cds
# Step 1: Search filtered releases
ukcd87_search_df <- search_releases("UK", 1987, "CD",
  discogs_key, discogs_secret, ua_string)

glimpse(ukcd87_search_df)

# Step 2: Pull full details
# run as background job
ukcd87_release_jsons <- fetch_release_details(ukcd87_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
uk_cd_1987 <- clean_all_releases(ukcd87_release_jsons)

glimpse(uk_cd_1987)

saveRDS(uk_cd_1987, "data/uk_cd_1987.rds")


## pull cassette
# Step 1: Search filtered releases
ukcas87_search_df <- search_releases("UK", 1987, "Cassette",
  discogs_key, discogs_secret, ua_string)

glimpse(ukcas87_search_df)

# Step 2: Pull full details
# run as background job
ukcas87_release_jsons <- fetch_release_details(ukcas87_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
uk_cas_1987 <- clean_all_releases(ukcas87_release_jsons)

glimpse(uk_cas_1987)

saveRDS(uk_cas_1987, "data/uk_cas_1987.rds")



## build UK 1987 file with vinyl, cd & cassette

uk_1987_all <- uk_cd_1987 %>%
  rbind(uk_cd_1987) %>%
  rbind(uk_cas_1987)

glimpse(uk_1987_all)

uk_1987_all %>%
  count(format1, format2)


### get us releases
## pull vinyl
# Step 1: Search filtered releases (e.g., only Vinyl LPs)
usvinyl87_search_df <- search_releases("US", 1987, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(usvinyl87_search_df )

# Step 2: Pull full details

# run as background job
usvinyl87_release_jsons <- fetch_release_details(usvinyl87_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
us_vinyl_1987 <- clean_all_releases(usvinyl87_release_jsons)

glimpse(us_vinyl_1987)

saveRDS(us_vinyl_1987, "data/us_vinyl_1987.rds")


## pull CD
# Step 1: Search filtered releases
uscd87_search_df <- search_releases("US", 1987, "CD",
  discogs_key, discogs_secret, ua_string)

glimpse(uscd87_search_df )

# Step 2: Pull full details

# run as background job
uscd87_release_jsons <- fetch_release_details(uscd87_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
us_cd_1987 <- clean_all_releases(uscd87_release_jsons)

glimpse(us_cd_1987)

saveRDS(us_cd_1987, "data/us_cd_1987.rds")

## pull Cassette
# Step 1: Search filtered releases
uscas87_search_df <- search_releases("US", 1987, "Cassette",
  discogs_key, discogs_secret, ua_string)

glimpse(uscas87_search_df )

# Step 2: Pull full details

# run as background job
uscas87_release_jsons <- fetch_release_details(uscas87_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
us_cas_1987 <- clean_all_releases(uscas87_release_jsons)

glimpse(us_cas_1987)

saveRDS(us_cas_1987, "data/us_cas_1987.rds")


## join datasets

uk_vinyl_1987 %>%
  count(artist, title) %>%
  arrange(desc(n), artist, title)

uk_aoty_1987 <- uk_vinyl_1987 %>%
  rbind(uk_cd_1987) %>%
  rbind(uk_cas_1987) %>%
  mutate(non_music = ifelse(
    genre1 == "Non-Music" | genre2 == "Non-Music" | genre3 == "Non-Music",
    1, 0)) %>%
  mutate(non_music = ifelse(is.na(non_music), 0, non_music)) %>%
  filter(non_music == 0) %>%
  mutate(artist = str_remove(artist, "\\s*\\([0-9]+\\)$")) %>%
  mutate(artist = str_replace_all(artist, '"([^"]+)"', '\\1')) %>%
  arrange(artist, title, master_id, status) %>%
  distinct(artist, title, master_id, .keep_all = T) %>%
  distinct(artist, title, .keep_all = T) %>%
  filter(!title == "")

glimpse(uk_aoty_1987)

uk_aoty_1987 %>%
  count(non_music, genre1, genre2, genre3) %>%
  view()

uk_aoty_1987 %>%
  count(artist, title) %>%
  arrange(desc(n), artist, title)

us_aoty_1987 <- us_vinyl_1987 %>%
  rbind(us_cd_1987) %>%
  rbind(us_cas_1987) %>%
  mutate(non_music = ifelse(
    genre1 == "Non-Music" | genre2 == "Non-Music" | genre3 == "Non-Music",
    1, 0)) %>%
  mutate(non_music = ifelse(is.na(non_music), 0, non_music)) %>%
  filter(non_music == 0) %>%
  mutate(artist = str_remove(artist, "\\s*\\([0-9]+\\)$")) %>%
  mutate(artist = str_replace_all(artist, '"([^"]+)"', '\\1')) %>%
  arrange(artist, title, master_id, status) %>%
  distinct(artist, title, master_id, .keep_all = T) %>%
  distinct(artist, title, .keep_all = T)

us_aoty_1987 %>%
  count(artist, title)%>%
  arrange(desc(n), artist, title)

aoty_1987 <- uk_aoty_1987 %>%
  rbind(us_aoty_1987) %>%
  arrange(artist, title, master_id, status, country) %>%
  distinct(artist, title, master_id, .keep_all = T) %>%
  distinct(artist, title, .keep_all = T)

aoty_1987 %>%
  count(artist, title)%>%
  arrange(desc(n), artist, title)


#### diagnostics
attr(uk_vinyl_1987, "failed_indices")
attr(uk_vinyl_1987, "failed_errors")

problem_index <- attr(uk_vinyl_1987, "failed_indices")[1]
ukvinyl87_release_jsons[[problem_index]]$id

# search in a json file
#returns the index(es) of matching releases in your list.
which(purrr::map_lgl(ukvinyl87_release_jsons, ~ .x$id == 598890))

ukvinyl87_release_jsons[[which(
  purrr::map_lgl(ukvinyl87_release_jsons, ~ .x$id == 598890))[1]]]

# Example: search by artist name (nested in $artists)

which(purrr::map_lgl(
  release_jsons,
  ~ "The Smiths" %in% purrr::map_chr(.x$artists, "name", .default = NA)
))

# Search for releases containing a string (not exact match)

smiths_hits <- which(purrr::map_lgl(
  release_jsons,
  ~ any(stringer::str_detect(
    tolower(paste(purrr::map_chr(.x$artists, "name", .default = ""), collapse = " ")),
    "smith"
  ))
))

release_jsons[[smiths_hits[1]]]$artists

# Extract a vector of values from all records
# If you want to quickly scan what values exist for a field:


## search cleaned releases for fails
attr(uk_vinyl_1987, "failed_indices")
