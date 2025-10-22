

### pulls releases by year
library(httr)
library(jsonlite)
library(tidyverse)
library(tidylog)
library(gregeRs)

## need to pulls separate

## pull vinyl
# Step 1: Search filtered releases (e.g., only Vinyl LPs)
ukvinyl90_search_df <- search_releases("UK", 1990, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(search_df)

# test_ids <- head(ukvinyl90_search_df$id, 100)

# Step 2: Pull full details

# run as background job
ukvinyl90_release_jsons <- fetch_release_details(ukvinyl90_search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean & save
uk_vinyl_1990 <- clean_all_releases(release_jsons_all)

glimpse(uk_vinyl_1990)

# # inspect which records failed and why
# failed_i <- attr(uk_vinyl_1990, "failed_indices")
# failed_errors <- attr(uk_vinyl_1990, "failed_errors")
# failed_errors[[2]]

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



## build UK 1990 file with vinyl, cd & cassette

uk_1990_all <- uk_cd_1990 %>%
  rbind(uk_cd_1990) %>%
  rbind(uk_cas_1990)

glimpse(uk_1990_all)

uk_1990_all %>%
  count(format1, format2)


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
us_vinyl_1990 <- clean_all_releases(release_jsons_all)

glimpse(us_vinyl_1990)

saveRDS(us_vinyl_1990, "data/us_vinyl_1990.rds")
