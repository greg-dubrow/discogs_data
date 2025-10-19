## use case of discogs scrape workflow

library(httr)
library(jsonlite)
library(tidyverse)
library(tidylog)

# load Discogs API credentials
discogs_key    <- Sys.getenv("DISCOGS_KEY")
discogs_secret <- Sys.getenv("DISCOGS_SECRET")
ua_string      <- "MyDiscogsApp/1.0 +https://example.com"

# Fetch a year of data
den_88_raw <- fetch_all_releases("Denmark", 1988, discogs_key, discogs_secret, ua_string)

glimpse(den_88_raw)

# clean fetched data
den_88_clean<- clean_discogs_data(den_88_raw)

glimpse(den_88_clean)

den_88_clean %>%
  count(artist) %>%
  arrange(desc(n)) %>%
  view()

den_88_clean %>%
  count(format1, format2, format3) %>%
  view()
