## tracks rise/fall/rise of vinyl, cassette & cd over time

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

source("00_functions_all.R")





## pull vinyl, one set each for LP, EP, Singles

# Step 1: Search filtered releases
uk_vinyl_92_lp_df1 <- search_releases_lp("UK", 1992, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(uk_vinyl_92_lp_df)

uk_vinyl_92_lp_df <- uk_vinyl_92_lp_df1 %>%
  tidyr::separate(title, into =
      c("artist", "release_title"),
    sep = " - ", extra = "merge", fill = "right") %>%
  mutate(artist = str_trim(artist)) %>%
  mutate(release_title = str_trim(release_title)) %>%
  filter(!map_lgl(format, ~ "Test Pressing" %in% .x)) %>%
  filter(!map_lgl(format, ~ "Promo" %in% .x)) %>%
  expand_list_column("label", "label", max_items = 3) %>%
  expand_list_column("format", "format") %>%
  expand_list_column("genre", "genre")

glimpse(uk_vinyl_92_lp_df_clean)

uk_vinyl_92_lp_df_clean %>%
  count(format_3)

uk_vinyl_92_lp_df_clean %>%
  filter(format_3 == "EP") %>%
  view()

uk_vinyl_92_ep_df1 <- search_releases_ep("UK", 1992, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(uk_vinyl_92_ep_df)

uk_vinyl_92_ep_df <- uk_vinyl_92_ep_df1 %>%
  tidyr::separate(title, into =
      c("artist", "release_title"),
    sep = " - ", extra = "merge", fill = "right") %>%
  mutate(artist = str_trim(artist)) %>%
  mutate(release_title = str_trim(release_title)) %>%
  expand_list_column("label", "label", max_items = 3) %>%
  expand_list_column("format", "format")

glimpse(uk_vinyl_92_ep_df_clean)

uk_vinyl_92_ep_df_clean %>%
  count(format_1)

uk_vinyl_92_lp_df %>%
  filter(format_3 == "EP") %>%
  view()

uk_vinyl_92_ep_df %>%
  count(title) %>%
  arrange(desc(n), title)

## singles
uk_vinyl_92_sing_df1 <- search_releases_single("UK", 1992, "Vinyl",
  discogs_key, discogs_secret, ua_string)

glimpse(uk_vinyl_92_sing_df1)


ukvinyl_sing_92_search_df2 %>%
  count(title) %>%
  arrange(desc(n), title)

ukvinyl_ep_92_filtered <- ukvinyl_ep_92_search_df %>%
  filter(!map_lgl(format, ~ "LP" %in% .x))

