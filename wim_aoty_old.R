## pulls one year of release data for UK and US for WiM AoTY page

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


# 1990 -------------------------------------------------------------

 # UK 1990

# gets vinyl only
uk_90_raw_v <- fetch_all_releases_v("UK", 1990, discogs_key, discogs_secret, ua_string)

uk_90_raw_v$label[1]

companies_df <- map_dfr(release$companies, ~ tibble(
  release_id = release$id,
  role = .x$entity_type_name,
  company = .x$name
))


uk_90_raw_v_full_clean <- get_full_release_data(
  release_ids = uk_90_raw_v$id,
  key = discogs_key,
  secret = discogs_secret,
  user_agent_string = ua_string
)

glimpse(uk_90_raw_v_full_clean)

# clean fetched vinyl data
uk_90_clean_v <- clean_discogs_data(uk_90_raw_v)

uk_90_clean_v %>%
  filter(artist == "Duran Duran") %>%
  filter(release_title == "Liberty") %>%
  glimpse()
  select(artist, release_title, id, master_id, catno, format1:format15) %>%
  glimpse()

uk_90_clean_v2 <- clean_release_data2(uk_90_raw_v)

glimpse(uk_90_clean_v2)

glimpse(uk_90_raw)
uk_90_raw %>%
  count(type)

# gets master releases
uk_90_raw_m <- fetch_all_releases_m("UK", 1990, discogs_key, discogs_secret, ua_string)

uk_90_raw_m %>%
  count(type)

# clean fetched master data
uk_90_clean_m <- clean_discogs_data(uk_90_raw)


# clean fetched data
uk_90_clean<- clean_discogs_data(uk_90_raw)

glimpse(uk_90_clean)
head(uk_90_clean)
colnames(uk_90_clean)

saveRDS(uk_90_clean, "data/uk_90_clean.RDS")

uk_90_clean %>%
  count(format1) %>%
  view()

uk_90_clean %>%
  filter(artist == "Duran Duran") %>%
  filter(release_title == "Liberty") %>%
  select(artist, release_title, id, master_id, catno, format1:format15) %>%
  glimpse()

uk_90_clean_m <- clean_discogs_data(uk_90_raw_m)


# dedupe based on artist and title

uk_90_clean %>%
  mutate(
    is_vhs = as.integer(
      coalesce(if_any(c(format1:format27), ~ .x == "VHS"), FALSE))) %>%
  select(country:label3, format1:format27, is_vhs) %>%
  view()

uk_1990_final <- uk_90_clean %>%
  filter(!format1 == "VHS") %>%
  filter(!format1 == "Acetate") %>%
  mutate(
    is_vhs = as.integer(
      coalesce(if_any(c(format1:format27), ~ .x == "VHS"), FALSE))) %>%
  filter(is_vhs == 0) %>%
  mutate(
    is_single = as.integer(
      coalesce(if_any(c(format2:format27), ~ .x %in%
          c("7\"", "Single", "Maxi-Single"), FALSE)))) %>%
  mutate(
    is_reissue = as.integer(
      coalesce(if_any(c(format2:format27), ~ .x == "Reissue"), FALSE))) %>%
  mutate(
    is_album = as.integer(
      coalesce(if_any(c(format2:format27), ~ .x == "Album"), FALSE))) %>%
  # mutate(
  #   is_other = as.integer(
  #     coalesce(if_any(c(format2:format27), ~ .x %in%
  #         c("White Label", "Promo", "Test Pressing"), FALSE)))) %>%
  filter(is_single == 0) %>%
  filter(is_reissue == 0) %>%
#  filter(is_other == 0) %>%
  distinct(artist, release_title, .keep_all = TRUE) %>%
  select(country:label1, format1:format5, is_album)

colnames(uk_1990_final)

uk_1990_final %>%
  filter(artist == "Duran Duran") %>%
  select(artist, release_title, format1:format5, is_album) %>%
  view()


## US 1990

us_90_raw <- fetch_all_releases("US", 1990, discogs_key, discogs_secret, ua_string)

glimpse(us_90_raw)

# clean fetched data
us_90_clean<- clean_discogs_data(us_90_raw)

glimpse(us_90_clean)
head(us_90_clean)
colnames(us_90_clean)

saveRDS(us_90_clean, "data/us_90_clean.RDS")

us_1990_final <- us_90_clean %>%
  filter(!format1 == "VHS") %>%
  filter(!format1 == "Acetate") %>%
  mutate(
    is_vhs = as.integer(
      coalesce(if_any(c(format1:format10), ~ .x == "VHS"), FALSE))) %>%
  filter(is_vhs == 0) %>%
  mutate(
    is_single = as.integer(
      coalesce(if_any(c(format2:format10), ~ .x %in%
          c("7\"", "Single", "Maxi-Single"), FALSE)))) %>%
  mutate(
    is_reissue = as.integer(
      coalesce(if_any(c(format2:format10), ~ .x == "Reissue"), FALSE))) %>%
  mutate(
    is_album = as.integer(
      coalesce(if_any(c(format2:format10), ~ .x == "Album"), FALSE))) %>%
  # mutate(
  #   is_other = as.integer(
  #     coalesce(if_any(c(format2:format27), ~ .x %in%
  #         c("White Label", "Promo", "Test Pressing"), FALSE)))) %>%
  filter(is_single == 0) %>%
  filter(is_reissue == 0) %>%
  #  filter(is_other == 0) %>%
  distinct(artist, release_title, .keep_all = TRUE) %>%
  select(country:label1, format1:format5, is_album)

us_1990_final %>%
  filter(artist == "A Tribe Called Quest") %>%
  view()

final_1990 <- uk_1990_final %>%
  rbind(us_1990_final) %>%
  distinct(artist, release_title, .keep_all = TRUE)

glimpse(final_1990)
