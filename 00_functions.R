## functions for accessing discogs API

library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(tibble)
library(stringr)

# rate limit checker
respect_rate_limit <- function(res) {
  remaining <- as.integer(httr::headers(res)[["x-ratelimit-remaining"]])
  reset <- as.integer(httr::headers(res)[["x-ratelimit-reset"]])

  message("Rate limit remaining: ", remaining, " | Reset in: ", reset, " sec")

  if (isTRUE(!is.na(remaining)) && isTRUE(!is.na(reset)) && remaining <= 1) {
    message("Rate limit reached. Waiting ", reset, " seconds...")
    Sys.sleep(reset + 1)
  } else {
    Sys.sleep(1)  # Be polite: short wait between calls
  }
}

# gets all releases for a country for a year.
 # format commented out so will return everything. amend if needing only vinyl or cd
fetch_all_releases <- function(country, year, key, secret, user_agent_string) {
  base_url <- "https://api.discogs.com/database/search"
  page <- 1
  all_data <- list()

  repeat {
    message("Fetching page ", page, " for ", country, " ", year)

    res <- GET(
      base_url,
      user_agent(user_agent_string),
      query = list(
        key     = key,
        secret  = secret,
        #format  = "Vinyl",
        country = country,
        year    = year,
        type    = "release",
        #type    = "master",
        per_page = 100,
        page     = page
      )
    )

    # Check for errors
    stop_for_status(res)

    # Rate limit check
    respect_rate_limit(res)

    # Parse response
    json_data <- fromJSON(content(res, "text", encoding = "UTF-8"), flatten = TRUE)

    # Append results
    if (!"results" %in% names(json_data)) break
    all_data[[page]] <- as_tibble(json_data$results)

    # Check if more pages exist
    total_pages <- json_data$pagination$pages
    if (page >= total_pages) break

    page <- page + 1
  }

  # Combine all pages into one dataframe
  full_df <- bind_rows(all_data)

  return(full_df)
}


# clean the dataframe returned in fetch_all_releases()
 # parses out nested dfs and separates artist from artist-title single column
 # amend final select statement to include more columns

## clean data
clean_discogs_data <- function(df) {
  # Make sure required columns exist
  required_cols <- c("country", "year", "id", "master_id", "label", "catno", "title", "format", "genre", "style")
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop("Missing columns in input data: ", paste(missing_cols, collapse = ", "))
  }

  # Handle title splitting into artist and title
  df <- df %>%
    mutate(
      title = str_remove(title, "\\s*\\(\\d+\\)"),              # remove e.g., (2)
      artist = str_trim(str_extract(title, "^[^-]+")),
      release_title = str_trim(str_remove(title, "^[^-]+-"))    # title after dash
    )

  # Expand genre/style/format columns (which are list-cols) into numbered columns
  expand_list_column <- function(column_data, prefix) {
    max_len <- max(purrr::map_int(column_data, length), na.rm = TRUE)
    padded <- purrr::map(column_data, ~ c(.x, rep(NA, max_len - length(.x))))
    padded_matrix <- do.call(rbind, padded)
    as_tibble(padded_matrix, .name_repair = ~ paste0(prefix, seq_len(max_len)))
  }

  genre_cols  <- expand_list_column(df$genre,  "genre")
  style_cols  <- expand_list_column(df$style,  "style")
  format_cols <- expand_list_column(df$format, "format")

  # Base columns
  base_df <- df %>%
    select(country, year, id, master_id, label, catno, artist, release_title)

  # Combine all parts
  final_df <- bind_cols(base_df, format_cols, genre_cols, style_cols)

  return(final_df)
}

## example usage

# load Discogs API credentials
discogs_key    <- Sys.getenv("DISCOGS_KEY")
discogs_secret <- Sys.getenv("DISCOGS_SECRET")
ua_string      <- "MyDiscogsApp/1.0 +https://example.com"

# Fetch a year of data
den_88_raw <- fetch_all_releases("Denmark", 1988, discogs_key, discogs_secret, ua_string)

# clean fetched data
den_88_clean<- clean_discogs_data(den_88_raw)

