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
# format commented out so will return everything. amend if needing only vinyl or cd

# gets all vinyl releases for a country for a year.

fetch_all_releases_v <- function(country, year, key, secret, user_agent_string) {
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
        format  = "Vinyl",
        country = country,
        year    = year,
        type    = "release",
        per_page = 100,
        page     = page
      )
    )

    # Check for errors
    stop_for_status(res)
    respect_rate_limit(res)

    json_data <- fromJSON(content(res, "text", encoding = "UTF-8"), flatten = TRUE)

    # Check if results exist
    if (!"results" %in% names(json_data)) break

    # Convert to tibble
    results_df <- as_tibble(json_data$results)

    # Filter to only releases where 'LP' appears in the format descriptions
    # 'format' is a list-column of lists; we check if "LP" exists in any of those
    results_df <- results_df %>%
      filter(map_lgl(format, ~ any(grepl("LP", unlist(.x), ignore.case = TRUE))))

    # Append filtered page to results
    all_data[[page]] <- results_df

    # Stop if last page
    total_pages <- json_data$pagination$pages
    if (page >= total_pages) break

    page <- page + 1
  }

  # Combine all pages into one dataframe
  full_df <- bind_rows(all_data)

  return(full_df)
}


# gets all cd releases for a country for a year.
fetch_all_releases_cd <- function(country, year, key, secret, user_agent_string) {
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
        format  = "CD",
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

 # gets all master releases on
fetch_all_releases_m <- function(country, year, key, secret, user_agent_string) {
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
        #type    = "release",
        type    = "master",
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

  label_cols  <- expand_list_column(df$label,  "label")
  genre_cols  <- expand_list_column(df$genre,  "genre")
  style_cols  <- expand_list_column(df$style,  "style")
  format_cols <- expand_list_column(df$format, "format")

  # Base columns
  base_df <- df %>%
    select(country, year, id, master_id, catno, artist, release_title)

  # Combine all parts
  final_df <- bind_cols(base_df, label_cols, format_cols, genre_cols, style_cols)

  return(final_df)
}

clean_release_data2 <- function(release) {
  labels <- release$labels
  companies <- release$companies

  # Extract labels (true release label info)
  label_names <- tryCatch(labels$name, error = function(e) NA_character_)
  catnos <- tryCatch(labels$catno, error = function(e) NA_character_)

  # Extract companies by role
  extract_company <- function(role) {
    match <- purrr::keep(companies, ~ .x$entity_type_name == role)
    if (length(match) > 0) {
      return(match[[1]]$name)
    } else {
      return(NA_character_)
    }
  }

  tibble::tibble(
    release_id   = release$id %||% NA_integer_,
    master_id    = release$master_id %||% NA_integer_,
    title        = release$title %||% NA_character_,
    year         = release$year %||% NA_integer_,
    country      = release$country %||% NA_character_,
    status       = release$status %||% NA_character_,
    artist_name  = tryCatch(release$artists[[1]]$name, error = function(e) NA_character_),

    # True label info
    label1       = label_names[[1]] %||% NA_character_,
    label2       = label_names[[2]] %||% NA_character_,
    catno1       = catnos[[1]] %||% NA_character_,
    catno2       = catnos[[2]] %||% NA_character_,

    # Format info
    format1      = tryCatch(release$formats$descriptions[[1]][1], error = function(e) NA_character_),
    format2      = tryCatch(release$formats$descriptions[[1]][2], error = function(e) NA_character_),
    format3      = tryCatch(release$formats$descriptions[[1]][3], error = function(e) NA_character_),

    # Genres/Styles
    genre1       = tryCatch(release$genres[[1]], error = function(e) NA_character_),
    genre2       = tryCatch(release$genres[[2]], error = function(e) NA_character_),
    style1       = tryCatch(release$styles[[1]], error = function(e) NA_character_),
    style2       = tryCatch(release$styles[[2]], error = function(e) NA_character_),

    # Associated companies
    mastered_at  = extract_company("Mastered At"),
    produced_for = extract_company("Produced For"),
    designed_at  = extract_company("Designed At"),
    published_by1 = extract_company("Published By"),
    licensed_to   = extract_company("Licensed To"),
    copyright_by  = extract_company("Copyright ℗")
  )
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


###
get_full_release_data <- function(release_ids, key, secret, user_agent_string) {
  release_list <- purrr::map(
    release_ids,
    function(id) {
      message("Fetching full data for release ID: ", id)
      release <- tryCatch(
        fetch_release_raw(id, key, secret, user_agent_string),
        error = function(e) {
          message("Failed to fetch release ID ", id, ": ", conditionMessage(e))
          return(NULL)
        }
      )
      if (!is.null(release)) clean_release_data(release) else NULL
    }
  )

  # Bind the results into a single tibble
  dplyr::bind_rows(release_list)
  # or tibble::list_rbind(release_list)  # if you prefer using `tibble`
}


##

fetch_artist_releases <- function(artist_id, key, secret, user_agent_string) {
  base_url <- paste0("https://api.discogs.com/artists/", artist_id, "/releases")
  page <- 1
  all_data <- list()

  repeat {
    message("Fetching page ", page, " for artist ID ", artist_id)

    res <- GET(
      base_url,
      user_agent(user_agent_string),
      query = list(
        key      = key,
        secret   = secret,
        per_page = 100,
        page     = page
      )
    )

    # Check for errors
    stop_for_status(res)

    # Rate limit check
    respect_rate_limit(res)

    # Parse JSON safely
    json_data <- fromJSON(content(res, "text", encoding = "UTF-8"), flatten = TRUE)

    # Ensure valid data structure
    if (!"releases" %in% names(json_data)) break

    # Store the current page's data
    all_data[[page]] <- as_tibble(json_data$releases)

    # Stop if last page
    total_pages <- json_data$pagination$pages
    if (page >= total_pages) break

    page <- page + 1
  }

  # Combine all into one tibble
  full_df <- bind_rows(all_data)

  return(full_df)
}



## sample use
artist_df <- fetch_artist_releases(
  artist_id = 10262,
  key = discogs_key,
  secret = discogs_secret,
  user_agent_string = ua_string
)

glimpse(artist_df)

fetch_release_raw <- function(release_id, key, secret, user_agent_string) {
  url <- paste0("https://api.discogs.com/releases/", release_id)

  res <- httr::GET(
    url,
    httr::user_agent(user_agent_string),
    query = list(
      key = key,
      secret = secret
    )
  )

  httr::stop_for_status(res)
  respect_rate_limit(res)

  # Return the full raw parsed JSON as a list
  jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"), flatten = FALSE)
}

raw_release <- fetch_release_raw(3055928, discogs_key, discogs_secret, ua_string)
str(raw_release, max.level = 2)

raw_release$ma



clean_release_data <- function(release) {
  tibble::tibble(
    release_id   = release$id %||% NA_integer_,
    master_id   = release$master_id %||% NA_integer_,
    title        = release$title %||% NA_character_,
    year         = release$year %||% NA_integer_,
    country      = release$country %||% NA_character_,
    status       = release$status %||% NA_character_,

    artist_name  = tryCatch(release$artists$name[[1]], error = function(e) NA_character_),
    label1        = tryCatch(release$labels$name[[1]], error = function(e) NA_character_),
    label2        = tryCatch(release$labels$name[[2]], error = function(e) NA_character_),
    catno1        = tryCatch(release$labels$catno[[1]], error = function(e) NA_character_),
    catno2        = tryCatch(release$labels$catno[[2]], error = function(e) NA_character_),

    format1      = tryCatch(release$formats$descriptions[[1]][1], error = function(e) NA_character_),
    format2      = tryCatch(release$formats$descriptions[[1]][2], error = function(e) NA_character_),
    format3      = tryCatch(release$formats$descriptions[[1]][3], error = function(e) NA_character_),

    genre1       = tryCatch(release$genres[[1]], error = function(e) NA_character_),
    genre2       = tryCatch(release$genres[[2]], error = function(e) NA_character_),
    style1       = tryCatch(release$styles[[1]], error = function(e) NA_character_),
    style2       = tryCatch(release$styles[[2]], error = function(e) NA_character_)

  )
}


release_id <- 3055928

cleaned_release <- clean_release_data(raw_release)

glimpse(cleaned_release)
