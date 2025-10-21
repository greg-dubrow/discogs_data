
library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(tibble)
library(stringr)

respect_rate_limit <- function(res) {
  remaining <- as.integer(httr::headers(res)[["x-ratelimit-remaining"]])
  reset <- as.integer(httr::headers(res)[["x-ratelimit-reset"]])

  message("Rate limit remaining: ", remaining, " | Reset in: ", reset, " sec")

  if (!is.na(remaining) && !is.na(reset) && remaining <= 1) {
    message("Rate limit reached. Waiting ", reset, " seconds...")
    Sys.sleep(reset + 1)
  } else {
    Sys.sleep(1)  # Be polite
  }
}


## set up cache directory
if (!dir.exists("cache")) dir.create("cache")

# This pulls from the Discogs search endpoint, applies
  # format + sub-format filters before returning.

search_releases <- function(country, year, format, key, secret, user_agent_string) {
  # Ensure format is correctly handled
  valid_formats <- c("Vinyl", "CD", "Cassette")
  if (!format %in% valid_formats) {
    stop("Unsupported format. Please choose one of: ", paste(valid_formats, collapse = ", "))
  }

  base_url <- "https://api.discogs.com/database/search"
  page <- 1
  all_data <- list()

  # Define required formats based on input
  format_filter <- switch(format,
    "Vinyl"    = list(c("Vinyl", "LP")),
    "CD"       = list(c("CD", "Album")),
    "Cassette" = list(c("Cassette", "Album"))
  )

  repeat {
    message("Fetching page ", page, " for ", country, " ", year)

    res <- httr::GET(
      base_url,
      httr::user_agent(user_agent_string),
      query = list(
        key     = key,
        secret  = secret,
        format  = format_filter[[1]][1],
        country = country,
        year    = year,
        type    = "release",
        per_page = 100,
        page     = page
      )
    )

    httr::stop_for_status(res)
    respect_rate_limit(res)

    json_data <- jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"), flatten = TRUE)

    if (!"results" %in% names(json_data)) break

    # Filter by secondary format (e.g., LP or Album)
    filtered <- dplyr::filter(json_data$results,
      purrr::map_lgl(format, ~ format_filter[[1]][2] %in% .x)
    )

    all_data[[page]] <- tibble::as_tibble(filtered)

    if (page >= json_data$pagination$pages) break
    page <- page + 1
  }

  dplyr::bind_rows(all_data)
}


# This safely fetches full release JSON for a vector of IDs.

# fetch_release_raw <- function(release_id, key, secret, user_agent_string) {
#   url <- paste0("https://api.discogs.com/releases/", release_id)
#
#   res <- httr::GET(
#     url,
#     httr::user_agent(user_agent_string),
#     query = list(key = key, secret = secret)
#   )
#
#   httr::stop_for_status(res)
#   respect_rate_limit(res)
#
#   jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"), flatten = FALSE)
# }

fetch_release_raw_cached <- function(release_id, key, secret, user_agent_string, cache_dir = "cache") {
  if (!dir.exists(cache_dir)) dir.create(cache_dir)

  # Build cache file path
  cache_file <- file.path(cache_dir, paste0(release_id, ".json"))

  # If cached, load from disk
  if (file.exists(cache_file)) {
    message("Reading release ", release_id, " from cache")
    return(jsonlite::fromJSON(cache_file, flatten = FALSE))
  }

  # Otherwise, fetch from API
  url <- paste0("https://api.discogs.com/releases/", release_id)

  res <- httr::GET(
    url,
    httr::user_agent(user_agent_string),
    query = list(key = key, secret = secret)
  )

  httr::stop_for_status(res)
  respect_rate_limit(res)

  parsed <- jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"), flatten = FALSE)

  # Save to cache
  jsonlite::write_json(parsed, cache_file, pretty = TRUE, auto_unbox = TRUE)

  return(parsed)
}


# fetch_release_details <- function(release_ids, key, secret, user_agent_string) {
#   release_list <- purrr::map(
#     release_ids,
#     function(id) {
#       message("Fetching release ID: ", id)
#       release <- tryCatch(
#         fetch_release_raw(id, key, secret, user_agent_string),
#         error = function(e) {
#           message("Failed to fetch release ", id, ": ", conditionMessage(e))
#           return(NULL)
#         }
#       )
#       release
#     }
#   )
#
#   # Return list of release JSONs
#   compact(release_list)
# }


fetch_release_details <- function(release_ids, key, secret, user_agent_string, cache_dir = "cache") {
  release_list <- purrr::map(
    release_ids,
    function(id) {
      message("Fetching release ID: ", id)
      tryCatch(
        fetch_release_raw_cached(id, key, secret, user_agent_string, cache_dir),
        error = function(e) {
          message("Failed to fetch release ", id, ": ", conditionMessage(e))
          NULL
        }
      )
    }
  )

  purrr::compact(release_list)
}


# This parses the release and cleanly separates labels and companies.
clean_release_data <- function(release) {
  `%||%` <- function(a, b) if (!is.null(a)) a else b

  get_first_n <- function(vec, n = 2) {
    if (is.null(vec) || length(vec) == 0) return(rep(NA_character_, n))
    vec <- as.character(vec)
    length(vec) <- n
    vec[is.na(vec)] <- NA_character_
    vec
  }

  # --- Artists ---
  artist <- if (!is.null(release$artists) && length(release$artists) > 0) {
    release$artists$name[[1]] %||% NA_character_
  } else {
    NA_character_
  }

  # # --- First Label (just name) ---
  # label1 <- if (
  #   !is.null(release$labels) && is.data.frame(release$labels) &&
  #     "name" %in% names(release$labels)) {
  #   release$labels$name[1] %||% NA_character_
  # } else {
  #   NA_character_
  # }

  # --- Catno ---
  catno1 <- if (
    !is.null(release$labels) && is.data.frame(release$labels) &&
      "catno" %in% names(release$labels)) {
    release$labels$catno[1] %||% NA_character_
  } else {
    NA_character_
  }

  # --- Labels ---
  label1 <- label2 <- catno1 <- catno2 <- NA_character_

  if (!is.null(release$labels) && is.data.frame(release$labels)) {
    if (nrow(release$labels) >= 1) {
      label1 <- release$labels$name[1] %||% NA_character_
      catno1 <- release$labels$catno[1] %||% NA_character_
    }
    if (nrow(release$labels) >= 2) {
      label2 <- release$labels$name[2] %||% NA_character_
      catno2 <- release$labels$catno[2] %||% NA_character_
    }
  }

  # # --- Labels ---
  # label_names <- if (!is.null(release$labels)) {
  #   purrr::map_chr(release$labels, function(x) if (is.list(x) && "name" %in% names(x)) x$name else NA_character_)
  # } else {
  #   rep(NA_character_, 2)
  # }
  #
  # catnos <- if (!is.null(release$labels)) {
  #   purrr::map_chr(release$labels, function(x) if (is.list(x) && "catno" %in% names(x)) x$catno else NA_character_)
  # } else {
  #   rep(NA_character_, 2)
  # }
#
#   catnos <- if (!is.null(release$labels)) {
#     purrr::map_chr(release$labels, function(x) if (is.list(x) && "catno" %in% names(x)) x$catno else NA_character_)
#   } else {
#     rep(NA_character_, 2)
#   }
#
#   # --- Formats ---
#   formats <- tryCatch({
#     desc <- purrr::pluck(release, "formats", 1, "descriptions", .default = NA_character_)
#     if (is.list(desc)) unlist(desc)[1:2] else rep(desc, length.out = 2)
#   }, error = function(e) rep(NA_character_, 2))
#

  # --- Genres ---
  genre1 <- purrr::pluck(release, "genres", 1, .default = NA_character_)
  genre2 <- purrr::pluck(release, "genres", 2, .default = NA_character_)
  genre1 <- if (is.list(genre1)) unlist(genre1)[1] else genre1
  genre2 <- if (is.list(genre2)) unlist(genre2)[1] else genre2

  # --- Styles ---
  style1 <- purrr::pluck(release, "styles", 1, .default = NA_character_)
  style2 <- purrr::pluck(release, "styles", 2, .default = NA_character_)
  style1 <- if (is.list(style1)) unlist(style1)[1] else style1
  style2 <- if (is.list(style2)) unlist(style2)[1] else style2

  # --- Companies ---
  if (!is.null(release$companies) && is.data.frame(release$companies)) {
    company_df <- release$companies %>%
      dplyr::select(entity_type_name, name) %>%
      dplyr::filter(!is.na(entity_type_name) & !is.na(name)) %>%
      dplyr::distinct() %>%
      tidyr::pivot_wider(
        names_from = entity_type_name,
        values_from = name,
        values_fn = list  # Keep list to avoid conflicts
      )
  } else {
    company_df <- tibble::tibble()
  }

  # --- Notes ---
  note1 <- if (!is.null(release$notes) && length(release$notes) > 0) {
    release$notes %||% NA_character_
  } else {
    NA_character_
  }


  tibble(
    release_id    = release$id %||% NA_integer_,
    master_id     = release$master_id %||% NA_integer_,
    title         = release$title %||% NA_character_,
    year          = release$year %||% NA_integer_,
    country       = release$country %||% NA_character_,
    status        = release$status %||% NA_character_,
    artist        = artist,
    catno1        = catno1,
    label1        = label1,
    label2        = label2,
    # format1       = formats[1],
    # format2       = formats[2],
    genre1        = genre1,
    genre2        = genre2,
    style1        = style1,
    style2        = style2,
    mastered_at   = company_df$`Mastered At`[[1]] %||% NA_character_,
    published_by  = company_df$`Published By`[[1]] %||% NA_character_,
    licensed_to   = company_df$`Licensed To`[[1]] %||% NA_character_,
    copyright_by  = company_df$`Copyright ©`[[1]] %||% NA_character_,
    distributed_by = company_df$`Distributed By`[[1]] %||% NA_character_,
    pressed_by     = company_df$`Pressed By`[[1]] %||% NA_character_,
    note1         = note1
  )
}




# This cleans a whole batch of full releases.
clean_all_releases <- function(release_json_list) {
  cleaned_list <- purrr::map(release_json_list, clean_release_data)
  dplyr::bind_rows(cleaned_list)
}

clean_all_releases <- function(release_json_list) {
  safe_clean <- purrr::safely(clean_release_data)
  cleaned_list <- purrr::map(release_json_list, safe_clean)

  # Pull out errors
  errors <- purrr::keep(cleaned_list, ~ !is.null(.x$error))
  n_errors <- length(errors)

  if (n_errors > 0) {
    message("Some records failed to clean: ", n_errors)
    message("First error:")
    print(errors[[1]]$error)
  }

  results <- purrr::map(cleaned_list, "result") %>% purrr::compact()

  if (length(results) == 0) {
    warning("No results to return.")
    return(tibble())
  }

  dplyr::bind_rows(results)
}



## sample usage
# Step 1: Search filtered releases (e.g., only Vinyl LPs)
search_df <- search_releases("UK", 1990, "Vinyl",
  discogs_key, discogs_secret, ua_string)

test_ids <- head(search_df$id, 100)

# Step 2: Pull full details
release_jsons <- fetch_release_details(test_ids,
  discogs_key, discogs_secret, ua_string)

release_jsons <- fetch_release_details(search_df$id,
  discogs_key, discogs_secret, ua_string)

# Step 3: Clean
final_df <- clean_all_releases(release_jsons)

glimpse(final_df)


####
# clean_release_data <- function(release) {
#   # Safe extractor
#   safe_extract <- function(x, path, default = NA) {
#     tryCatch(eval(parse(text = path)), error = function(e) default)
#   }
#
#   # Extract company roles
#   #company_roles <- purrr::map_chr(release$companies, ~ .x$entity_type_name %||% NA_character_)
#   #  company_names <- purrr::map_chr(release$companies, ~ .x$name %||% NA_character_)
#
#   if (!is.null(release$companies)) {
#     company_roles <- purrr::map_chr(release$companies, function(x) {
#       if (is.list(x) && "entity_type_name" %in% names(x)) {
#         x$entity_type_name
#       } else {
#         NA_character_
#       }
#     })
#
#     company_names <- purrr::map_chr(release$companies, function(x) {
#       if (is.list(x) && "name" %in% names(x)) {
#         x$name
#       } else {
#         NA_character_
#       }
#     })
#   } else {
#     company_roles <- character(0)
#     company_names <- character(0)
#   }
#
#   company_df <- tibble::tibble(role = company_roles, name = company_names) %>%
#     tidyr::pivot_wider(
#       names_from = role,
#       values_from = name,
#       names_prefix = "",
#       values_fn = list
#     )
#
#
#   tibble::tibble(
#     release_id   = release$id %||% NA_integer_,
#     master_id    = release$master_id %||% NA_integer_,
#     title        = release$title %||% NA_character_,
#     year         = release$year %||% NA_integer_,
#     country      = release$country %||% NA_character_,
#     status       = release$status %||% NA_character_,
#     artist       = safe_extract(release, "release$artists[[1]]$name"),
#     catno        = safe_extract(release, "release$labels[[1]]$catno"),
#     label1       = safe_extract(release, "release$labels[[1]]$name"),
#     label2       = safe_extract(release, "release$labels[[2]]$name"),
#     format1      = safe_extract(release, "release$formats[[1]]$descriptions[[1]]"),
#     format2      = safe_extract(release, "release$formats[[1]]$descriptions[[2]]"),
#     genre1       = safe_extract(release, "release$genres[[1]]"),
#     genre2       = safe_extract(release, "release$genres[[2]]"),
#     style1       = safe_extract(release, "release$styles[[1]]"),
#     style2       = safe_extract(release, "release$styles[[2]]"),
#     mastered_at  = company_df$`Mastered At`[[1]] %||% NA_character_,
#     published_by = company_df$`Published By`[[1]] %||% NA_character_,
#     licensed_to  = company_df$`Licensed To`[[1]] %||% NA_character_,
#     copyright_by = company_df$`Copyright ©`[[1]] %||% NA_character_
#   )
# }

## possible fix for companies
# --- Companies ---
# if (!is.null(release$companies) && is.data.frame(release$companies)) {
#   company_df <- release$companies %>%
#     dplyr::select(entity_type_name, name) %>%
#     dplyr::filter(!is.na(entity_type_name) & !is.na(name)) %>%
#     dplyr::distinct() %>%
#     tidyr::pivot_wider(
#       names_from = entity_type_name,
#       values_from = name,
#       values_fn = list  # Keep list to avoid conflicts
#     )
# } else {
#   company_df <- tibble::tibble()
# }
#
# # then in tibble creation
# mastered_at   = company_df$`Mastered At`[[1]] %||% NA_character_,
# published_by  = company_df$`Published By`[[1]] %||% NA_character_,
# licensed_to   = company_df$`Licensed To`[[1]] %||% NA_character_,
# copyright_by  = company_df$`Copyright ©`[[1]] %||% NA_character_
# distributed_by = company_df$`Distributed By`[[1]] %||% NA_character_,
# pressed_by     = company_df$`Pressed By`[[1]] %||% NA_character_,
