# ==============================================================================
# Data Cleaning and Validation Functions
# ==============================================================================
# A collection of functions for validating, cleaning, and converting data types
# in data frames, with specific support for bid data processing.

library(logger)
library(glue)
library(dplyr)
library(tidyverse)
library(tidyr)
library(rlang)
library(lubridate)
library(tictoc)
library(here)
library(arrow)
library(jsonlite)
library(tigris)   # for zctas()
library(sf)       # for spatial functions
library(stringr)  # for string cleaning
library(profvis)
library(zipcodeR)

log_appender(appender_tee("logs/data_cleaning.log"))

options(tigris_use_cache = TRUE)

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================
#' Load Oregon ZCTA (ZIP Code Tabulation Area) spatial data
#'
#' Loads Oregon ZCTA polygons from a cached parquet file. If the cache doesn't
#' exist, downloads full US ZCTAs from the Census Bureau via tigris, filters
#' to Oregon (ZIPs starting with "97"), and caches the result for future use.
#'
#' @param zips_oregon_path Path to the Oregon ZCTA parquet cache file
#'   (default: here("data", "zips_zcta_oregon.parquet"))
#' @return An sf object containing Oregon ZCTA polygons with ZCTA5CE20 column
#' @examples
#' zips <- load_oregon_zips()
load_oregon_zips <- function(zips_oregon_path = here("data", "zips_zcta_oregon.parquet")) {
  cat("\n", "Loading Oregon ZCTA data", "\n")
  if (file.exists(zips_oregon_path)) {
    cat("Loading Oregon ZCTA data from cached parquet...\n")
    zips <- sfarrow::st_read_parquet(zips_oregon_path)
  } else {
    cat("Oregon cache not found. Loading full US ZCTAs and filtering...\n")
    # Download full US (state param doesn't work for 2020)
    zips_full <- zctas(year = 2020)
    # Filter to Oregon only (ZIPs starting with 97)
    zips <- zips_full %>% filter(str_starts(.data$ZCTA5CE20, "97"))
    cat(glue("Filtered to {nrow(zips)} Oregon ZCTAs (from {nrow(zips_full)} US total)\n"))
    # Cache Oregon-only for future runs
    cat("Caching Oregon ZCTA data to parquet...\n")
    sfarrow::st_write_parquet(zips, zips_oregon_path)
  }

  zips
}

# ==============================================================================
# VALIDATION FUNCTIONS
# ==============================================================================

#' Check if expected columns exist in a data frame
#'
#' @param df A data frame to check
#' @param expected_columns Character vector of expected column names
#' @return Character vector of missing column names (invisible)
#' @examples
#' check_columns(df, c("id", "name", "age"))
check_columns <- function(df, expected_columns) {
  if (!is.list(expected_columns) && !is.character(expected_columns)) {
    msg <- "expected_columns must be a list or character vector"
    log_error(msg)
    stop(msg)
  }

  expected_columns <- as.character(expected_columns)
  missing_columns <- setdiff(expected_columns, colnames(df))

  if (length(missing_columns) == length(expected_columns)) {
    msg <- "All expected columns are missing from the dataframe"
    log_error(msg)
    stop(msg)
  } else if (length(missing_columns) > 0) {
    warning(glue("Missing columns:\n  {paste(missing_columns, collapse = '\n  ')}"))
  }

  missing_columns
}

#' Check column types against expected types
#'
#' @param df A data frame to check
#' @param expected_types A data frame with columns: 'column', 'Expected_Type'
#' @return A data frame comparing actual vs expected types
#' @examples
#' expected <- data.frame(
#'   column = c("age", "name"),
#'   Expected_Type = c("numeric", "character")
#' )
#' check_column_types(df, expected)
check_column_types <- function(df, expected_types) {
  if (is.null(expected_types$column) || length(expected_types$column) == 0) {
    stop("expected_types must have a 'column' field with column names")
  }

  if (!all(sapply(expected_types$Expected_Type, is.character))) {
    stop("All values in Expected_Type must be character strings")
  }

  # Get actual types
  df_types <- sapply(df, function(col) class(col)[1])

  # Find common columns
  common_cols <- intersect(names(df), expected_types$column)

  # Create comparison
  type_comparison <- expected_types %>%
    filter(.data$column %in% common_cols) %>%
    mutate(
      actual = df_types[.data$column],
      .before = "Expected_Type"
    ) %>%
    mutate(
      match = .data$actual == .data$Expected_Type,
      .after = "Expected_Type"
    )

  type_comparison
}



# ==============================================================================
# DATA QUALITY ASSESSMENT FUNCTIONS
# ==============================================================================

#' Check for NA values by column
#'
#' @param df A data frame to check
#' @return Named integer vector of NA counts per column
check_na_by_column <- function(df) {
  colSums(is.na(df))
}

#' Get row indices containing NA values
#'
#' @param df A data frame to check
#' @return Integer vector of row indices with NA values
get_na_row_indices <- function(df) {
  which(!complete.cases(df))
}

#' Comprehensive NA check
#'
#' @param df A data frame to check
#' @return List with na_by_column and na_row_indices
check_na <- function(df) {
  list(
    na_by_column = check_na_by_column(df),
    na_row_indices = get_na_row_indices(df)
  )
}

#' Check for blank/empty strings by column
#'
#' @param df A data frame to check
#' @return Named integer vector of blank counts per column
check_blanks_by_column <- function(df) {
  sapply(df, function(col) {
    if (is.character(col)) {
      sum(col == "", na.rm = TRUE)
    } else {
      0
    }
  })
}

#' Get row indices containing blank values
#'
#' @param df A data frame to check
#' @return Integer vector of row indices with blank values
get_blank_row_indices <- function(df) {
  df %>%
    mutate(row_id = row_number()) %>%
    filter(if_any(where(is.character), ~ . == "")) %>%
    pull(.data$row_id)
}

#' Comprehensive blank check
#'
#' @param df A data frame to check
#' @return List with blanks_by_column and blank_row_indices
check_blanks <- function(df) {
  list(
    blanks_by_column = check_blanks_by_column(df),
    blank_row_indices = get_blank_row_indices(df)
  )
}

#' Find duplicate rows
#'
#' @param df A data frame to check
#' @param exclude_cols Column names to exclude from duplication check
#' @return List with duplicate_indices and num_duplicates
find_duplicates <- function(df, exclude_cols = c("row_id")) {
  check_cols <- setdiff(names(df), exclude_cols)
  duplicate_indices <- which(duplicated(df[, check_cols]))

  list(
    duplicate_indices = duplicate_indices,
    num_duplicates = length(duplicate_indices)
  )
}

#' Comprehensive data quality assessment
#'
#' @param df A data frame to assess
#' @param expected_types Optional data frame with expected column types
#' @param time_checks Whether to time each check operation
#' @return List containing all quality check results
assess_data_quality <- function(df, expected_types = NULL, time_checks = TRUE) {
  results <- list()

  # Column validation
  if (!is.null(expected_types)) {
    if (time_checks) tic("Column Check")
    results$missing_columns <- check_columns(df, expected_types$column)
    if (time_checks) toc()

    if (time_checks) tic("Type Check")
    results$type_comparison <- check_column_types(df, expected_types)
    if (time_checks) toc()
  }

  # Quality checks
  if (time_checks) tic("NA Check")
  results$na_check <- check_na(df)
  if (time_checks) toc()

  if (time_checks) tic("Blank Check")
  results$blank_check <- check_blanks(df)
  if (time_checks) toc()

  if (time_checks) tic("Duplicate Check")
  results$duplicate_check <- find_duplicates(df)
  if (time_checks) toc()

  class(results) <- "data_quality_report"
  results
}

#' Print method for data quality reports
#'
#' @param x A data_quality_report object
#' @param ... Additional arguments (unused)
print.data_quality_report <- function(x, ...) {
  cat("\n")
  cat(strrep("=", 70), "\n")
  cat("DATA QUALITY REPORT", "\n")
  cat(strrep("=", 70), "\n\n")

  if (!is.null(x$type_comparison)) {
    cat("Type Mismatches:\n")
    mismatches <- x$type_comparison %>% filter(!match)
    if (nrow(mismatches) > 0) {
      print(mismatches)
    } else {
      cat("  All types match expected types ✓\n")
    }
    cat("\n")
  }

  cat(glue("NA Values: {sum(x$na_check$na_by_column)} total across {sum(x$na_check$na_by_column > 0)} columns"), "\n")
  cat(glue("Blank Values: {sum(x$blank_check$blanks_by_column)} total across {sum(x$blank_check$blanks_by_column > 0)} columns"), "\n")
  cat(glue("Duplicate Rows: {x$duplicate_check$num_duplicates}"), "\n")

  cat("\n")
  cat(strrep("=", 70), "\n")
}

# ==============================================================================
# TYPE CONVERSION HELPERS
# ==============================================================================

#' Get type conversion function for a target type
#'
#' @param target_type Character string of target type
#' @return Function that converts to target type
get_type_func <- function(target_type) {
  converters <- list(
    character = as.character,
    numeric = as.numeric,
    integer = as.integer,
    logical = as.logical,
    Date = as.Date,
    POSIXct = function(x, ...) as.POSIXct(x, ...)
  )

  if (!target_type %in% names(converters)) {
    stop(glue("Unknown type: {target_type}\nSupported: {paste(names(converters), collapse = ', ')}"))
  }

  converters[[target_type]]
}

#' Get type checking function for a target type
#'
#' @param target_type Character string of target type
#' @return Function that checks if value is of target type
get_check_func <- function(target_type) {
  checkers <- list(
    character = is.character,
    numeric = is.numeric,
    integer = is.integer,
    logical = is.logical,
    Date = function(x) inherits(x, "Date"),
    POSIXct = function(x) inherits(x, "POSIXct")
  )

  if (!target_type %in% names(checkers)) {
    stop(glue("Unknown type: {target_type}"))
  }

  checkers[[target_type]]
}

# ==============================================================================
# GENERIC TYPE CONVERSION FUNCTION
# ==============================================================================

#' Convert a column to a target type with optional preprocessing
#'
#' @param df A data frame
#' @param col_name Name of the column to convert
#' @param target_type Target type as string (e.g., "numeric", "POSIXct")
#' @param preprocess_fn Optional preprocessing function
#' @param verbose Whether to print conversion messages
#' @param ... Additional arguments passed to conversion function
#' @return Data frame with converted column
#' @examples
#' # Convert with preprocessing
#' df <- convert_column(df, "PRICE", "numeric",
#'                     preprocess_fn = function(x) gsub("^O", "0", x))
#'
#' # Convert with format specification
#' df <- convert_column(df, "TIMESTAMP", "POSIXct",
#'                     format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
convert_column <- function(df, col_name, target_type,
                           preprocess_fn = NULL,
                           output_col_name = NULL,
                           verbose = TRUE,
                           ...) {

  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Converting {col_name} to {target_type}"), "\n")
    cat(strrep("=", 60), "\n")
  }

  # Get conversion and check functions
  converter <- get_type_func(target_type)
  checker <- get_check_func(target_type)

  # Check if already correct type
  if (checker(df[[col_name]])) {
    if (verbose) {
      cat(glue("Column {col_name} is already {target_type}. No conversion needed."), "\n\n")
    }
    return(df)
  }

  # Create new column name if not provided
  if (is.null(output_col_name)) {
    output_col_name <- paste0(col_name, "_clean")
  }

  # Apply preprocessing if provided otherwise copy source column
  if (!is.null(preprocess_fn)) {
    if (verbose) cat("Applying preprocessing...\n")
    df[[output_col_name]] <- preprocess_fn(df[[col_name]])
  }else{
    df[[output_col_name]] <- df[[col_name]]
  }

  # Perform conversion with error handling
  tryCatch({
    if (verbose) cat(glue("Converting {col_name}..."), "\n")
    df[[output_col_name]] <- converter(df[[output_col_name]], ...)

    if (verbose) {
      cat(glue("{output_col_name} is now: {class(df[[output_col_name]])[1]}"), "\n\n")
    }
  }, error = function(e) {
    log_error(glue("Failed to convert {output_col_name}: {e$message}"), "\n")
    stop(glue("Conversion failed for {output_col_name}: {e$message}"), "\n")
  })


  if (verbose) {
    num_na <- sum(is.na(df[[output_col_name]]))
    cat(glue("NA COUNT: \n There are {num_na} NAs in {output_col_name}"), "\n\n")
  }

  df
}

# ==============================================================================
# Column Cleaning Functions
# ==============================================================================

# Clean PRICE column
#
#' @param df A data frame
#' @param col_name Name of the column to convert to numeric
#' @param fix_leading_o gsub "O" --> 0
#' @param verbose Whether to print messages
#' @return Data frame with converted column
clean_price_column <- function(df, min_price = 0, max_price = 10,
                               output_col_name = "PRICE_clean", fix_leading_o = FALSE, verbose = TRUE) {

  preprocess <- if (fix_leading_o) {
    function(x) {
      problem_rows <- which(is.na(suppressWarnings(as.numeric(x))) & !is.na(x))
      if (verbose && length(problem_rows) > 0) {
        cat(glue("Found {length(problem_rows)} non-numeric value(s), attempting to fix..."), "\n")
      }
      gsub("^O", "0", x)
    }
  } else {
    NULL
  }

  df <- convert_column(df,
                       col_name = "PRICE",
                       target_type = "numeric",
                       preprocess_fn = preprocess,
                       output_col_name = "PRICE_clean",
                       verbose = verbose)

  df %>%
    mutate(PRICE_final = case_when(
      .data[[output_col_name]] <= min_price ~ NA_real_,
      .data[[output_col_name]] > max_price ~ NA_real_,
      TRUE ~ .data[[output_col_name]]
    ))

}


#' Clean and standardize geographic region column
#'
#' Standardizes the DEVICE_GEO_REGION column by converting Oregon-related
#' values (e.g., "or", "oregon", "xor") to a consistent "OR" format.
#' Non-Oregon values are set to NA.
#'
#' @param df A data frame containing a DEVICE_GEO_REGION column
#' @param codes_errors Character vector of regex patterns identifying Oregon variants
#'   (default: c("^or$", "oregon", "xor", "^or[^a-z]*$"))
#' @param output_col_name Name for the cleaned output column
#'   (default: "DEVICE_GEO_REGION_clean")
#' @param verbose Logical; if TRUE, prints cleaning summary statistics
#'   (default: TRUE)
#' @return Data frame with new DEVICE_GEO_REGION_clean column added
#' @examples
#' df_clean <- clean_geo_region_column(bids_df)
clean_geo_region_column <- function(df,
                                    codes_errors = c("^or$", "oregon", "xor", "^or[^a-z]*$"),
                                    output_col_name = "DEVICE_GEO_REGION_clean",
                                    verbose = TRUE) {

  df <- df %>%
    mutate(
      DEVICE_GEO_REGION_clean = case_when(
        str_to_lower(str_trim(DEVICE_GEO_REGION)) %in% c("or", "oregon", "xor") ~ "OR",
        TRUE ~ NA_character_
      )
    )

  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Cleaning DEVICE_GEO_REGION column"), "\n")
    cat(strrep("=", 60), "\n")
    cat(glue("Current values in DEVICE_GEO_REGION:\n"))
    print(table(df$DEVICE_GEO_REGION, useNA = "always"))

    df %>%
      mutate(region_lower = stringr::str_to_lower(stringr::str_trim(.data$DEVICE_GEO_REGION))) %>%
      count(.data$region_lower, sort = TRUE) %>%
      filter(
        str_detect(.data$region_lower, "^or$") |
        str_detect(.data$region_lower, "oregon") |
        str_detect(.data$region_lower, "xor") |
        str_detect(.data$region_lower, "^or[^a-z]*$")
      ) %>%
      print()

    cat("\n",
        glue::glue("Number of NA values in DEVICE_GEO_REGION_clean: {sum(is.na(df$DEVICE_GEO_REGION_clean))}"),
        "\n")
  }

  df
}


#' Clean and validate ZIP codes with spatial recovery
#'
#' Cleans the DEVICE_GEO_ZIP column by removing sentinel values and invalid
#' formats. Uses spatial join with Oregon ZCTA polygons to recover missing
#' ZIP codes from lat/long coordinates (DEVICE_GEO_LAT, DEVICE_GEO_LONG).
#'
#' @param df A data frame containing DEVICE_GEO_ZIP, DEVICE_GEO_LAT, and
#'   DEVICE_GEO_LONG columns
#' @param sentinels Character vector of sentinel ZIP codes to treat as missing
#'   (default: c("00000", "99999", "11111", "12345", "-999", "-99"))
#' @param zip_code_db Optional pre-loaded ZCTA spatial data; if NULL, loads
#'   from zips_oregon_path
#' @param zips_oregon_path Path to Oregon ZCTA parquet file
#'   (default: here("data", "zips_zcta_oregon.parquet"))
#' @param output_col_name Name for the cleaned output column
#'   (default: "DEVICE_GEO_ZIP_clean")
#' @param verbose Logical; if TRUE, prints ZIP code recovery report
#'   (default: TRUE)
#' @return Data frame with new DEVICE_GEO_ZIP_clean column containing valid
#'   5-digit ZIP codes or NA
#' @examples
#' df_clean <- clean_zip_column(bids_df)
clean_zip_column <- function(df,
                             sentinels = c("00000", "99999", "11111", "12345", "-999", "-99"),
                             zip_code_db = NULL,
                             zips_oregon_path = here("data", "zips_zcta_oregon.parquet"),
                             output_col_name = "DEVICE_GEO_ZIP_clean",
                             verbose = TRUE) {
  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Cleaning DEVICE_GEO_ZIP column"), "\n")
    cat(strrep("=", 60), "\n")
    cat(glue("Current values in DEVICE_GEO_ZIP:\n"))
    # print(head(table(df$DEVICE_GEO_ZIP, useNA = "always")))
  }

  if (is.null(zip_code_db)) {
    zips <- load_oregon_zips(zips_oregon_path)
  } else {
    zips <- zip_code_db
  }

  # converting to string
  df <- df %>%
    mutate(
      DEVICE_GEO_ZIP = as.character(.data$DEVICE_GEO_ZIP)
    )

  # Convert bids → sf object using lat/long
  df_sf <- st_as_sf(
    df,
    coords = c("DEVICE_GEO_LONG", "DEVICE_GEO_LAT"),
    crs = 4326   # WGS84
  )

  # Ensure ZCTAs use the same CRS as df_sf
  zips <- st_transform(zips, st_crs(df_sf))

  # map each point to the ZCTA polygon it falls in
  joined <- st_join(
    df_sf,
    zips[, c("ZCTA5CE20")],
    join = st_within           # strict point-in-polygon
  )

  # Use ZCTA to fill missing ZIP codes
  df$zipcode <- ifelse(
    is.na(df$DEVICE_GEO_ZIP),   # if original ZIP is missing
    joined$ZCTA5CE20,             # use mapped ZIP
    df$DEVICE_GEO_ZIP           # else keep original
  )

  # Create DEVICE_GEO_ZIP_clean

  # Define pattern for valid U.S. 5-digit ZIP codes
  valid_zip_pattern <- "^[0-9]{5}$"

  df <- df %>%
    mutate(
      # Work with character format
      zipcode = as.character(.data$zipcode),

      # Trim whitespace
      zipcode_trim = str_trim(.data$zipcode),

      # Replace sentinel codes with NA
      zipcode_no_sentinel = ifelse(.data$zipcode_trim %in% sentinels,
                                   NA,
                                   .data$zipcode_trim),

      # Enforce valid ZIP pattern (5 numeric digits)
      DEVICE_GEO_ZIP_clean = ifelse(
        str_detect(.data$zipcode_no_sentinel, valid_zip_pattern),
        .data$zipcode_no_sentinel,   # keep valid ZIP
        NA                     # invalid → NA
      )
    )

  # drop unrequired columns
  df <- df %>%
    select(-starts_with("zipcode"))

  if (verbose) {
    # --- Report original ZIP quality ---
    original_na <- sum(is.na(df$DEVICE_GEO_ZIP))
    original_sentinels <- sum(df$DEVICE_GEO_ZIP %in% sentinels, na.rm = TRUE)
    original_bad <- original_na + original_sentinels

    cat("\n")
    cat(strrep("-", 50), "\n")
    cat("ZIP CODE RECOVERY REPORT\n")
    cat(strrep("-", 50), "\n")
    cat(sprintf("  Original missing (NA): %d\n", original_na))
    cat(sprintf("  Original sentinels:    %d\n", original_sentinels))
    cat(sprintf("  Total bad ZIPs:        %d\n", original_bad))

    # Report spatial join success
    spatial_matches <- sum(!is.na(joined$ZCTA5CE20))
    cat(sprintf("  Spatial join matches:  %d (points matched to ZCTA polygons)\n", spatial_matches))

    # --- Final ZIP quality report ---
    final_na <- sum(is.na(df$DEVICE_GEO_ZIP_clean))
    recovered <- original_bad - final_na

    cat(sprintf("  Recovered ZIPs:        %d\n", recovered))
    cat(sprintf("  Remaining NA:          %d\n", final_na))
    cat(sprintf("  Recovery rate:         %.1f%%\n", recovered / original_bad * 100))
    cat(strrep("-", 50), "\n\n")
  }

  df
}


#' Clean and recover missing city names using ZIP code lookup
#'
#' Creates a cleaned city column by copying existing DEVICE_GEO_CITY values
#' and filling missing entries using the major_city field from a ZIP code
#' database lookup on DEVICE_GEO_ZIP_clean.
#'
#' @param df A data frame containing DEVICE_GEO_CITY and DEVICE_GEO_ZIP_clean
#'   columns (run clean_zip_column first)
#' @param zip_code_db Optional pre-loaded ZIP code database with zipcode and
#'   major_city columns; if NULL, loads from oregon_zip_path
#' @param oregon_zip_path Path to Oregon ZIP code database parquet file
#'   (default: here("data", "oregon_zip_code_db.parquet"))
#' @param output_col_name Name for the cleaned output column
#'   (default: "DEVICE_GEO_CITY_clean")
#' @param verbose Logical; if TRUE, prints city recovery report and lists
#'   unmatched ZIPs (default: TRUE)
#' @return Data frame with new DEVICE_GEO_CITY_clean column added
#' @note Requires clean_zip_column to be run first to create DEVICE_GEO_ZIP_clean
#' @examples
#' df_clean <- df %>%
#'   clean_zip_column() %>%
#'   clean_city_column()
clean_city_column <- function(df,
                              zip_code_db = NULL,
                              oregon_zip_path = here("data", "oregon_zip_code_db.parquet"),
                              output_col_name = "DEVICE_GEO_CITY_clean",
                              verbose = TRUE) {

  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Cleaning DEVICE_GEO_CITY column"), "\n")
    cat(strrep("=", 60), "\n")
  }

  # Get ZIP → city lookup from zipcodeR
  if (is.null(zip_code_db)) {
    zip_code_db <- load_oregon_zips(oregon_zip_path)
  } else {
    zip_code_db <- zip_code_db
  }

  zip_db <- zipcodeR::zip_code_db %>%
    select("zipcode", "major_city")

  # Join and fill missing cities where ZIP is available
  df <- df %>%
    mutate(DEVICE_GEO_CITY_clean = .data$DEVICE_GEO_CITY) %>%
    left_join(zip_db, by = c("DEVICE_GEO_ZIP_clean" = "zipcode")) %>%
    mutate(
      DEVICE_GEO_CITY_clean = ifelse(
        is.na(.data$DEVICE_GEO_CITY_clean) & !is.na(.data$major_city),
        .data$major_city,
        .data$DEVICE_GEO_CITY_clean
      )
    ) %>%
    select(-"major_city")


  if (verbose) {
    city_missing_before <- sum(is.na(df$DEVICE_GEO_CITY))
    city_missing_after <- sum(is.na(df$DEVICE_GEO_CITY_clean))
    city_recovered <- city_missing_before - city_missing_after

    cat("\n")
    cat(strrep("-", 50), "\n")
    cat("CITY RECOVERY REPORT\n")
    cat(strrep("-", 50), "\n")
    cat(sprintf("  Original missing:  %d\n", city_missing_before))
    cat(sprintf("  Recovered via ZIP: %d\n", city_recovered))
    cat(sprintf("  Remaining NA:      %d\n", city_missing_after))
    cat(strrep("-", 50), "\n")

    # Check what ZIPs aren't matching (for debugging)
    if (city_missing_after > 0) {
      unmatched_zips <- df %>%
        filter(!is.na(.data$DEVICE_GEO_ZIP_clean) & is.na(.data$  DEVICE_GEO_CITY_clean)) %>%
        count(.data$DEVICE_GEO_ZIP_clean, sort = TRUE)
      cat(sprintf("  Unmatched ZIPs: %d unique values\n", nrow(unmatched_zips)))
      cat("  Top unmatched ZIPs:\n")
      print(head(unmatched_zips, 10))
    }
  }

  df
}


#' Clean and validate geographic coordinates
#'
#' Validates latitude and longitude values against specified bounds (defaulting
#' to Oregon's geographic extent). Creates cleaned columns where out-of-bounds
#' coordinates are set to NA.
#'
#' @param df A data frame containing DEVICE_GEO_LAT and DEVICE_GEO_LONG columns
#' @param lat_min Minimum valid latitude (default: 42, Oregon's southern border)
#' @param lat_max Maximum valid latitude (default: 46.5, Oregon's northern border)
#' @param long_min Minimum valid longitude (default: -125, Oregon's western coast)
#' @param long_max Maximum valid longitude (default: -116, Oregon's eastern border)
#' @param verbose Logical; if TRUE, prints coordinate summary and count of
#'   implausible values (default: TRUE)
#' @return Data frame with DEVICE_GEO_LAT_clean and DEVICE_GEO_LONG_clean
#'   columns added, containing valid coordinates or NA
#' @examples
#' df_clean <- clean_geo_coordinates_column(bids_df)
#' df_clean <- clean_geo_coordinates_column(bids_df, lat_min = 40, lat_max = 50)
clean_geo_coordinates_column <- function(df,
                                         lat_min = 42,
                                         lat_max = 46.5,
                                         long_min = -125,
                                         long_max = -116,
                                         verbose = TRUE) {
  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Cleaning DEVICE_GEO_LAT and DEVICE_GEO_LONG columns"), "\n")
    cat(strrep("=", 60), "\n")
  }

  df <- df %>%
    mutate(
      DEVICE_GEO_LAT_clean = ifelse(
        .data$DEVICE_GEO_LAT >= lat_min & .data$DEVICE_GEO_LAT <= lat_max,
        .data$DEVICE_GEO_LAT,
        NA
      ),

      DEVICE_GEO_LONG_clean = ifelse(
        .data$DEVICE_GEO_LONG >= long_min & .data$DEVICE_GEO_LONG <= long_max,
        .data$DEVICE_GEO_LONG,
        NA
      )
    )

  if (verbose) {
    coord_summary <- df %>%
      summarise(
        min_lat  = min(.data$DEVICE_GEO_LAT,  na.rm = TRUE),
        max_lat  = max(.data$DEVICE_GEO_LAT,  na.rm = TRUE),
        min_long = min(.data$DEVICE_GEO_LONG, na.rm = TRUE),
        max_long = max(.data$DEVICE_GEO_LONG, na.rm = TRUE)
      )

    #
    coord_summary %>%
      mutate(
        lat_within_oregon =
          .data$min_lat >= 42 & .data$max_lat <= 46.5,

        long_within_oregon =
          .data$min_long >= -125 & .data$max_long <= -116
      )
    #
    if (coord_summary$min_lat >= 42 && coord_summary$max_lat <= 46.5) {
      cat("Latitudes are consistent with Oregon.\n")
    } else {
      cat("Latitudes include locations outside Oregon.\n")
    }

    if (coord_summary$min_long >= -125 && coord_summary$max_long <= -116) {
      cat("Longitudes are consistent with Oregon.\n")
    } else {
      cat("Longitudes include locations outside Oregon.\n")
    }

    bids_implausible_coords <- df %>%
      filter(
        .data$DEVICE_GEO_LAT  < lat_min   | .data$DEVICE_GEO_LAT  > lat_max |
        .data$DEVICE_GEO_LONG < long_min | .data$DEVICE_GEO_LONG > long_max
      )

    cat(glue("Number of implausible coordinates: {bids_implausible_coords %>% nrow()}"), "\n")
  }

  df
}

clean_bids_won_column <- function(df, verbose = TRUE) {
  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Cleaning BID_WON column"), "\n")
    cat(strrep("=", 60), "\n")
  }

  df <- df %>%
    mutate(
      BID_WON_clean = case_when(
        tolower(.data$BID_WON) == "true" ~ "TRUE",
        tolower(.data$BID_WON) == "false" ~ "FALSE",
        .data$BID_WON == "TRUE" ~ "TRUE",
        .data$BID_WON == "FALSE" ~ "FALSE",
        TRUE ~ NA_character_
      )
    )

  if (verbose) {
    cat("Current values in BID_WON:\n")
    print(table(df$BID_WON, useNA = "always"))
    cat("\n")
    cat(glue("Current values in BID_WON_clean:\n"))
    print(table(df$BID_WON_clean, useNA = "always"))
  }

  df
}

#' Clean and convert timestamp column to POSIXct
#'
#' Combines date from DATE_UTC column with time from TIMESTAMP column to create
#' a clean POSIXct timestamp. This handles inconsistent date formats in the
#' original TIMESTAMP column by using the reliably formatted DATE_UTC.
#'
#' @param df A data frame containing TIMESTAMP and DATE_UTC columns
#' @param col_name Name of column (not used directly, kept for API consistency)
#' @param format Datetime format string (default: "%Y-%m-%d %H:%M:%S")
#' @param tz Timezone (default: "UTC")
#' @param verbose Whether to print messages (default: TRUE)
#' @param date_col Name of the date column to use (default: "DATE_UTC")
#' @param timestamp_col Name of the timestamp column (default: "TIMESTAMP")
#' @param output_col Name for the output column (default: "TIMESTAMP_clean")
#' @return Data frame with TIMESTAMP_clean column added as POSIXct
clean_timestamp_column <- function(df, col_name,
                                   format = "%Y-%m-%d %H:%M:%S",
                                   tz = "UTC",
                                   verbose = TRUE,
                                   date_col = "DATE_UTC",
                                   timestamp_col = "TIMESTAMP",
                                   output_col = "TIMESTAMP_clean") {

  df <- df %>%
    separate(
      "TIMESTAMP",
      into = c("TIMESTAMP_date", "TIMESTAMP_time"),
      sep = " ",
      remove = FALSE
    ) %>%
    mutate(
      "{output_col}" := paste(.data[[date_col]], .data$TIMESTAMP_time)
    )

  convert_column(df, col_name = output_col, output_col_name = output_col, target_type = "POSIXct",
                 format = format,
                 tz = tz,
                 verbose = verbose,
                 date_col = date_col,
                 timestamp_col = timestamp_col)
}


#' Clean and convert column to Date
#'
#' Converts a character column to Date format.
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param format Date format string (default: "%Y-%m-%d")
#' @param output_col_name Name for the output column (default: "DATE_UTC_clean")
#' @param verbose Whether to print messages (default: TRUE)
#' @return Data frame with converted Date column added
clean_date_column <- function(df, col_name,
                              format = "%Y-%m-%d",
                              output_col_name = "DATE_UTC_clean",
                              verbose = TRUE) {
  convert_column(df, col_name, "Date",
                 format = format,
                 output_col_name = output_col_name,
                 verbose = verbose)
}

#' Clean and convert DEVICE_TYPE column to character
#'
#' Converts DEVICE_TYPE column to character format for consistent handling.
#'
#' @param df A data frame containing the device type column
#' @param col_name Name of column to convert
#' @param output_col_name Name for the output column (default: "DEVICE_TYPE_clean")
#' @param verbose Whether to print messages (default: TRUE)
#' @return Data frame with DEVICE_TYPE_clean column added as character
clean_device_type_column <- function(df, col_name,
                                     output_col_name = "DEVICE_TYPE_clean",
                                     verbose = TRUE) {
  convert_column(df, col_name, "character",
                 output_col_name = output_col_name,
                 verbose = verbose)
}


#' Clean and convert RESPONSE_TIME column to integer
#'
#' Converts RESPONSE_TIME to integer, optionally extracting only numeric
#' digits from string values before conversion.
#'
#' @param df A data frame containing the response time column
#' @param col_name Name of column to convert
#' @param extract_digits Whether to extract only digits before converting
#'   (default: TRUE)
#' @param output_col_name Name for the output column (default: "RESPONSE_TIME_clean")
#' @param verbose Whether to print messages (default: TRUE)
#' @return Data frame with RESPONSE_TIME_clean column added as integer
clean_response_time_column <- function(df, col_name,
                                       extract_digits = TRUE,
                                       output_col_name = "RESPONSE_TIME_clean",
                                       verbose = TRUE) {
  preprocess <- if (extract_digits) {
    function(x) {
      if (verbose) cat("Extracting digits from string...", "\n")
      gsub("[^0-9]", "", x)
    }
  } else {
    NULL
  }

  convert_column(df, col_name, "integer",
                 preprocess_fn = preprocess,
                 output_col_name = output_col_name,
                 verbose = verbose)
}


#' Clean and parse REQUESTED_SIZES column from JSON to list
#'
#' Parses JSON-formatted strings in REQUESTED_SIZES column into R lists.
#'
#' @param df A data frame containing the requested sizes column
#' @param col_name Name of column to convert
#' @param output_col_name Name for the output column (default: "REQUESTED_SIZES_clean")
#' @param parser Function to parse each element (default: jsonlite::fromJSON)
#' @param verbose Whether to print messages (default: TRUE)
#' @return Data frame with REQUESTED_SIZES_clean column added as list
clean_requested_sizes_column <- function(df, col_name,
                                         output_col_name = "REQUESTED_SIZES_clean",
                                         parser = jsonlite::fromJSON,
                                         verbose = TRUE) {

  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Converting {col_name} to list"), "\n")
    cat(strrep("=", 60), "\n")
    cat("Parsing JSON elements...", "\n")
  }

  preprocess <- function(x) {
    lapply(x, parser)
  }

  if (is.list(df[[output_col_name]])) {
    if (verbose) {
      cat(glue("Column {col_name} is already a list. No conversion needed."), "\n\n")
    }
    return(df)
  }

  df[[output_col_name]] <- preprocess(df[[col_name]])

  if (verbose) {
    cat(glue("{col_name} is now: list"), "\n\n")
  }

  df
}


# ==============================================================================
# DATA CLEANING FUNCTIONS
# ==============================================================================

#' Remove rows with missing (NA) values
#'
#' @param df A data frame
#' @param verbose Whether to print removal summary
#' @return Data frame with complete cases only
remove_na_rows <- function(df, verbose = TRUE) {
  original_rows <- nrow(df)
  clean_df <- df[complete.cases(df), ]
  removed_rows <- original_rows - nrow(clean_df)

  if (verbose) {
    cat(glue("Removed {removed_rows} rows with NA values."), "\n")
    cat(glue("Remaining rows: {nrow(clean_df)}"), "\n")
  }

  clean_df
}


#' Remove duplicate rows
#'
#' @param df A data frame
#' @param exclude_cols Column names to exclude from duplication check
#' @param verbose Whether to print removal summary
#' @return Data frame with duplicates removed
remove_duplicates <- function(df, exclude_cols = NULL, verbose = TRUE) {
  original_rows <- nrow(df)

  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Removing duplicate rows"), "\n")
    cat(strrep("=", 60), "\n")
  }

  if (!is.null(exclude_cols)) {
    check_cols <- setdiff(names(df), exclude_cols)
    duplicated_mask <- duplicated(df[, check_cols])
  } else {
    duplicated_mask <- duplicated(df)
  }

  clean_df <- df[!duplicated_mask, ]
  removed_rows <- original_rows - nrow(clean_df)

  if (verbose) {
    cat(glue("Removed {removed_rows} duplicate rows."), "\n")
    cat(glue("Remaining rows: {nrow(clean_df)}"), "\n")
  }

  list(
    df = clean_df,
    removed_indices = removed_rows
  )
}
