# library(logger)
# library(glue)
# library(dplyr)
# library(tidyverse)
# library(lubridate)
# library(tictoc)
# log_appender(appender_tee("logs/data_cleaning.log"))

# #' Check if expected columns exist in a data frame
# #'
# #' @param df A data frame to check
# #' @param expected_columns Character vector of expected column names
# #' @return Character vector of missing column names (invisible)
# #' @examples
# #' check_columns(df, c("id", "name", "age"))
# check_columns <- function(df, expected_columns) {
#   # Check expected_columns is list or character vector
#   if (!is.list(expected_columns) && !is.character(expected_columns)) {
#     msg <- "expected_columns must be a list or character vector. STOPPING"
#     log_debug(msg)
#     stop(msg)
#   }

#   # Always convert to character - works whether it's list or character
#   expected_columns <- as.character(expected_columns)
#   missing_columns <- setdiff(expected_columns, colnames(df))

#   # report if columns are missing. If all columns are missing, stop.
#   if (length(missing_columns) == length(expected_columns)) {
#     msg <- "All expected columns are missing from the dataframe. STOPPING"
#     log_debug(msg)
#     stop(msg)
#   } else if (length(missing_columns) > 0) {
#     print(glue("The following columns are missing from the dataframe:
#       \n {paste(missing_columns, collapse = '\n  \t')}"))
#   }

#   invisible(missing_columns)
# }

# #' Check column types against expected types
# #'
# #' @param df A data frame to check
# #' @param expected_types A data frame with columns: 'column' (character vector
# #'                       of column names), 'Expected_Type' (character vector of
# #'                       R data types), and optionally 'Notes_Actual_Type'
# #' @return A data frame with columns: column, actual, Expected_Type, match,
# #'         Notes_Actual_Type
# #' @examples
# #' expected <- data.frame(
# #'   column = c("age", "name", "active"),
# #'   Expected_Type = c("numeric", "character", "logical")
# #' )
# #' result <- check_column_types(my_df, expected)
# check_column_types <- function(df, expected_types) {
#   #Most of this code is hard coded and specific to the expected_types format.
#   #Will look to make more flexible later.

#   # Validate expected_types structure
#   if (is.null(expected_types$column) || length(expected_types$column) == 0) {
#     stop("expected_types must be a named vector/list with column names as names and types as values.\n",
#          "Example: c(age = 'numeric', name = 'character')")
#   }

#   if (!all(sapply(expected_types$Expected_Type, is.character))) {
#     stop("All values in expected_types must be character strings representing R types.")
#   }

#   # Get actual column types from the dataframe
#   df_types <- sapply(df, function(col) class(col)[1])

#   # Find common columns between df and expected_types
#   common_cols <- intersect(names(df), expected_types$column)

#   # Create comparison data frame for common columns only and add actual column types
#   type_comparison <- expected_types %>%
#     filter(.data$column %in% common_cols) %>%
#     mutate(
#       actual = df_types[.data$column],
#       .before = "Expected_Type"
#     ) %>%
#     mutate(
#       match = .data$actual == .data$Expected_Type,
#       .after = "Expected_Type"
#     )

#   type_comparison
# }

# #' Check for NA values by column
# #'
# #' @param df A data frame to check
# #' @return Named integer vector of NA counts per column
# check_na_by_column <- function(df) {
#   colSums(is.na(df))
# }

# #' Get row indices containing NA values
# #'
# #' @param df A data frame to check
# #' @return Integer vector of row indices with NA values
# get_na_row_indices <- function(df) {
#   # complete.cases returns true if all rows are complete, false if any row is incomplete (i.e. has NA)
#   # Exclude list columns (complete.cases doesn't support them)
#   non_list_cols <- !sapply(df, is.list)
#   df_to_check <- df[, non_list_cols, drop = FALSE]
#   which(!complete.cases(df_to_check))
# }

# #' Comprehensive NA check
# #'
# #' @param df A data frame to check
# #' @return List with na_by_column and na_row_indices
# na_check <- function(df) {
#   # Can uncomment code below to time functions
#   # tic("NA Check")
#   # col_na_counts <- col_na_check(df)
#   # toc()
#   # tic("NA Row Indices")
#   # na_row_indices <- get_na_row_indices(df)
#   # toc()
#   list(
#     na_by_column = check_na_by_column(df),
#     na_row_indices = get_na_row_indices(df)
#   )
# }

# #' Check for blank/empty strings by column
# #'
# #' @param df A data frame to check
# #' @return Named integer vector of blank counts per column
# check_blanks_by_column <- function(df) {
#   # Could use tidyverse approach or sapply to all cols. Current way is faster since it only checks character cols
#   # blank_counts <- df %>% summarise(across(everything(), ~ sum(. == "", na.rm = TRUE)))
#   # blank_counts <- sapply(df, function(col) sum(col == "", na.rm = TRUE))

#   blank_counts <- sapply(df, function(col) {
#     if (is.character(col)) {
#       sum(col == "", na.rm = TRUE)
#     } else {
#       0
#     }
#   })

#   blank_counts
# }

# #' Get row indices containing blank values
# #'
# #' @param df A data frame to check
# #' @return Integer vector of row indices with blank values
# get_blank_row_indices <- function(df) {
#   df %>%
#     mutate(row_id = row_number()) %>%
#     filter(if_any(where(is.character), ~ . == "")) %>%
#     pull(.data$row_id)
# }

# #' Comprehensive blank check
# #'
# #' @param df A data frame to check
# #' @return List with blanks_by_column and blank_row_indices
# blank_check <- function(df) {
#   # Can uncomment code below to time functions
#   # tic("Blank counts")
#   # blank_counts <- col_blank_check(df)
#   # toc()
#   # tic("Blank row indices")
#   # blank_row_indices <- get_blank_row_indices(df)
#   # toc()

#   list(
#     blanks_by_column = check_blanks_by_column(df),
#     blank_row_indices = get_blank_row_indices(df)
#   )
# }

# #' Find duplicate rows
# #'
# #' @param df A data frame to check
# #' @return List with duplicate_indices and num_duplicates
# find_duplicates <- function(df) {
#   check_cols <- setdiff(names(df), "row_id")
#   duplicate_indices <- which(duplicated(df[, check_cols]))
#   list(
#     duplicate_indices = duplicate_indices,
#     num_duplicates = length(duplicate_indices)
#   )
# }

# convert_price_to_numeric <- function(df) {
#   # find rows where PRICE is not a number as a string and not NA

#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat("PRICE Conversion\n")
#   cat(strrep("=", 60), "\n")

#   if (is.numeric(df$PRICE)) {
#     cat("The PRICE column is already a numeric column. No conversion needed.\n")
#     return(df)
#   }

#   problem_rows <- which(is.na(suppressWarnings(as.numeric(df$PRICE))) & !is.na(df$PRICE))

#   cat("There are", length(problem_rows),
#       "row(s) in the PRICE column that is (are) not a number as a string and not NA.\n")
#   cat("the(se) row(s) are:\n", problem_rows, "\n\n")

#   # handle row in PRICE that starts with O instead of 0
#   # gsub(pattern, replacement, where to search)
#   if (length(problem_rows) > 0) {
#     df$PRICE[problem_rows] <- gsub("^O", "0", df$PRICE[problem_rows])
#   }

#   # convert the PRICE column to numeric
#   cat("Converting the PRICE column to numeric...\n")
#   df$PRICE <- as.numeric(df$PRICE)

#   # print type of PRICE column
#   cat("df$PRICE is now:", class(df$PRICE), "\n\n")

#   df
# }

# convert_timestamp_to_posixct <- function(df) {
#   # convert the TIMESTAMP column to POSIXct

#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat("Converting TIMESTAMP from character to TIMESTAMP_NTZ (POSIXct)\n")
#   cat(strrep("=", 60), "\n")

#   if (is.POSIXct(df$TIMESTAMP)) {
#     cat("The TIMESTAMP column is already a POSIXct column. No conversion needed.\n")
#     return(df)
#   }

#   # convert the TIMESTAMP column to POSIXct
#   cat("Converting the TIMESTAMP column to POSIXct...\n")
#   df$TIMESTAMP <- as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

#   # print type of TIMESTAMP column
#   cat("df$TIMESTAMP is now:", class(df$TIMESTAMP), "\n\n")

#   df
# }

# convert_date_to_date <- function(df) {
#   # convert the TIMESTAMP column to POSIXct

#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat("Converting DATE_UTC from character to Date (YYYY-MM-DD)\n")
#   cat(strrep("=", 60), "\n")

#   if (is.Date(df$DATE_UTC)) {
#     cat("The DATE_UTC column is already a Date column. No conversion needed.\n")
#     return(df)
#   }

#   # convert the DATE_UTC column to Date
#   cat("Converting the TIMESTAMP column to POSIXct...\n")
#   df$DATE_UTC <- as.Date(df$DATE_UTC, format = "%Y-%m-%d")

#   # print type of DATE_UTC column
#   cat("df$DATE_UTC is now:", class(df$DATE_UTC), "\n\n")

#   df
# }

# convert_device_type_to_char <- function(df) {
#   # convert the DEVICE_TYPE column to character
#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat("Converting DEVICE_TYPE from integer to character\n")
#   cat(strrep("=", 60), "\n")

#   if (is.character(df$DEVICE_TYPE)) {
#     cat("The DEVICE_TYPE column is already a character column. No conversion needed.\n")
#     return(df)
#   }

#   # convert the DEVICE_TYPE column to character
#   cat("Converting the DEVICE_TYPE column to character...\n")
#   df$DEVICE_TYPE <- as.character(df$DEVICE_TYPE)

#   # print type of DEVICE_TYPE column
#   cat("df$DEVICE_TYPE is now:", class(df$DEVICE_TYPE), "\n\n")

#   df
# }

# convert_requested_size_to_list <- function(df) {
#   # convert the TIMESTAMP column to POSIXct

#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat("Converting REQUESTED_SIZES from character to list\n")
#   cat(strrep("=", 60), "\n")

#   if (is.list(df$REQUESTED_SIZES)) {
#     cat("The REQUESTED_SIZES column is already a list column. No conversion needed.\n")
#     return(df)
#   }

#   # convert the REQUESTED_SIZES column to list
#   cat("Converting the TIMESTAMP column to POSIXct...\n")
#   df$REQUESTED_SIZES <- lapply(df$REQUESTED_SIZES, fromJSON)

#   # print type of DATE_UTC column
#   cat("df$REQUESTED_SIZES is now:", class(df$REQUESTED_SIZES), "\n\n")

#   df
# }

# convert_response_time_to_int <- function(df) {
#   # convert the TIMESTAMP column to POSIXct

#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat("Converting RESPONSE_TIME from character to Integer\n")
#   cat(strrep("=", 60), "\n")

#   if (is.integer(df$RESPONSE_TIME)) {
#     cat("The RESPONSE_TIME column is already an integer column. No conversion needed.\n")
#     return(df)
#   }

#   # gsub() = global substitution.
#   # It finds all matches of pattern in x and replaces them with replacement.
#   # [0-9] means “a digit from 0 to 9”
#   # ^ inside brackets means NOT
#   # [^0-9]  means: "anything that is NOT a digit"
#   # "" means: "replace with nothing"
#   print("extracting digits from RESPONSE_TIME, stripping string")
#   df$RESPONSE_TIME <- gsub("[^0-9]", "", df$RESPONSE_TIME)

#   # convert the RESPONSE_TIME column to integer
#   cat("Converting the TIMESTAMP column to POSIXct...\n")
#   df$RESPONSE_TIME <- as.integer(df$RESPONSE_TIME)

#   # print type of RESPONSE_TIME column
#   cat("df$RESPONSE_TIME is now:", class(df$RESPONSE_TIME), "\n\n")

#   df
# }

# convert_bid_won_to_logical <- function(df) {
#   # convert the TIMESTAMP column to POSIXct

#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat("Converting BID_WON from character to Logical\n")
#   cat(strrep("=", 60), "\n")

#   if (is.logical(df$BID_WON)) {
#     cat("The BID_WON column is already a logical column. No conversion needed.\n")
#     return(df)
#   }

#   # convert the BID_WON column to logical
#   cat("Converting the BID_WON column to logical...\n")
#   df$BID_WON <- as.logical(df$BID_WON)

#   # print type of BID_WON column
#   cat("df$BID_WON is now:", class(df$BID_WON), "\n\n")

#   df
# }


# #' Get type conversion function for a given type name
# #'
# #' @param target_type A character string specifying the target type
# #'                    ("character", "numeric", "integer", "logical", "Date", "POSIXct")
# #' @return A function that converts values to the target type
# #' @examples
# #' converter <- get_type_converter("numeric")
# #' result <- converter("123")  # Returns 123
# get_type_func <- function(target_type) {
#   converters <- list(
#     character = as.character,
#     numeric = as.numeric,
#     integer = as.integer,
#     logical = as.logical,
#     Date = as.Date,
#     POSIXct = function(x) as.POSIXct(x, tz = "UTC")
#   )

#   if (!target_type %in% names(converters)) {
#     stop(paste("Unknown type:", target_type,
#                "\nSupported types:", paste(names(converters), collapse = ", ")))
#   }

#   converters[[target_type]]
# }



# #' Convert a column to a target type
# #'
# #' @param df A data frame
# #' @param col_name A character string specifying the column name
# #' @param target_type A character string specifying the target type
# #' @return The data frame with the column converted
# #' @examples
# #' df <- type_conversion(df, "PRICE", "numeric")
# type_conversion <- function(df, col_name, target_type) {
#   converter <- type_conversion(target_type)
#   df[[col_name]] <- converter(df[[col_name]])
#   df
# }


# # col_blank_count <- bids %>% summarise(across(everything(), ~ sum(. == "", na.rm = TRUE))) %>%
# #   pivot_longer(everything(), names_to = "column", values_to = "blank count")

#   # # Step 1: Count blanks in each column
#   # summarise(across(everything(), ~ sum(. == "", na.rm = TRUE))) %>%
#   # # Step 2: Convert from wide to long format (easier to work with)
#   # pivot_longer(everything(), names_to = "column", values_to = "blank count")


# #' Check for blank/empty strings in a dataframe
# #'
# #' @param df A data frame to check
# #' @return A data frame with columns that have blank values and their counts
# check_blank_values <- function(df) {
#   blank_counts <- sapply(df, function(col) {
#     if (is.character(col)) {
#       sum(col == "", na.rm = TRUE)
#     } else {
#       0
#     }
#   })

#   cols_with_blanks <- blank_counts[blank_counts > 0]

#   if (length(cols_with_blanks) == 0) {
#     cat("No blank/empty strings found in any column.\n")
#     return(data.frame(column = character(0), blank_count = numeric(0)))
#   }

#   result <- data.frame(
#     column = names(cols_with_blanks),
#     blank_count = unname(cols_with_blanks),
#     stringsAsFactors = FALSE,
#     row.names = NULL
#   )

#   cat("Found", nrow(result), "columns with blank values.\n")
#   return(result)
# }


# #' Remove rows with any missing (NA) values
# #'
# #' @param df A data frame
# #' @return A data frame with complete cases only
# remove_missing_rows <- function(df) {
#   original_rows <- nrow(df)
#   clean_df <- df[complete.cases(df), ]
#   removed_rows <- original_rows - nrow(clean_df)

#   cat("Removed", removed_rows, "rows with missing values.\n")
#   cat("Remaining rows:", nrow(clean_df), "\n")

#   return(clean_df)
# }


# #' Remove duplicate rows from a dataframe
# #'
# #' @param df A data frame
# #' @param exclude_cols Column names to exclude from duplication check
# #' @return A data frame with duplicate rows removed
# remove_duplicates <- function(df, exclude_cols = NULL) {
#   original_rows <- nrow(df)

#   if (!is.null(exclude_cols)) {
#     check_cols <- setdiff(names(df), exclude_cols)
#     duplicated_mask <- duplicated(df[, check_cols])
#   } else {
#     duplicated_mask <- duplicated(df)
#   }

#   clean_df <- df[!duplicated_mask, ]
#   removed_rows <- original_rows - nrow(clean_df)

#   cat("Removed", removed_rows, "duplicate rows.\n")
#   cat("Remaining rows:", nrow(clean_df), "\n")

#   return(clean_df)
# }


# # ==============================================================================
# # Main execution (only runs when script is executed directly, not when sourced)
# # ==============================================================================


# library(tidyverse)
# library(arrow)
# library(dplyr)
# library(here)
# library(jsonlite)

# original_bids <- read_parquet(here("data", "bids_data_vDTR.parquet"))
# bids <- original_bids %>% mutate(row_id = row_number())

# expected_bids_columns <- data.frame(
#   column = c(
#     "TIMESTAMP", "DATE_UTC", "AUCTION_ID", "PUBLISHER_ID",
#     "DEVICE_TYPE", "DEVICE_GEO_COUNTRY", "DEVICE_GEO_REGION",
#     "DEVICE_GEO_CITY", "DEVICE_GEO_ZIP", "DEVICE_GEO_LAT",
#     "DEVICE_GEO_LONG", "REQUESTED_SIZES", "SIZE", "PRICE",
#     "RESPONSE_TIME", "BID_WON"
#   ),
#   Expected_Type = c(
#     "POSIXct", "Date", "character", "character", "character",
#     "character", "character", "character", "character", "numeric",
#     "numeric", "list", "character", "numeric", "integer", "logical"
#   ),
#   Notes_Actual_Type = c(
#     "TIMESTAMP_NTZ", "DATE", "VARCHAR", "VARCHAR", "VARCHAR",
#     "VARCHAR(2)", "VARCHAR(2)", "VARCHAR", "VARCHAR(10)", "FLOAT",
#     "FLOAT", "VARCHAR (or ARRAY)", "VARCHAR", "NUMBER(12,6)",
#     "NUMBER(10,0)", "BOOLEAN"
#   ),
#   stringsAsFactors = FALSE
# )


# # =========================================================================================
# # 1) check if columns are present in bids that are expected (listed in data_dictionary.md)
# # 2) identify columns with NA (count) and rows with NA (indices)
# # 3) identify columns with blank values (count) and rows with blank values (indices)
# # 4) identify duplicate rows
# # 5) convert columns to expected types
# # ========================================================================================

# # check if columns are present in the dataframe that are listed in the expected_bids_columns dataframe
# tic("Missing Columns")
# missing_cols <- check_columns(bids, expected_bids_columns$column)
# toc()
# # check if bids column types match the expected types
# tic("Column Types Check")
# type_check_summary <- check_column_types(bids, expected_bids_columns)
# toc()

# # identify columns with NA (count) and rows with NA (indices)
# tic("NA Check")
# na_check <- na_check(bids)
# toc()
# na_col_count <- na_check$col_na_counts
# na_row_indices <- na_check$na_row_indices

# # identify columns with blank values (count) and rows with blank values (indices)
# tic("Blank Check")
# blank_check <- blank_check(bids)
# toc()
# blank_col_count <- blank_check$blank_counts
# blank_row_indices <- blank_check$blank_row_indices

# # identify duplicate rows
# tic("Duplicate Check")
# duplicate_check <- find_duplicates(bids)
# toc()
# duplicate_indices <- duplicate_check$duplicate_indices
# num_duplicates <- duplicate_check$num_duplicates


# bids <- convert_price_to_numeric(bids)
# bids <- convert_timestamp_to_posixct(bids)
# bids <- convert_date_to_date(bids)
# bids <- convert_device_type_to_char(bids)
# bids <- convert_requested_size_to_list(bids)
# bids <- convert_response_time_to_int(bids)
# bids <- convert_bid_won_to_logical(bids)

# type_check_summary_2 <- type_check(bids, expected_bids_columns)

# all((type_check_summary_2$actual == type_check_summary_2$Expected_Type))

# # get all unique sized in requested sizes
# print("Getting all unique sized in requested sizes")
# sizes <- unique(unlist(bids$REQUESTED_SIZES))
# print(glue("there are , {length(sizes)}, unique REQUESTED_SIZES"))





# ==============================================================================
# Data Cleaning and Validation Functions
# ==============================================================================
# A collection of functions for validating, cleaning, and converting data types
# in data frames, with specific support for bid data processing.

library(logger)
library(glue)
library(dplyr)
library(tidyverse)
library(lubridate)
library(tictoc)
library(here)
library(arrow)
library(jsonlite)

log_appender(appender_tee("logs/data_cleaning.log"))

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

  invisible(missing_columns)
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
  cat("DATA QUALITY REPORT\n")
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

  cat(glue("NA Values: {sum(x$na_check$na_by_column)} total across {sum(x$na_check$na_by_column > 0)} columns\n"))
  cat(glue("Blank Values: {sum(x$blank_check$blanks_by_column)} total across {sum(x$blank_check$blanks_by_column > 0)} columns\n"))
  cat(glue("Duplicate Rows: {x$duplicate_check$num_duplicates}\n"))

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
                           verbose = TRUE,
                           ...) {

  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Converting {col_name} to {target_type} \n"))
    cat(strrep("=", 60), "\n")
  }

  # Get conversion and check functions
  converter <- get_type_func(target_type)
  checker <- get_check_func(target_type)

  # Check if already correct type
  if (checker(df[[col_name]])) {
    if (verbose) {
      cat(glue("Column {col_name} is already {target_type}. No conversion needed.\n\n"))
    }
    return(df)
  }

  # Apply preprocessing if provided
  if (!is.null(preprocess_fn)) {
    if (verbose) cat("Applying preprocessing...\n")
    df[[col_name]] <- preprocess_fn(df[[col_name]])
  }

  # Perform conversion with error handling
  tryCatch({
    if (verbose) cat(glue("Converting {col_name}...\n"))
    df[[col_name]] <- converter(df[[col_name]], ...)

    if (verbose) {
      cat(glue("{col_name} is now: {class(df[[col_name]])[1]}\n\n"))
    }
  }, error = function(e) {
    log_error(glue("Failed to convert {col_name}: {e$message}"))
    stop(glue("Conversion failed for {col_name}: {e$message}"))
  })

  df
}

# ==============================================================================
# CONVERSION WRAPPERS
# ==============================================================================

#' Convert column to numeric with optional preprocessing
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param fix_leading_o Whether to replace leading 'O' with '0'
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_numeric <- function(df, col_name, fix_leading_o = FALSE, verbose = TRUE) {
  preprocess <- if (fix_leading_o) {
    function(x) {
      problem_rows <- which(is.na(suppressWarnings(as.numeric(x))) & !is.na(x))
      if (verbose && length(problem_rows) > 0) {
        cat(glue("Found {length(problem_rows)} non-numeric value(s), attempting to fix...\n"))
      }
      gsub("^O", "0", x)
    }
  } else {
    NULL
  }

  convert_column(df, col_name, "numeric", preprocess_fn = preprocess, verbose = verbose)
}

#' Convert column to POSIXct timestamp
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param format Datetime format string
#' @param tz Timezone
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_posixct <- function(df, col_name,
                               format = "%Y-%m-%d %H:%M:%S",
                               tz = "UTC",
                               verbose = TRUE) {
  convert_column(df, col_name, "POSIXct",
                 format = format, tz = tz, verbose = verbose)
}

#' Convert column to Date
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param format Date format string
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_date <- function(df, col_name,
                            format = "%Y-%m-%d",
                            verbose = TRUE) {
  convert_column(df, col_name, "Date",
                 format = format, verbose = verbose)
}

#' Convert column to character
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_character <- function(df, col_name, verbose = TRUE) {
  convert_column(df, col_name, "character", verbose = verbose)
}

#' Convert column to integer with digit extraction
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param extract_digits Whether to extract only digits before converting
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_integer <- function(df, col_name,
                               extract_digits = FALSE,
                               verbose = TRUE) {
  preprocess <- if (extract_digits) {
    function(x) {
      if (verbose) cat("Extracting digits from string...\n")
      gsub("[^0-9]", "", x)
    }
  } else {
    NULL
  }

  convert_column(df, col_name, "integer",
                 preprocess_fn = preprocess, verbose = verbose)
}

#' Convert column to logical
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_logical <- function(df, col_name, verbose = TRUE) {
  convert_column(df, col_name, "logical", verbose = verbose)
}

#' Convert column to list (for JSON-like data)
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param parser Function to parse each element (default: fromJSON)
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_list <- function(df, col_name,
                            parser = jsonlite::fromJSON,
                            verbose = TRUE) {
  preprocess <- function(x) {
    lapply(x, parser)
  }

  if (is.list(df[[col_name]])) {
    if (verbose) {
      cat(glue("Column {col_name} is already a list. No conversion needed.\n\n"))
    }
    return(df)
  }

  if (verbose) {
    cat("\n", strrep("=", 60), "\n")
    cat(glue("Converting {col_name} to list\n"))
    cat(strrep("=", 60), "\n")
    cat("Parsing JSON elements...\n")
  }

  df[[col_name]] <- preprocess(df[[col_name]])

  if (verbose) {
    cat(glue("{col_name} is now: list\n\n"))
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
    cat(glue("Removed {removed_rows} rows with NA values.\n"))
    cat(glue("Remaining rows: {nrow(clean_df)}\n"))
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

  if (!is.null(exclude_cols)) {
    check_cols <- setdiff(names(df), exclude_cols)
    duplicated_mask <- duplicated(df[, check_cols])
  } else {
    duplicated_mask <- duplicated(df)
  }

  clean_df <- df[!duplicated_mask, ]
  removed_rows <- original_rows - nrow(clean_df)

  if (verbose) {
    cat(glue("Removed {removed_rows} duplicate rows.\n"))
    cat(glue("Remaining rows: {nrow(clean_df)}\n"))
  }

  clean_df
}

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Define expected column types for bids data
expected_bids_columns <- data.frame(readr::read_csv(
  here::here("data", "expected_columns.csv"),
  col_types = "ccc"
))

cat("\n")
cat(strrep("=", 70), "\n")
cat("STARTING BIDS DATA PROCESSING\n")
cat(strrep("=", 70), "\n\n")

# Load data
cat("Loading data...\n")
print(here())
original_bids <- read_parquet(here("data", "bids_data_vDTR.parquet"))
bids <- original_bids %>% mutate(row_id = row_number())
cat(glue("Loaded {nrow(original_bids)} rows and {ncol(original_bids)} columns\n\n"))

# Clean data using pipeline
# bids_clean <- clean_bids_data(original_bids, expected_bids_columns)


cat("\n")
cat(strrep("=", 70), "\n")
cat("BIDS DATA CLEANING PIPELINE\n")
cat(strrep("=", 70), "\n\n")

# check if columns are present in the dataframe that are listed in the expected_bids_columns dataframe
tic("Missing Columns")
missing_cols <- check_columns(bids, expected_bids_columns$column)
toc()
# check if bids column types match the expected types
tic("Column Types Check")
type_check_summary <- check_column_types(bids, expected_bids_columns)
toc()

# identify columns with NA (count) and rows with NA (indices)
tic("NA Check")
na_check <- check_na(bids)
toc()
na_col_count <- na_check$col_na_counts
na_row_indices <- na_check$na_row_indices

# identify columns with blank values (count) and rows with blank values (indices)
tic("Blank Check")
blank_check <- check_blanks(bids)
toc()
blank_col_count <- blank_check$blank_counts
blank_row_indices <- blank_check$blank_row_indices

# identify duplicate rows
tic("Duplicate Check")
duplicate_check <- find_duplicates(bids)
toc()
duplicate_indices <- duplicate_check$duplicate_indices
num_duplicates <- duplicate_check$num_duplicates




# Type conversions
cat("\n")
cat(strrep("=", 70), "\n")
cat("APPLYING TYPE CONVERSIONS\n")
cat(strrep("=", 70), "\n")

bids <- convert_to_numeric(bids, "PRICE", fix_leading_o = TRUE)
bids <- convert_to_posixct(bids, "TIMESTAMP")
bids <- convert_to_date(bids, "DATE_UTC")
bids <- convert_to_character(bids, "DEVICE_TYPE")
bids <- convert_to_list(bids, "REQUESTED_SIZES")
bids <- convert_to_integer(bids, "RESPONSE_TIME", extract_digits = TRUE)
bids <- convert_to_logical(bids, "BID_WON")

# Verify conversions
cat("\n")
cat(strrep("=", 70), "\n")
cat("VERIFYING CONVERSIONS\n")
cat(strrep("=", 70), "\n\n")

type_check_final <- check_column_types(bids, expected_bids_columns)
all_match <- all(type_check_final$match, na.rm = TRUE)

if (all_match) {
  cat("All column types match expected types!\n\n")
} else {
  cat("Some type mismatches remain:\n")
  print(type_check_final %>% filter(!match))
  cat("\n")
}




# ========================================================================================
# Get DATE_UTC column, which is a character initially, and convert to Date
# Get rows where complete.cases is FALSE


test_func <- function(...) {
  print(names(list(...)))
  if ("a" %in% names(list(...))) {
    print("a is in the list")
    print(glue("a = {list(...)[['a']]}"))
  }
}

test_func(a = 1, b = 2, c = 3)


datetime_df <- data.frame(
  DATE_UTC_0 = original_bids$DATE_UTC,
  TIMESTAMP_0 = original_bids$TIMESTAMP
)

datetime_df$DATE <- as.Date(datetime_df$DATE_UTC_0, format = "%Y-%m-%d")

# get TIMESTAMP column, which is a character initially, split each row with delim " "
# Then convert to a dataframe and find all rows where the first column is "NA" <-- string
# Parse the TIMESTAMP column using space delimiter into date and time components
timestamp_split <- strsplit(as.character(datetime_df$TIMESTAMP_0), " ")
timestamp_df <- as.data.frame(do.call(rbind, timestamp_split), stringsAsFactors = FALSE)
colnames(timestamp_df) <- c("TIMESTAMP_date", "TIMESTAMP_time")

# Combine into one df
datetime_df <- cbind(datetime_df, timestamp_df)

DATE_date_NA_indices <- which(is.na(datetime_df$DATE))
TIMESTAMP_date_NA_indices <- which(apply(timestamp_df, 1, function(row) any(row == "NA")))


# Add new column to datetime_dfthat duplicates the TIMESTAMP_date column
# Overwrite TIMESTAMP_date2 with date_col for rows in na_row_indices
datetime_df$TIMESTAMP_date2 <- datetime_df$TIMESTAMP_date
datetime_df$TIMESTAMP_date2[TIMESTAMP_date_NA_indices] <- as.character(datetime_df$DATE[TIMESTAMP_date_NA_indices])

# Check if date_col and TIMESTAMP_date2 are equal (element-wise, after coercion if needed)
are_equal <- all(as.Date(datetime_df$TIMESTAMP_date2) == as.Date(datetime_df$DATE), na.rm = TRUE)
cat("Are date_col and TIMESTAMP_date2 equal?\n")
print(are_equal)
cat(sprintf("Number of mismatches: %d\n", sum(!are_equal, na.rm = TRUE)))

# Create a new TIMESTAMP column that is the concatenation of TIMESTAMP_date2 and TIMESTAMP_time_col
datetime_df$TIMESTAMP_corrected <- paste(datetime_df$TIMESTAMP_date2, datetime_df$TIMESTAMP_time)

# Handle multiple formats in the TIMESTAMP column
format_1 <- "%Y-%m-%d %H:%M:%S"
format_2 <- "%m/%d/%Y %H:%M:%S"

format_1_col <- as.POSIXct(datetime_df$TIMESTAMP_corrected, format = format_1, tz = "UTC")
format_2_col <- as.POSIXct(datetime_df$TIMESTAMP_corrected, format = format_2, tz = "UTC")

not_NA_format_1 <- which(!is.na(format_1_col))
not_NA_format_2 <- which(!is.na(format_2_col))

any_in_both <- any(not_NA_format_1 %in% not_NA_format_2)

combined_indices <- sort(unique(c(not_NA_format_1, not_NA_format_2)))

all_row_indices <- seq_len(nrow(datetime_df))

missing_indices <- setdiff(all_row_indices, combined_indices)

all_corrected_col <- format_1_col
all_corrected_col[not_NA_format_2] <- format_2_col[not_NA_format_2]

datetime_df$TIMESTAMP_posixct <- all_corrected_col

# Check for NA values in TIMESTAMP_posixct column
na_posixct_indices <- which(is.na(datetime_df$TIMESTAMP_posixct))
cat(sprintf("Number of NA values in TIMESTAMP_posixct: %d\n", length(na_posixct_indices)))
if (length(na_posixct_indices) > 0) {
  cat("Indices with NA in TIMESTAMP_posixct:\n")
  print(na_posixct_indices)
}



convert_to_posixct_2 <- function(df, col_name,
                                 formats = c("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"),
                                 tz = "UTC",
                                 verbose = TRUE) {
  convert_column(df, col_name, "POSIXct",
                 format = format, tz = tz, verbose = verbose)
}



#' Convert column to numeric with optional preprocessing
#'
#' @param df A data frame
#' @param col_name Name of column to convert
#' @param fix_leading_o Whether to replace leading 'O' with '0'
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_numeric <- function(df, col_name, fix_leading_o = FALSE, verbose = TRUE) {
  preprocess <- if (fix_leading_o) {
    function(x) {
      problem_rows <- which(is.na(suppressWarnings(as.numeric(x))) & !is.na(x))
      if (verbose && length(problem_rows) > 0) {
        cat(glue("Found {length(problem_rows)} non-numeric value(s), attempting to fix...\n"))
      }
      gsub("^O", "0", x)
    }
  } else {
    NULL
  }

  convert_column(df, col_name, "numeric", preprocess_fn = preprocess, verbose = verbose)
}



datetime_df <- original_bids %>%
  select(DATE_UTC_0 = DATE_UTC, TIMESTAMP_0 = TIMESTAMP) %>%
  mutate(
    DATE = as.Date(DATE_UTC_0, format = "%Y-%m-%d"),
    # Separate TIMESTAMP into date and time
    TIMESTAMP_date = str_extract(TIMESTAMP_0, "^[^ ]+"),
    TIMESTAMP_time = str_extract(TIMESTAMP_0, "[^ ]+$"),
    # Replace "NA" strings with DATE
    TIMESTAMP_date2 = if_else(TIMESTAMP_date == "NA", as.character(DATE), TIMESTAMP_date),
    # Combine corrected date and time
    TIMESTAMP_corrected = paste(TIMESTAMP_date2, TIMESTAMP_time),
    # Parse with multiple formats using lubridate
    TIMESTAMP_posixct = parse_date_time(
      TIMESTAMP_corrected,
      orders = c("Ymd HMS", "mdy HMS"),
      tz = "UTC"
    )
  )


datetime_df <- original_bids %>%
  transmute(
    DATE_UTC_0 = DATE_UTC,
    TIMESTAMP_0 = TIMESTAMP,
    DATE = as.Date(DATE_UTC_0)
  )

# Can do separate from tidyr package or str_split from stringr package.
# str_split is faster but requires more steps
#    TIMESTAMP_parts = str_split(TIMESTAMP_0, " ", n = 2, simplify = TRUE),
#    TIMESTAMP_date = TIMESTAMP_parts[, 1],
#    TIMESTAMP_time = TIMESTAMP_parts[, 2],

datetime_df <- datetime_df %>%
  separate(
    TIMESTAMP_0,
    into = c("TIMESTAMP_date", "TIMESTAMP_time"),
    sep = " ",
    remove = FALSE
  ) %>%
  mutate(
    TIMESTAMP_date_clean = case_when(
      TIMESTAMP_date == "NA" ~ as.character(DATE),
      TRUE ~ TIMESTAMP_date
    )
  ) %>%
  mutate(
    TIMESTAMP_corrected = paste(TIMESTAMP_date_clean, TIMESTAMP_time)
  )

print(head(datetime_df, 11))


x <- paste(datetime_df$TIMESTAMP_date_clean, datetime_df$TIMESTAMP_time)
print(head(x, 11))


datetime_df <- original_bids %>%
  transmute(
    DATE_UTC_0 = DATE_UTC,
    TIMESTAMP_0 = TIMESTAMP,
    DATE = as.Date(DATE_UTC_0),
    # Split timestamp
    TIMESTAMP_parts = str_split(TIMESTAMP_0, " ", n = 2, simplify = TRUE),
    TIMESTAMP_date = TIMESTAMP_parts[, 1],
    TIMESTAMP_time = TIMESTAMP_parts[, 2],
    # Replace NA strings
    TIMESTAMP_date_clean = case_when(
      TIMESTAMP_date == "NA" ~ as.character(DATE),
      TRUE ~ TIMESTAMP_date
    ),
    # Combine
    TIMESTAMP_corrected = paste(TIMESTAMP_date_clean, TIMESTAMP_time),
    # Try both formats, use whichever works
    TIMESTAMP_posixct = coalesce(
      ymd_hms(TIMESTAMP_corrected, quiet = TRUE),
      mdy_hms(TIMESTAMP_corrected, quiet = TRUE)
    )
  )


DATE_parts_DATE <- str_split(datetime_df$DATE_UTC_0, "/", n = 3, simplify = TRUE)
DATE_parts_TIME <- str_split(datetime_df$TIMESTAMP_date, "/", n = 3, simplify = TRUE)
# Find all rows where the second column in DATE_parts is not an empty string
second_col_not_empty_indices_DATE <- which(DATE_parts_DATE[, 2] != "")
second_col_not_empty_indices_TIME <- which(DATE_parts_TIME[, 2] != "")
print(head(second_col_not_empty_indices_DATE, 11))
print(head(second_col_not_empty_indices_TIME, 11))
