library(logger)
library(glue)
library(dplyr)
library(tidyverse)
library(lubridate)
library(tictoc)
log_appender(appender_tee("logs/data_cleaning.log"))

#' Check if expected columns exist in a data frame
#'
#' @param df A data frame to check
#' @param expected_columns Character vector of expected column names
#' @return Character vector of missing column names (invisible)
#' @examples
#' check_columns(df, c("id", "name", "age"))
check_columns <- function(df, expected_columns) {
  # Check expected_columns is list or character vector
  if (!is.list(expected_columns) && !is.character(expected_columns)) {
    msg <- "expected_columns must be a list or character vector. STOPPING"
    log_debug(msg)
    stop(msg)
  }

  # Always convert to character - works whether it's list or character
  expected_columns <- as.character(expected_columns)
  missing_columns <- setdiff(expected_columns, colnames(df))

  # report if columns are missing. If all columns are missing, stop.
  if (length(missing_columns) == length(expected_columns)) {
    msg <- "All expected columns are missing from the dataframe. STOPPING"
    log_debug(msg)
    stop(msg)
  } else if (length(missing_columns) > 0) {
    print(glue("The following columns are missing from the dataframe:
      \n {paste(missing_columns, collapse = '\n  \t')}"))
  }

  invisible(missing_columns)
}

#' Check column types against expected types
#'
#' @param df A data frame to check
#' @param expected_types A data frame with columns: 'column' (character vector
#'                       of column names), 'Expected_Type' (character vector of
#'                       R data types), and optionally 'Notes_Actual_Type'
#' @return A data frame with columns: column, actual, Expected_Type, match,
#'         Notes_Actual_Type
#' @examples
#' expected <- data.frame(
#'   column = c("age", "name", "active"),
#'   Expected_Type = c("numeric", "character", "logical")
#' )
#' result <- check_column_types(my_df, expected)
check_column_types <- function(df, expected_types) {
  #Most of this code is hard coded and specific to the expected_types format.
  #Will look to make more flexible later.

  # Validate expected_types structure
  if (is.null(expected_types$column) || length(expected_types$column) == 0) {
    stop("expected_types must be a named vector/list with column names as names and types as values.\n",
         "Example: c(age = 'numeric', name = 'character')")
  }

  if (!all(sapply(expected_types$Expected_Type, is.character))) {
    stop("All values in expected_types must be character strings representing R types.")
  }

  # Get actual column types from the dataframe
  df_types <- sapply(df, function(col) class(col)[1])

  # Find common columns between df and expected_types
  common_cols <- intersect(names(df), expected_types$column)

  # Create comparison data frame for common columns only and add actual column types
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
  # complete.cases returns true if all rows are complete, false if any row is incomplete (i.e. has NA)
  which(!complete.cases(df))
}

#' Comprehensive NA check
#'
#' @param df A data frame to check
#' @return List with na_by_column and na_row_indices
na_check <- function(df) {
  # Can uncomment code below to time functions
  # tic("NA Check")
  # col_na_counts <- col_na_check(df)
  # toc()
  # tic("NA Row Indices")
  # na_row_indices <- get_na_row_indices(df)
  # toc()
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
  # Could use tidyverse approach or sapply to all cols. Current way is faster since it only checks character cols
  # blank_counts <- df %>% summarise(across(everything(), ~ sum(. == "", na.rm = TRUE)))
  # blank_counts <- sapply(df, function(col) sum(col == "", na.rm = TRUE))

  blank_counts <- sapply(df, function(col) {
    if (is.character(col)) {
      sum(col == "", na.rm = TRUE)
    } else {
      0
    }
  })

  blank_counts
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
blank_check <- function(df) {
  # Can uncomment code below to time functions
  # tic("Blank counts")
  # blank_counts <- col_blank_check(df)
  # toc()
  # tic("Blank row indices")
  # blank_row_indices <- get_blank_row_indices(df)
  # toc()

  list(
    blanks_by_column = check_blanks_by_column(df),
    blank_row_indices = get_blank_row_indices(df)
  )
}

#' Find duplicate rows
#'
#' @param df A data frame to check
#' @return List with duplicate_indices and num_duplicates
find_duplicates <- function(df) {
  check_cols <- setdiff(names(df), "row_id")
  duplicate_indices <- which(duplicated(df[, check_cols]))
  list(
    duplicate_indices = duplicate_indices,
    num_duplicates = length(duplicate_indices)
  )
}

convert_price_to_numeric <- function(df) {
  # find rows where PRICE is not a number as a string and not NA

  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("PRICE Conversion\n")
  cat(strrep("=", 60), "\n")

  if (is.numeric(df$PRICE)) {
    cat("The PRICE column is already a numeric column. No conversion needed.\n")
    return(df)
  }

  problem_rows <- which(is.na(suppressWarnings(as.numeric(df$PRICE))) & !is.na(df$PRICE))

  cat("There are", length(problem_rows),
      "row(s) in the PRICE column that is (are) not a number as a string and not NA.\n")
  cat("the(se) row(s) are:\n", problem_rows, "\n\n")

  # handle row in PRICE that starts with O instead of 0
  # gsub(pattern, replacement, where to search)
  if (length(problem_rows) > 0) {
    df$PRICE[problem_rows] <- gsub("^O", "0", df$PRICE[problem_rows])
  }

  # convert the PRICE column to numeric
  cat("Converting the PRICE column to numeric...\n")
  df$PRICE <- as.numeric(df$PRICE)

  # print type of PRICE column
  cat("df$PRICE is now:", class(df$PRICE), "\n\n")

  df
}

convert_timestamp_to_posixct <- function(df) {
  # convert the TIMESTAMP column to POSIXct

  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("Converting TIMESTAMP from character to TIMESTAMP_NTZ (POSIXct)\n")
  cat(strrep("=", 60), "\n")

  if (is.POSIXct(df$TIMESTAMP)) {
    cat("The TIMESTAMP column is already a POSIXct column. No conversion needed.\n")
    return(df)
  }

  # convert the TIMESTAMP column to POSIXct
  cat("Converting the TIMESTAMP column to POSIXct...\n")
  df$TIMESTAMP <- as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

  # print type of TIMESTAMP column
  cat("df$TIMESTAMP is now:", class(df$TIMESTAMP), "\n\n")

  df
}

convert_date_to_date <- function(df) {
  # convert the TIMESTAMP column to POSIXct

  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("Converting DATE_UTC from character to Date (YYYY-MM-DD)\n")
  cat(strrep("=", 60), "\n")

  if (is.Date(df$DATE_UTC)) {
    cat("The DATE_UTC column is already a Date column. No conversion needed.\n")
    return(df)
  }

  # convert the DATE_UTC column to Date
  cat("Converting the TIMESTAMP column to POSIXct...\n")
  df$DATE_UTC <- as.Date(df$DATE_UTC, format = "%Y-%m-%d")

  # print type of DATE_UTC column
  cat("df$DATE_UTC is now:", class(df$DATE_UTC), "\n\n")

  df
}

convert_device_type_to_char <- function(df) {
  # convert the DEVICE_TYPE column to character
  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("Converting DEVICE_TYPE from integer to character\n")
  cat(strrep("=", 60), "\n")

  if (is.character(df$DEVICE_TYPE)) {
    cat("The DEVICE_TYPE column is already a character column. No conversion needed.\n")
    return(df)
  }

  # convert the DEVICE_TYPE column to character
  cat("Converting the DEVICE_TYPE column to character...\n")
  df$DEVICE_TYPE <- as.character(df$DEVICE_TYPE)

  # print type of DEVICE_TYPE column
  cat("df$DEVICE_TYPE is now:", class(df$DEVICE_TYPE), "\n\n")

  df
}

convert_requested_size_to_list <- function(df) {
  # convert the TIMESTAMP column to POSIXct

  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("Converting REQUESTED_SIZES from character to list\n")
  cat(strrep("=", 60), "\n")

  if (is.list(df$REQUESTED_SIZES)) {
    cat("The REQUESTED_SIZES column is already a list column. No conversion needed.\n")
    return(df)
  }

  # convert the REQUESTED_SIZES column to list
  cat("Converting the TIMESTAMP column to POSIXct...\n")
  df$REQUESTED_SIZES <- lapply(df$REQUESTED_SIZES, fromJSON)

  # print type of DATE_UTC column
  cat("df$REQUESTED_SIZES is now:", class(df$REQUESTED_SIZES), "\n\n")

  df
}

convert_response_time_to_int <- function(df) {
  # convert the TIMESTAMP column to POSIXct

  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("Converting RESPONSE_TIME from character to Integer\n")
  cat(strrep("=", 60), "\n")

  if (is.integer(df$RESPONSE_TIME)) {
    cat("The RESPONSE_TIME column is already an integer column. No conversion needed.\n")
    return(df)
  }

  # gsub() = global substitution.
  # It finds all matches of pattern in x and replaces them with replacement.
  # [0-9] means “a digit from 0 to 9”
  # ^ inside brackets means NOT
  # [^0-9]  means: "anything that is NOT a digit"
  # "" means: "replace with nothing"
  print("extracting digits from RESPONSE_TIME, stripping string")
  df$RESPONSE_TIME <- gsub("[^0-9]", "", df$RESPONSE_TIME)

  # convert the RESPONSE_TIME column to integer
  cat("Converting the TIMESTAMP column to POSIXct...\n")
  df$RESPONSE_TIME <- as.integer(df$RESPONSE_TIME)

  # print type of RESPONSE_TIME column
  cat("df$RESPONSE_TIME is now:", class(df$RESPONSE_TIME), "\n\n")

  df
}

convert_bid_won_to_logical <- function(df) {
  # convert the TIMESTAMP column to POSIXct

  cat("\n")
  cat(strrep("=", 60), "\n")
  cat("Converting BID_WON from character to Logical\n")
  cat(strrep("=", 60), "\n")

  if (is.logical(df$BID_WON)) {
    cat("The BID_WON column is already a logical column. No conversion needed.\n")
    return(df)
  }

  # convert the BID_WON column to logical
  cat("Converting the BID_WON column to logical...\n")
  df$BID_WON <- as.logical(df$BID_WON)

  # print type of BID_WON column
  cat("df$BID_WON is now:", class(df$BID_WON), "\n\n")

  df
}


#' Get type conversion function for a given type name
#'
#' @param target_type A character string specifying the target type
#'                    ("character", "numeric", "integer", "logical", "Date", "POSIXct")
#' @return A function that converts values to the target type
#' @examples
#' converter <- get_type_converter("numeric")
#' result <- converter("123")  # Returns 123
get_type_func <- function(target_type) {
  converters <- list(
    character = as.character,
    numeric = as.numeric,
    integer = as.integer,
    logical = as.logical,
    Date = as.Date,
    POSIXct = function(x) as.POSIXct(x, tz = "UTC")
  )

  if (!target_type %in% names(converters)) {
    stop(paste("Unknown type:", target_type,
               "\nSupported types:", paste(names(converters), collapse = ", ")))
  }

  converters[[target_type]]
}



#' Convert a column to a target type
#'
#' @param df A data frame
#' @param col_name A character string specifying the column name
#' @param target_type A character string specifying the target type
#' @return The data frame with the column converted
#' @examples
#' df <- type_conversion(df, "PRICE", "numeric")
type_conversion <- function(df, col_name, target_type) {
  converter <- type_conversion(target_type)
  df[[col_name]] <- converter(df[[col_name]])
  df
}


# col_blank_count <- bids %>% summarise(across(everything(), ~ sum(. == "", na.rm = TRUE))) %>%
#   pivot_longer(everything(), names_to = "column", values_to = "blank count")

  # # Step 1: Count blanks in each column
  # summarise(across(everything(), ~ sum(. == "", na.rm = TRUE))) %>%
  # # Step 2: Convert from wide to long format (easier to work with)
  # pivot_longer(everything(), names_to = "column", values_to = "blank count")


#' Check for blank/empty strings in a dataframe
#'
#' @param df A data frame to check
#' @return A data frame with columns that have blank values and their counts
check_blank_values <- function(df) {
  blank_counts <- sapply(df, function(col) {
    if (is.character(col)) {
      sum(col == "", na.rm = TRUE)
    } else {
      0
    }
  })

  cols_with_blanks <- blank_counts[blank_counts > 0]

  if (length(cols_with_blanks) == 0) {
    cat("No blank/empty strings found in any column.\n")
    return(data.frame(column = character(0), blank_count = numeric(0)))
  }

  result <- data.frame(
    column = names(cols_with_blanks),
    blank_count = unname(cols_with_blanks),
    stringsAsFactors = FALSE,
    row.names = NULL
  )

  cat("Found", nrow(result), "columns with blank values.\n")
  return(result)
}


#' Remove rows with any missing (NA) values
#'
#' @param df A data frame
#' @return A data frame with complete cases only
remove_missing_rows <- function(df) {
  original_rows <- nrow(df)
  clean_df <- df[complete.cases(df), ]
  removed_rows <- original_rows - nrow(clean_df)

  cat("Removed", removed_rows, "rows with missing values.\n")
  cat("Remaining rows:", nrow(clean_df), "\n")

  return(clean_df)
}


#' Remove duplicate rows from a dataframe
#'
#' @param df A data frame
#' @param exclude_cols Column names to exclude from duplication check
#' @return A data frame with duplicate rows removed
remove_duplicates <- function(df, exclude_cols = NULL) {
  original_rows <- nrow(df)

  if (!is.null(exclude_cols)) {
    check_cols <- setdiff(names(df), exclude_cols)
    duplicated_mask <- duplicated(df[, check_cols])
  } else {
    duplicated_mask <- duplicated(df)
  }

  clean_df <- df[!duplicated_mask, ]
  removed_rows <- original_rows - nrow(clean_df)

  cat("Removed", removed_rows, "duplicate rows.\n")
  cat("Remaining rows:", nrow(clean_df), "\n")

  return(clean_df)
}


# ==============================================================================
# Main execution (only runs when script is executed directly, not when sourced)
# ==============================================================================


library(tidyverse)
library(arrow)
library(dplyr)
library(here)
library(jsonlite)

original_bids <- read_parquet(here("data", "bids_data_vDTR.parquet"))
bids <- original_bids %>% mutate(row_id = row_number())

expected_bids_columns <- data.frame(
  column = c(
    "TIMESTAMP", "DATE_UTC", "AUCTION_ID", "PUBLISHER_ID",
    "DEVICE_TYPE", "DEVICE_GEO_COUNTRY", "DEVICE_GEO_REGION",
    "DEVICE_GEO_CITY", "DEVICE_GEO_ZIP", "DEVICE_GEO_LAT",
    "DEVICE_GEO_LONG", "REQUESTED_SIZES", "SIZE", "PRICE",
    "RESPONSE_TIME", "BID_WON"
  ),
  Expected_Type = c(
    "POSIXct", "Date", "character", "character", "character",
    "character", "character", "character", "character", "numeric",
    "numeric", "list", "character", "numeric", "integer", "logical"
  ),
  Notes_Actual_Type = c(
    "TIMESTAMP_NTZ", "DATE", "VARCHAR", "VARCHAR", "VARCHAR",
    "VARCHAR(2)", "VARCHAR(2)", "VARCHAR", "VARCHAR(10)", "FLOAT",
    "FLOAT", "VARCHAR (or ARRAY)", "VARCHAR", "NUMBER(12,6)",
    "NUMBER(10,0)", "BOOLEAN"
  ),
  stringsAsFactors = FALSE
)


# =========================================================================================
# 1) check if columns are present in bids that are expected (listed in data_dictionary.md)
# 2) identify columns with NA (count) and rows with NA (indices)
# 3) identify columns with blank values (count) and rows with blank values (indices)
# 4) identify duplicate rows
# 5) convert columns to expected types
# ========================================================================================

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
na_check <- na_check(bids)
toc()
na_col_count <- na_check$col_na_counts
na_row_indices <- na_check$na_row_indices

# identify columns with blank values (count) and rows with blank values (indices)
tic("Blank Check")
blank_check <- blank_check(bids)
toc()
blank_col_count <- blank_check$blank_counts
blank_row_indices <- blank_check$blank_row_indices

# identify duplicate rows
tic("Duplicate Check")
duplicate_check <- find_duplicates(bids)
toc()
duplicate_indices <- duplicate_check$duplicate_indices
num_duplicates <- duplicate_check$num_duplicates


bids <- convert_price_to_numeric(bids)
bids <- convert_timestamp_to_posixct(bids)
bids <- convert_date_to_date(bids)
bids <- convert_device_type_to_char(bids)
bids <- convert_requested_size_to_list(bids)
bids <- convert_response_time_to_int(bids)
bids <- convert_bid_won_to_logical(bids)

type_check_summary_2 <- type_check(bids, expected_bids_columns)

all((type_check_summary_2$actual == type_check_summary_2$Expected_Type))

# get all unique sized in requested sizes
print("Getting all unique sized in requested sizes")
sizes <- unique(unlist(bids$REQUESTED_SIZES))
print(glue("there are , {length(sizes)}, unique REQUESTED_SIZES"))








# has_columns(bids, expected_bids_columns$column)

# type_check_summary <- col_type_check(bids, expected_bids_columns)

# bid_na_check <- col_na_check(bids)
# row_na_indices <- get_na_row_indices(bids)


# col_blank_count <- bids %>%
#   # Step 1: Count blanks in each column
#   summarise(across(everything(), ~ sum(. == "", na.rm = TRUE))) %>%
#   # Step 2: Convert from wide to long format (easier to work with)
#   pivot_longer(everything(), names_to = "column", values_to = "blank count")

# col_check <- col_check %>%
#   left_join(col_blank_count, by = "column")


# # Step 3: Keep only columns that have blanks
# col_w_blank <- col_blank_count %>% filter(`blank count` > 0)

# blank_counts <- sapply(bids, function(col) {
#   if (is.character(col)) {
#     sum(col == "", na.rm = TRUE)
#   } else {
#     0
#   }
# })


# print(blank_counts)





# main <- function() {
#   library(tidyverse)
#   library(arrow)
#   library(dplyr)
#   library(here)
#   library(jsonlite)

#   original_bids <- read_parquet(here("data", "bids_data_vDTR.parquet"))
#   bids <- original_bids %>% mutate(row_id = row_number())
#   glimpse(bids)

#   expected_bids_columns <- data.frame(
#     column = c(
#       "TIMESTAMP", "DATE_UTC", "AUCTION_ID", "PUBLISHER_ID",
#       "DEVICE_TYPE", "DEVICE_GEO_COUNTRY", "DEVICE_GEO_REGION",
#       "DEVICE_GEO_CITY", "DEVICE_GEO_ZIP", "DEVICE_GEO_LAT",
#       "DEVICE_GEO_LONG", "REQUESTED_SIZES", "SIZE", "PRICE",
#       "RESPONSE_TIME", "BID_WON"
#     ),
#     Expected_Type = c(
#       "POSIXct", "Date", "character", "character", "character",
#       "character", "character", "character", "character", "numeric",
#       "numeric", "list", "character", "numeric", "integer", "logical"
#     ),
#     Notes_Actual_Type = c(
#       "TIMESTAMP_NTZ", "DATE", "VARCHAR", "VARCHAR", "VARCHAR",
#       "VARCHAR(2)", "VARCHAR(2)", "VARCHAR", "VARCHAR(10)", "FLOAT",
#       "FLOAT", "VARCHAR (or ARRAY)", "VARCHAR", "NUMBER(12,6)",
#       "NUMBER(10,0)", "BOOLEAN"
#     ),
#     stringsAsFactors = FALSE
#   )



# }

# if (interactive()) {
#   cat("Running main function...\n")
#   # main()
# }






# # ============================================================================
# # REFACTORED VERSION - Generic Column Type Converter
# # ============================================================================

# #' Generic column type converter with validation
# #'
# #' @param df Data frame to convert
# #' @param col_name Name of column to convert
# #' @param target_type_name Human-readable type name (e.g., "numeric", "POSIXct")
# #' @param check_fn Function to check if already correct type (e.g., is.numeric)
# #' @param convert_fn Function to perform conversion (e.g., as.numeric)
# #' @param preprocess_fn Optional function to preprocess before conversion
# #' @param ... Additional arguments passed to convert_fn
# #'
# #' @return Modified data frame with converted column
# convert_col_to_type <- function(df,
#                                 col_name,
#                                 target_type_name,
#                                 check_fn,
#                                 convert_fn,
#                                 preprocess_fn = NULL,
#                                 ...) {

#   # Print header
#   cat("\n")
#   cat(strrep("=", 60), "\n")
#   cat(glue("{col_name} Conversion to {target_type_name}\n"))
#   cat(strrep("=", 60), "\n")

#   # Check if already correct type
#   if (check_fn(df[[col_name]])) {
#     cat(glue("The {col_name} column is already {target_type_name}. No conversion needed.\n\n"))
#     return(df)
#   }

#   # Apply preprocessing if provided
#   if (!is.null(preprocess_fn)) {
#     df[[col_name]] <- preprocess_fn(df[[col_name]])
#   }

#   # Perform conversion
#   cat(glue("Converting {col_name} to {target_type_name}...\n"))
#   df[[col_name]] <- convert_fn(df[[col_name]], ...)

#   # Report result
#   cat(glue("{col_name} is now: {class(df[[col_name]])[1]}\n\n"))

#   df
# }

# # ============================================================================
# # USAGE EXAMPLES
# # ============================================================================

# # Convert PRICE to numeric with preprocessing for leading 'O'
# bids <- convert_column_type(
#   df = bids,
#   col_name = "PRICE",
#   target_type_name = "numeric",
#   check_fn = is.numeric,
#   convert_fn = as.numeric,
#   preprocess_fn = function(x) gsub("^O", "0", x)
# )

# # Convert TIMESTAMP to POSIXct
# bids <- convert_column_type(
#   df = bids,
#   col_name = "TIMESTAMP",
#   target_type_name = "POSIXct",
#   check_fn = is.POSIXct,
#   convert_fn = as.POSIXct,
#   format = "%Y-%m-%d %H:%M:%S",
#   tz = "UTC"
# )

# # Convert DATE_UTC to Date
# bids <- convert_column_type(
#   df = bids,
#   col_name = "DATE_UTC",
#   target_type_name = "Date",
#   check_fn = function(x) inherits(x, "Date"),
#   convert_fn = as.Date,
#   format = "%Y-%m-%d"
# )

# # ============================================================================
# # ALTERNATIVE: Even more specific wrapper functions
# # ============================================================================

# convert_to_numeric <- function(df, col_name, fix_leading_o = FALSE) {
#   preprocess <- if (fix_leading_o) {
#     function(x) gsub("^O", "0", x)
#   } else {
#     NULL
#   }

#   convert_column_type(
#     df = df,
#     col_name = col_name,
#     target_type_name = "numeric",
#     check_fn = is.numeric,
#     convert_fn = as.numeric,
#     preprocess_fn = preprocess
#   )
# }

# convert_to_posixct <- function(df, col_name, format = "%Y-%m-%d %H:%M:%S", tz = "UTC") {
#   convert_column_type(
#     df = df,
#     col_name = col_name,
#     target_type_name = "POSIXct",
#     check_fn = is.POSIXct,
#     convert_fn = as.POSIXct,
#     format = format,
#     tz = tz
#   )
# }

# convert_to_date <- function(df, col_name, format = "%Y-%m-%d") {
#   convert_column_type(
#     df = df,
#     col_name = col_name,
#     target_type_name = "Date",
#     check_fn = function(x) inherits(x, "Date"),
#     convert_fn = as.Date,
#     format = format
#   )
# }

# # Usage with wrappers (even cleaner!)
# bids <- convert_to_numeric(bids, "PRICE", fix_leading_o = TRUE)
# bids <- convert_to_posixct(bids, "TIMESTAMP")
# bids <- convert_to_date(bids, "DATE_UTC")

