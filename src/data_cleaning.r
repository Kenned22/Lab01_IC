library(logger)
library(glue)
log_appender(appender_tee("logs/data_cleaning.log"))


column_check <- function(df, expected_columns) {
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

  missing_columns
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
type_check <- function(df, expected_types) {
  # Validate expected_types structure

  #THIS IS HARDCODED FOR NOW, EXPECTING SPECIFIC FORMAT OF expected_types, WILL MAKE MORE FLEXIBLE LATER
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

  temp_comparison <- tibble(column = common_cols) %>%
    left_join(expected_types, by = "column") %>%
    pull("Expected_Type") %>%
    unname()

  col_type_comp <- temp_comparison == df_types[common_cols]

  # Create comparison data frame for common columns only
  comparison <- tibble(column = common_cols) %>%
    mutate(actual = df_types[common_cols]) %>%
    left_join(expected_types, by = "column") %>%
    mutate(match = col_type_comp, .before = "Notes_Actual_Type")

  comparison
}

col_na_check <- function(df) {
  col_na_counts <- colSums(is.na(df))
  col_na_counts
}

get_na_row_indices <- function(df) {
  which(apply(df, 1, function(x) any(is.na(x))))
}

#' Get type conversion function for a given type name
#'
#' @param target_type A character string specifying the target type
#'                    ("character", "numeric", "integer", "logical", "Date", "POSIXct")
#' @return A function that converts values to the target type
#' @examples
#' converter <- get_type_converter("numeric")
#' result <- converter("123")  # Returns 123
type_conversion <- function(target_type) {
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


col_blank_count <- bids %>%
  # Step 1: Count blanks in each column
  summarise(across(everything(), ~ sum(. == "", na.rm = TRUE))) %>%
  # Step 2: Convert from wide to long format (easier to work with)
  pivot_longer(everything(), names_to = "column", values_to = "blank count")


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

# check if columns are present in the dataframe that are listed in the expected_bids_columns dataframe
missing_cols <- column_check(bids, expected_bids_columns$column)

# check if bids column types match the expected types
type_check_summary <- type_check(bids, expected_bids_columns)

# identify columns with NA (count) and rows with NA (indices)
na_col_count <- col_na_check(bids)
na_row_indices <- get_na_row_indices(bids)

# identify columns with blank values (count) and rows with blank values (indices)



# identify duplicate rows



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


