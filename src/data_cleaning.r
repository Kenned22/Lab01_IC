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

log_appender(appender_tee("logs/data_cleaning.log"))

# # ==============================================================================
# # VALIDATION FUNCTIONS
# # ==============================================================================

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

#' #' Find duplicate rows
#' #'
#' #' @param df A data frame to check
#' #' @param exclude_cols Column names to exclude from duplication check
#' #' @return List with duplicate_indices and num_duplicates
#' find_duplicates <- function(df, exclude_cols = c("row_id")) {
#'   check_cols <- setdiff(names(df), exclude_cols)
#'   duplicate_indices <- which(duplicated(df[, check_cols]))
#'
#'   list(
#'     duplicate_indices = duplicate_indices,
#'     num_duplicates = length(duplicate_indices)
#'   )
#' }

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
                           output_col_name = NULL,
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

  # Create new column name if not provided
  if (is.null(output_col_name)) {
    output_col_name <- paste0(col_name, "_clean")
  }

  # Apply preprocessing if provided
  if (!is.null(preprocess_fn)) {
    if (verbose) cat("Applying preprocessing...\n")
    df[[output_col_name]] <- preprocess_fn(df[[col_name]])
  }

  # Perform conversion with error handling
  tryCatch({
    if (verbose) cat(glue("Converting {col_name}...\n"))
    df[[output_col_name]] <- converter(df[[output_col_name]], ...)

    if (verbose) {
      cat(glue("{output_col_name} is now: {class(df[[output_col_name]])[1]}\n\n"))
    }
  }, error = function(e) {
    log_error(glue("Failed to convert {output_col_name}: {e$message}"))
    stop(glue("Conversion failed for {output_col_name}: {e$message}"))
  })


  if (verbose) {
    num_NA = sum(is.na(df[[output_col_name]]))
    cat(glue("NA COUNT: \n There are {num_NA} NAs in {output_col_name}.\n\n"))
  }


  df
}

# ==============================================================================
# CONVERSION WRAPPERS
# ==============================================================================

# convert column in df to numeric. Handle leading "O" --> 0 if fix_leading_o = TRUE.

#' @param df A data frame
#' @param col_name Name of the column to convert to numeric
#' @param fix_leading_o gsub "O" --> 0
#' @param verbose Whether to print messages
#' @return Data frame with converted column
convert_to_numeric <- function(df, col_name, output_col_name = NULL, fix_leading_o = FALSE, verbose = TRUE) {
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

  convert_column(df,
                 col_name = col_name,
                 target_type = "numeric",
                 preprocess_fn = preprocess,
                 output_col_name = output_col_name,
                 verbose = verbose)
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
                               verbose = TRUE,
                               date_col = "DATE_UTC",
                               timestamp_col = "TIMESTAMP",
                               output_col = "TIMESTAMP_clean") {

  # checked DATE_UTC and TIMESTAMP columns. DATE_UTC is only the date and TIME STAMP has date and time
  # but some dates are str "NA" and when dates are present, there are two different formats. Compared
  # all dates present in TIMESTAMP to those in DATE_UTC, and found they are the same. Decided to take
  # take the dates from DATE_UTC, which has consistant formatting, and time from TIMESTAMP, and made
  # new column TIMESTAMP, which is the concatenation of the date and time. Renamed old TIMESTAMP
  # column to TIMESTAMP_0.

  # df <- df %>% rename(TIMESTAMP_0 = "TIMESTAMP")

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
                               output_col_name = NULL,
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
                 preprocess_fn = preprocess,
                 output_col_name = output_col_name,
                 verbose = verbose)
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
