source("src/data_cleaning.r")  # Functions loaded at runtime - linter warnings are false positives
library(here)
library(dplyr)
library(tigris)   # for zctas()
library(sf)       # for spatial functions
library(stringr)  # for string cleaning
library(profvis)
library(zipcodeR)
library(arrow)

data_cleaning_pipeline <- function(df, expected_columns, zip_code_db = NULL, save_path = NULL, verbose = TRUE) {
  bids <- clean_price_column(bids,
                             min_price = 0,
                             max_price = 10,
                             fix_leading_o = TRUE,
                             verbose = verbose)

  bids <- clean_geo_region_column(bids,
                                  verbose = verbose)

  bids <- clean_zip_column(bids,
                           zip_code_db = zip_code_db,
                           verbose = verbose)

  bids <- clean_response_time_column(bids,
                                     col_name = "RESPONSE_TIME",
                                     output_col_name = "RESPONSE_TIME_clean",
                                     extract_digits = TRUE,
                                     verbose = verbose)

  bids <- clean_timestamp_column(bids,
                                 col_name = "TIMESTAMP",
                                 verbose = verbose)

  bids <- clean_city_column(bids,
                            zip_code_db = zip_code_db,
                            verbose = verbose)

  bids <- clean_geo_coordinates_column(bids,
                                       verbose = verbose)

  bids <- clean_bids_won_column(bids,
                                verbose = verbose)

  bids <- clean_date_column(bids,
                            col_name = "DATE_UTC",
                            output_col_name = "DATE_UTC_clean",
                            verbose = verbose)

  bids <- clean_device_type_column(bids,
                                   col_name = "DEVICE_TYPE",
                                   output_col_name = "DEVICE_TYPE_clean",
                                   verbose = verbose)

  bids <- clean_response_time_column(bids,
                                     col_name = "RESPONSE_TIME",
                                     output_col_name = "RESPONSE_TIME_clean",
                                     extract_digits = TRUE,
                                     verbose = verbose)

  bids <- clean_requested_sizes_column(bids,
                                       col_name = "REQUESTED_SIZES",
                                       output_col_name = "REQUESTED_SIZES_clean",
                                       verbose = verbose)

  duplicate_handler <- remove_duplicates(bids,
                                         exclude_cols = c("row_id"),
                                         verbose = verbose)
  bids <- duplicate_handler[["df"]]
  removed_indices <- duplicate_handler[["removed_indices"]]

  if (!is.null(save_path)) {
    write_parquet(bids, save_path)
  }

  return(bids)
}


# ---- Timing Start ----
run_time <- system.time({

#------------------------------------------------------
# LOAD EXPECTED BIDS COLUMNS FROM CSV
#------------------------------------------------------
expected_columns <- data.frame(readr::read_csv(
  here::here("data", "expected_columns.csv"),
  col_types = "ccc"
))

#------------------------------------------------------
# LOAD BIDS DATA FROM PARQUET
#------------------------------------------------------
cat("\n")
cat(strrep("=", 70), "\n")
cat("STARTING BIDS DATA PROCESSING\n")
cat(strrep("=", 70), "\n\n")

# Load data
cat("Loading data...\n")
original_bids <- read_parquet(here("data", "bids_data_vDTR.parquet"))
bids <- original_bids %>% mutate(row_id = row_number())
cat(glue("Loaded {nrow(original_bids)} rows and {ncol(original_bids)} columns\n\n"))

#------------------------------------------------------
# LOAD ZIPCODE DATA
#------------------------------------------------------
# Load ZIP → city lookup from zipcodeR
zip_code_db <- load_oregon_zips()

#------------------------------------------------------
# CHECK FOR MISSING COLUMNS
#------------------------------------------------------
missing_columns <- check_columns(bids, expected_columns$column)
cat(glue::glue("There are {length(missing_columns)} missing column(s): \n {paste(missing_columns, collapse = ', ')}"))

#------------------------------------------------------
# TYPE SUMMARY
#------------------------------------------------------
bids_type_summary <- check_column_types(bids, expected_columns)
print(bids_type_summary)

save_path <- NULL
# save_path <- here("data", "bids_data_vDTR_clean.parquet")
bids <- data_cleaning_pipeline(bids, expected_columns, zip_code_db, save_path, verbose = TRUE)

cat("\n")



})  # ---- Timing End ----

cat(glue::glue("\n\nTotal runtime for data cleaning: {round(run_time[['elapsed']], 2)} seconds\n"))







# # Fill missing cities where ZIP is available
# bids2 <- bids2 %>%
#   left_join(zip_db, by = c("DEVICE_GEO_ZIP_clean" = "zipcode")) %>%
#   mutate(
#     DEVICE_GEO_CITY_clean = ifelse(
#       is.na(DEVICE_GEO_CITY_clean) & !is.na(DEVICE_GEO_ZIP_clean),
#       major_city,           # Use lookup
#       DEVICE_GEO_CITY_clean # Keep existing
#     )
#   ) %>%
#   select(-major_city)  # Remove temp column

# })

# # View the profile (saves to HTML and opens in browser)
# htmlwidgets::saveWidget(p, "profile.html")
# browseURL("profile.html")

# price_pipe <- function(df,
#                        col_name,
#                        output_col_name = "PRICE_clean",
#                        fix_leading_o = TRUE,
#                        verbose = TRUE){

#   df <- convert_to_numeric(bids, "PRICE", output_col_name = "PRICE_clean", fix_leading_o = TRUE) #nolint
#   df <- df %>%
#     mutate(PRICE_final = case_when(
#       .data[[output_col_name]] <= 0 ~ NA_real_,
#       .data[[output_col_name]] > 10 ~ NA_real_,
#       TRUE ~ .data[[output_col_name]]
#     ))
#   df

# }

# bids <- price_pipe(bids, "PRICE", output_col_name = "PRICE_clean", fix_leading_o = TRUE)


# bids <- convert_to_numeric(bids, "PRICE", output_col_name = "PRICE_clean", fix_leading_o = TRUE)
# cat("\nFirst 10 PRICE_clean values:\n")
# cat(head(bids$PRICE_clean, 10), sep = "\n")
#
# bids <- bids %>%
#   mutate(PRICE_final = case_when(
#     PRICE_clean <= 0 ~ NA_real_,
#     PRICE_clean > 10 ~ NA_real_,
#     TRUE ~ PRICE_clean
#   ))


# ==============================================================================
# USAGE EXAMPLE / MAIN EXECUTION
# ==============================================================================
#
# # Only run this section if the script is executed directly (not sourced as library)
# if (interactive()) {
#
#   # Define expected column types for bids data
#   expected_bids_columns <- data.frame(readr::read_csv(
#     here::here("data", "expected_columns.csv"),
#     col_types = "ccc"
#   ))
#
#   cat("\n")
#   cat(strrep("=", 70), "\n")
#   cat("STARTING BIDS DATA PROCESSING\n")
#   cat(strrep("=", 70), "\n\n")
#
#   # Load data
#   cat("Loading data...\n")
#   print(here())
#   original_bids <- read_parquet(here("data", "bids_data_vDTR.parquet"))
#   bids <- original_bids %>% mutate(row_id = row_number())
#   cat(glue("Loaded {nrow(original_bids)} rows and {ncol(original_bids)} columns\n\n"))
#
#   # Clean data using pipeline
#   # bids_clean <- clean_bids_data(original_bids, expected_bids_columns)
#
#
#   cat("\n")
#   cat(strrep("=", 70), "\n")
#   cat("BIDS DATA CLEANING PIPELINE\n")
#   cat(strrep("=", 70), "\n\n")
#
#   # check if columns are present in the dataframe that are listed in the expected_bids_columns dataframe
#   tic("Missing Columns")
#   missing_cols <- check_columns(bids, expected_bids_columns$column)
#   toc()
#   # check if bids column types match the expected types
#   tic("Column Types Check")
#   type_check_summary <- check_column_types(bids, expected_bids_columns)
#   toc()
#
#   # identify columns with NA (count) and rows with NA (indices)
#   tic("NA Check")
#   na_check <- check_na(bids)
#   toc()
#   na_col_count <- na_check$col_na_counts
#   na_row_indices <- na_check$na_row_indices
#
#   # identify columns with blank values (count) and rows with blank values (indices)
#   tic("Blank Check")
#   blank_check <- check_blanks(bids)
#   toc()
#   blank_col_count <- blank_check$blank_counts
#   blank_row_indices <- blank_check$blank_row_indices
#
#   # identify duplicate rows
#   tic("Duplicate Check")
#   duplicate_check <- find_duplicates(bids)
#   toc()
#   duplicate_indices <- duplicate_check$duplicate_indices
#   num_duplicates <- duplicate_check$num_duplicates
#
#
#
#
#   # Type conversions
#   cat("\n")
#   cat(strrep("=", 70), "\n")
#   cat("APPLYING TYPE CONVERSIONS\n")
#   cat(strrep("=", 70), "\n")
#
#
#   # Verify conversions
#   cat("\n")
#   cat(strrep("=", 70), "\n")
#   cat("VERIFYING CONVERSIONS\n")
#   cat(strrep("=", 70), "\n\n")
#
#   type_check_final <- check_column_types(bids, expected_bids_columns)
#   all_match <- all(type_check_final$match, na.rm = TRUE)
#
#   if (all_match) {
#     cat("✓ All column types match expected types!\n\n")
#   } else {
#     cat("Some type mismatches remain:\n")
#     print(type_check_final %>% filter(!match))
#     cat("\n")
#   }
#
#   type_check_final <- check_column_types(bids, expected_bids_columns)
#   # End of main execution block
# }  # End if (sys.nframe() == 0)
#



