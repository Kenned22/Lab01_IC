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
  df <- clean_price_column(df,
                           min_price = 0,
                           max_price = 10,
                           fix_leading_o = TRUE,
                           verbose = verbose)

  df <- clean_geo_region_column(df,
                                verbose = verbose)

  df <- clean_zip_column(df,
                         zip_code_db = zip_code_db,
                         verbose = verbose)

  df <- clean_response_time_column(df,
                                   col_name = "RESPONSE_TIME",
                                   output_col_name = "RESPONSE_TIME_clean",
                                   extract_digits = TRUE,
                                   verbose = verbose)

  df <- clean_timestamp_column(df,
                               col_name = "TIMESTAMP",
                               verbose = verbose)

  df <- clean_city_column(df,
                          zip_code_db = zip_code_db,
                          verbose = verbose)

  df <- clean_geo_coordinates_column(df,
                                     verbose = verbose)

  df <- clean_bids_won_column(df,
                              verbose = verbose)

  df <- clean_date_column(df,
                          col_name = "DATE_UTC",
                          output_col_name = "DATE_UTC_clean",
                          verbose = verbose)

  df <- clean_device_type_column(df,
                                 col_name = "DEVICE_TYPE",
                                 output_col_name = "DEVICE_TYPE_clean",
                                 verbose = verbose)

  df <- clean_response_time_column(df,
                                   col_name = "RESPONSE_TIME",
                                   output_col_name = "RESPONSE_TIME_clean",
                                   extract_digits = TRUE,
                                   verbose = verbose)

  df <- clean_requested_sizes_column(df,
                                     col_name = "REQUESTED_SIZES",
                                     output_col_name = "REQUESTED_SIZES_clean",
                                     verbose = verbose)

  duplicate_handler <- remove_duplicates(df,
                                         exclude_cols = c("row_id"),
                                         verbose = verbose)
  df <- duplicate_handler[["df"]]
  removed_indices <- duplicate_handler[["removed_indices"]]

  if (!is.null(save_path)) {
    write_parquet(df, save_path)
  }

  return(df)
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
cat(strrep("=", 70), "\n")
cat("CREATING FINAL CLEANED DATASET\n")
cat(strrep("=", 70), "\n\n")
# Create final cleaned dataset
bids_clean <- bids %>%
  select(
    row_id,
    DATE_UTC_clean,
    TIMESTAMP_clean,
    AUCTION_ID,
    PUBLISHER_ID,
    PRICE_final,
    DEVICE_GEO_REGION_clean,
    DEVICE_GEO_ZIP_clean,
    DEVICE_GEO_CITY_clean,
    DEVICE_GEO_LAT_clean,
    DEVICE_GEO_LONG_clean,
    BID_WON_clean,
    RESPONSE_TIME_clean,
    DEVICE_TYPE_clean,
    SIZE,
    REQUESTED_SIZES_clean
  )
  # %>%
  # rename_with(~ str_remove(., "(_clean|_final)$"))


print(class(bids_clean))
glimpse(bids_clean)
# NA counts per column
na_count_by_col <- colSums(is.na(bids_clean %>% select(-REQUESTED_SIZES_clean)))
cat("\nNA Counts per Column:\n")
cat(strrep("=", 70), "\n")
print(na_count_by_col)
total_na_rows <- sum(!complete.cases(bids_clean %>% select(-REQUESTED_SIZES)))
print(glue("Total NA rows: {total_na_rows}"))



# # Verify conversions
# cat("\n")
# cat(strrep("=", 70), "\n")
# cat("VERIFYING CONVERSIONS\n")
# cat(strrep("=", 70), "\n\n")

# type_check_final <- check_column_types(bids, expected_bids_columns)
# all_match <- all(type_check_final$match, na.rm = TRUE)

# if (all_match) {
#   cat("✓ All column types match expected types!\n\n")
# } else {
#   cat("Some type mismatches remain:\n")
#   print(type_check_final %>% filter(!match))
#   cat("\n")
# }

# type_check_final <- check_column_types(bids, expected_columns)



})  # ---- Timing End ----

cat(glue::glue("\n\nTotal runtime for data cleaning: {round(run_time[['elapsed']], 2)} seconds\n"))



