source("src/data_cleaning.r")
library(here)
library(dplyr)
library(tigris)   # for zctas()
library(sf)       # for spatial functions
library(stringr)  # for string cleaning
library(profvis)
library(zipcodeR)

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

#------------------------------------------------------
# PRICE COLUMN: CONVERT TO NUMERIC
# AND HANDLE IMPOSSIBLE VALUES
#------------------------------------------------------
bids <- clean_price_column(bids, min_price = 0, max_price = 10, fix_leading_o = TRUE)

#------------------------------------------------------
# DEVICE_GEO_REGION COLUMN: STANDARDIZE OREGON CODES
#------------------------------------------------------
bids <- clean_geo_region_column(bids, verbose = TRUE)

#------------------------------------------------------
# DEVICE_GEO_ZIP COLUMN: CONVERT TO STRING AND
# HANDLE SENTINEL VALUES
#------------------------------------------------------
bids <- clean_zip_column(bids, zip_code_db = zip_code_db, verbose = TRUE)

#------------------------------------------------------
# RESPONSE_TIME COLUMN: CONVERT TO INTEGER
#------------------------------------------------------
bids <- convert_to_integer(bids, "RESPONSE_TIME", extract_digits = TRUE, output_col_name = "RESPONSE_TIME_clean")
na_response_time <- sum(is.na(bids$RESPONSE_TIME_clean))
cat(glue("There are {na_response_time} NA response times"), "\n")


#------------------------------------------------------
# TIMESTAMP COLUMN: CONVERT TO POSIXct
#------------------------------------------------------
bids <- convert_to_posixct(bids, "TIMESTAMP")
TIMESTAMP_na_count <- sum(is.na(bids$TIMESTAMP_clean))
cat(glue("There are {TIMESTAMP_na_count} NAs in the TIMESTAMP_clean column"), "\n")

#------------------------------------------------------
# DEVICE_GEO_CITY: FILL MISSING CITIES FROM ZIP LOOKUP
#------------------------------------------------------
bids <- clean_city_column(bids, zip_code_db = zip_code_db, verbose = TRUE)
})  # ---- Timing End ----

cat(glue::glue("\n\nTotal runtime for data cleaning: {round(run_time[['elapsed']], 2)} seconds\n"))





# #------------------------------------------------------
# # DEVICE_GEO_CITY: FILL MISSING CITIES FROM ZIP LOOKUP
# #------------------------------------------------------
# # Count missing before
# city_missing_before <- sum(is.na(bids$DEVICE_GEO_CITY))

# # Get ZIP → city lookup from zipcodeR
# zip_db <- zip_code_db %>%
#   select(zipcode, major_city)

# # Join and fill missing cities where ZIP is available
# bids <- bids %>%
#   mutate(DEVICE_GEO_CITY_clean = DEVICE_GEO_CITY) %>%
#   left_join(zip_db, by = c("DEVICE_GEO_ZIP_clean" = "zipcode")) %>%
#   mutate(
#     DEVICE_GEO_CITY_clean = ifelse(
#       is.na(DEVICE_GEO_CITY_clean) & !is.na(major_city),
#       major_city,
#       DEVICE_GEO_CITY_clean
#     )
#   ) %>%
#   select(-major_city)

# # Report results
# city_missing_after <- sum(is.na(bids$DEVICE_GEO_CITY_clean))
# city_recovered <- city_missing_before - city_missing_after

# cat("\n")
# cat(strrep("-", 50), "\n")
# cat("CITY RECOVERY REPORT\n")
# cat(strrep("-", 50), "\n")
# cat(sprintf("  Original missing:  %d\n", city_missing_before))
# cat(sprintf("  Recovered via ZIP: %d\n", city_recovered))
# cat(sprintf("  Remaining NA:      %d\n", city_missing_after))
# cat(strrep("-", 50), "\n")

# # Check what ZIPs aren't matching (for debugging)
# if (city_missing_after > 0) {
#   unmatched_zips <- bids %>%
#     filter(!is.na(DEVICE_GEO_ZIP_clean) & is.na(DEVICE_GEO_CITY_clean)) %>%
#     count(DEVICE_GEO_ZIP_clean, sort = TRUE)
#   cat(sprintf("  Unmatched ZIPs: %d unique values\n", nrow(unmatched_zips)))
#   cat("  Top unmatched ZIPs:\n")
#   print(head(unmatched_zips, 10))
# }

#------------------------------------------------------
# DEVICE_GEO_LAT AND DEVICE_GEO_LONG: FILTER OUT IMPLAUSIBLE COORDINATES
#------------------------------------------------------

coord_summary <- bids %>%
  summarise(
    min_lat  = min(DEVICE_GEO_LAT,  na.rm = TRUE),
    max_lat  = max(DEVICE_GEO_LAT,  na.rm = TRUE),
    min_long = min(DEVICE_GEO_LONG, na.rm = TRUE),
    max_long = max(DEVICE_GEO_LONG, na.rm = TRUE)
  )

#
coord_summary %>%
  mutate(
    lat_within_oregon =
      min_lat >= 42 & max_lat <= 46.5,

    long_within_oregon =
      min_long >= -125 & max_long <= -116
  )
#
if (coord_summary$min_lat >= 42 & coord_summary$max_lat <= 46.5) {
  cat("Latitudes are consistent with Oregon.\n")
} else {
  cat("Latitudes include locations outside Oregon.\n")
}

if (coord_summary$min_long >= -125 & coord_summary$max_long <= -116) {
  cat("Longitudes are consistent with Oregon.\n")
} else {
  cat("Longitudes include locations outside Oregon.\n")
}

bids_implausible_coords <- bids %>%
  filter(
    DEVICE_GEO_LAT  < 42   | DEVICE_GEO_LAT  > 46.5 |
    DEVICE_GEO_LONG < -125 | DEVICE_GEO_LONG > -116
  )
# count

bids_implausible_coords %>% nrow()

lat_min <- 42
lat_max <- 46.5
long_min <- -125
long_max <- -116

bids <- bids %>%
  mutate(
    DEVICE_GEO_LAT_clean = ifelse(
      DEVICE_GEO_LAT >= lat_min & DEVICE_GEO_LAT <= lat_max,
      DEVICE_GEO_LAT,
      NA
    ),

    DEVICE_GEO_LONG_clean = ifelse(
      DEVICE_GEO_LONG >= long_min & DEVICE_GEO_LONG <= long_max,
      DEVICE_GEO_LONG,
      NA
    )
  )
#------------------------------------------------------
# BID_WON COLUMN: CONVERT TO LOGICAL
#------------------------------------------------------
cat("Current values in BID_WON:\n")
print(table(bids$BID_WON, useNA = "always"))

# Fix the "true" values to "TRUE"
bids <- bids %>%
  mutate(
    BID_WON_clean = case_when(
      tolower(BID_WON) == "true" ~ "TRUE",
      tolower(BID_WON) == "false" ~ "FALSE",
      BID_WON == "TRUE" ~ "TRUE",
      BID_WON == "FALSE" ~ "FALSE",
      TRUE ~ NA_character_  # Set anything else to NA
    )
  )


cat("\nCleaned BID_WON_clean:\n")
print(table(bids$BID_WON_clean, useNA = "always"))

#------------------------------------------------------
# DUPLICATE ROW HANDLING
#------------------------------------------------------
unique_ids <- dplyr::count(bids, row_id)
num_unique_ids <- length(unique(bids$row_id))
cat(glue("nrows == num_unique_ids: {num_unique_ids == nrow(bids)}"), "\n")

duplicate_check <- find_duplicates(bids, exclude_cols = c("row_id"))
duplicate_indices <- duplicate_check$duplicate_indices
num_duplicates <- duplicate_check$num_duplicates

cat(glue("There are {num_duplicates} duplicate rows in the dataset"), "\n")
cat("first 10 duplicate rows: ", duplicate_indices[1:10], "\n")

duplicate_handler <- remove_duplicates(bids, exclude_cols = c("row_id"))
bids_nodup <- duplicate_handler[["df"]]
removed_indices <- duplicate_handler[["removed_indices"]]


cat("\n")


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
#   bids <- convert_to_numeric(bids, "PRICE", fix_leading_o = TRUE)
#   bids <- convert_to_posixct(bids, "TIMESTAMP")
#   bids <- convert_to_date(bids, "DATE_UTC")
#   bids <- convert_to_character(bids, "DEVICE_TYPE")
#   bids <- convert_to_list(bids, "REQUESTED_SIZES")
#   bids <- convert_to_integer(bids, "RESPONSE_TIME", extract_digits = TRUE)
#   bids <- convert_to_logical(bids, "BID_WON")
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



