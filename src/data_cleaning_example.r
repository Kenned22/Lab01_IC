source("src/data_cleaning.r")
library(here)
library(dplyr)
library(tigris)   # for zctas()
library(sf)       # for spatial functions
library(stringr)  # for string cleaning
library(profvis)

p <- profvis({
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
# CHECK FOR MISSING COLUMNS
#------------------------------------------------------
missing_columns <- check_columns(bids, expected_columns$column)
cat(glue::glue("There are {length(missing_columns)} missing column(s): \n {paste(missing_columns, collapse = ', ')}"))

#------------------------------------------------------
# TYPE SUMMARY
#------------------------------------------------------
bids_type_summary <- check_column_types(bids,  expected_columns)
print(bids_type_summary)

#------------------------------------------------------
# PRICE COLUMN: CONVERT TO NUMERIC
# AND HANDLE IMPOSSIBLE VALUES
#------------------------------------------------------
bids <- convert_to_numeric(bids, "PRICE", output_col_name = "PRICE_clean", fix_leading_o = TRUE)
bids <- bids %>%
  mutate(PRICE_final = case_when(
    .data[["PRICE_clean"]] <= 0 ~ NA_real_,
    .data[["PRICE_clean"]] > 10 ~ NA_real_,
    TRUE ~ .data[["PRICE_clean"]]
  ))

#------------------------------------------------------
# DEVICE_GEO_REGION COLUMN: STANDARDIZE OREGON CODES
#------------------------------------------------------
bids %>%
  mutate(region_lower = stringr::str_to_lower(stringr::str_trim(DEVICE_GEO_REGION))) %>%
  count(region_lower, sort = TRUE) %>%
  filter(
    str_detect(region_lower, "^or$")|
    str_detect(region_lower, "oregon") |
    str_detect(region_lower, "xor") |
    str_detect(region_lower, "^or[^a-z]*$")
  ) %>%
  print()

bids <- bids %>%
  mutate(
    DEVICE_GEO_REGION_clean = case_when(
      str_to_lower(str_trim(DEVICE_GEO_REGION)) %in% c("or", "oregon", "xor") ~ "OR",
      TRUE ~ NA_character_
    )
  )

print(glue::glue("Number of NA values in DEVICE_GEO_REGION_clean: {sum(is.na(bids$DEVICE_GEO_REGION_clean))}"))

#------------------------------------------------------
# DEVICE_GEO_ZIP COLUMN: CONVERT TO STRING AND
# HANDLE SENTINEL VALUES
#------------------------------------------------------
# converting to string
bids <- bids %>%
  mutate(
    DEVICE_GEO_ZIP = as.character(DEVICE_GEO_ZIP)
  )

# Define sentinel ZIP codes
sentinels <- c("00000", "99999", "11111", "12345", "-999", "-99")

# check for bids issues
bids_suspicious <- bids %>%
  filter(
    is.na(DEVICE_GEO_ZIP) |                      # include NA ZIPs
    nchar(DEVICE_GEO_ZIP) != 5 |                 # wrong length
    !str_detect(DEVICE_GEO_ZIP, "^[0-9]{5}$") |  # contains non-digits
    DEVICE_GEO_ZIP %in% sentinels                # sentinel codes
  )

bids_suspicious %>%
  count(DEVICE_GEO_ZIP, sort = TRUE)

# checking for zipcode pattern
bids %>%
  count(valid_oregon = str_detect(DEVICE_GEO_ZIP, "^97[0-9]{3}$"))




options(tigris_use_cache = TRUE)

# Download U.S. ZCTA polygons (2020), pulling Oregon only
zips <- zctas(year = 2020)

# Convert bids → sf object using lat/long
df_sf <- st_as_sf(
  bids,
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
bids$zipcode <- ifelse(
  is.na(bids$DEVICE_GEO_ZIP),   # if original ZIP is missing
  joined$ZCTA5CE20,             # use mapped ZIP
  bids$DEVICE_GEO_ZIP           # else keep original
)

# Create DEVICE_GEO_ZIP_clean

# Define sentinel ZIPs (invalid placeholders)
sentinels <- c("00000", "99999", "11111", "12345", "-999", "-99")

# Define pattern for valid U.S. 5-digit ZIP codes
valid_zip_pattern <- "^[0-9]{5}$"

bids <- bids %>%
  mutate(
    # Work with character format
    zipcode = as.character(zipcode),

    # Trim whitespace
    zipcode_trim = str_trim(zipcode),

    # Replace sentinel codes with NA
    zipcode_no_sentinel = ifelse(zipcode_trim %in% sentinels,
                                 NA,
                                 zipcode_trim),

    # Enforce valid ZIP pattern (5 numeric digits)
    DEVICE_GEO_ZIP_clean = ifelse(
      str_detect(zipcode_no_sentinel, valid_zip_pattern),
      zipcode_no_sentinel,   # keep valid ZIP
      NA                     # invalid → NA
    )
  )
# drop unrequired columns
bids <- bids %>%
  select(-starts_with("zipcode"))


#------------------------------------------------------
#
#------------------------------------------------------
})

# View the profile (saves to HTML and opens in browser)
htmlwidgets::saveWidget(p, "profile.html")
browseURL("profile.html")

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

