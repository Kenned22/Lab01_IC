source("src/data_cleaning.r")
library(here)

#------------------------------------------
# LOAD EXPECTED BIDS COLUMNS FROM CSV
#------------------------------------------
expected_columns <- data.frame(readr::read_csv(
  here::here("data", "expected_columns.csv"),
  col_types = "ccc"
))

#------------------------------------------
# LOAD BIDS DATA FROM PARQUET
#------------------------------------------
cat("\n")
cat(strrep("=", 70), "\n")
cat("STARTING BIDS DATA PROCESSING\n")
cat(strrep("=", 70), "\n\n")

# Load data
cat("Loading data...\n")
original_bids <- read_parquet(here("data", "bids_data_vDTR.parquet"))
bids <- original_bids %>% mutate(row_id = row_number())
cat(glue("Loaded {nrow(original_bids)} rows and {ncol(original_bids)} columns\n\n"))

#------------------------------------------
# CHECK FOR MISSING COLUMNS
#------------------------------------------
missing_columns <- check_columns(bids, expected_columns$column)
cat(glue::glue("There are {length(missing_columns)} missing column(s): \n {paste(missing_columns, collapse = ', ')}"))

#------------------------------------------
# TYPE SUMMARY
#------------------------------------------
bids_type_summary <- check_column_types(bids,  expected_columns)
print(bids_type_summary)



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

