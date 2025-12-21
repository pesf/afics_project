library(data.table)
library(lubridate)
library(zoo)
library(tsibble) # Essential for time series objects
library(dplyr)   # Helper for data manipulation

# --- 1. SETUP & LOAD ---
path_calendar <- "data/calendar_afcs2025.csv"
path_prices   <- "data/sell_prices_afcs2025.csv"
path_train    <- "data/sales_train_validation_afcs2025.csv"
path_val      <- "data/sales_test_validation_afcs2025.csv" # The "File 4"

# Load reference data
cal <- fread(path_calendar)
prices <- fread(path_prices)

# Load sales data
train_wide <- fread(path_train)
val_wide   <- fread(path_val)

# --- 2. PREPARATION HELPERS ---

# Helper to split IDs (Category, Department, Store details)
# We apply this to both datasets to ensure identical features
enrich_features <- function(dt) {
  # Split ID string
  dt[, c("cat_id", "dept_id", "item_code", "state_id", "store_code", "set_type") := 
       tstrsplit(id, "_", fixed = TRUE)]
  
  # Reconstruct standard columns
  dt[, item_id := paste(cat_id, dept_id, item_code, sep = "_")]
  dt[, store_id := paste(state_id, store_code, sep = "_")]
  
  # Clean up temporary columns if desired, or keep them. 
  # Returning with primary keys for merging.
  return(dt)
}

# Helper to Melt and Merge
process_dataset <- function(wide_df, calendar_df, prices_df, dataset_label) {
  
  # 1. Feature Engineering (Split IDs)
  wide_df <- enrich_features(wide_df)
  
  # 2. Identify 'd_' columns dynamically
  dcols <- grep("^d_", names(wide_df), value = TRUE)
  
  # 3. Reshape Wide to Long
  # We use 'id' and the generated feature columns as identifiers
  id_vars <- c("id", "item_id", "store_id", "cat_id", "dept_id", "state_id")
  long_df <- melt(wide_df, 
                  id.vars = id_vars, 
                  measure.vars = dcols,
                  variable.name = "d", 
                  value.name = "units", 
                  variable.factor = FALSE)
  
  # 4. Create Date Column
  # d_1 corresponds to 2011-01-29. Logic: Start + (Index - 1)
  start_date <- as.Date("2011-01-29")
  long_df[, d_idx := as.integer(sub("d_", "", d))]
  long_df[, date := start_date + (d_idx - 1L)]
  
  # 5. Label the dataset
  long_df[, dataset_type := dataset_label]
  
  # 6. Merge Calendar
  # Ensure calendar date is Date object
  calendar_df[, date := as.Date(date, format = "%m/%d/%Y")]
  setkey(long_df, date)
  setkey(calendar_df, date)
  long_df <- calendar_df[long_df] # Left join sales to calendar
  
  # 7. Merge Prices
  # Ensure join keys are integers/consistent
  prices_df[, wm_yr_wk := as.integer(wm_yr_wk)]
  long_df[, wm_yr_wk := as.integer(wm_yr_wk)]
  
  setkey(prices_df, store_id, item_id, wm_yr_wk)
  setkey(long_df, store_id, item_id, wm_yr_wk)
  long_df <- prices_df[long_df] # Left join sales to prices
  
  # 8. Price Imputation (Last Observation Carried Forward)
  setorder(long_df, item_id, store_id, date)
  long_df[, sell_price := na.locf(sell_price, na.rm = FALSE), by = c("item_id", "store_id")]
  long_df[is.na(sell_price), sell_price := 0] # Fill remaining NAs
  
  return(long_df)
}

# --- 3. EXECUTION ---

# Process Training Data
cat("Processing Training Data...\n")
train_dt <- process_dataset(train_wide, copy(cal), copy(prices), "train")

# Process Validation Data (File 4)
cat("Processing Validation Data...\n")
val_dt   <- process_dataset(val_wide, copy(cal), copy(prices), "validation")


# --- 4. CONVERT TO TSIBBLE ---

# Function to convert data.table to tsibble
to_tsibble_format <- function(dt) {
  # Tsibble requires a Key (Unique identifier) and Index (Time)
  # We convert back to tibble first as tsibble prefers it
  as_tsibble(dt, 
             key = c(id, store_id, item_id), # Key: What defines a unique series?
             index = date)                   # Index: The time variable
}

cat("Converting to tsibble...\n")
train_ts <- to_tsibble_format(train_dt)
val_ts   <- to_tsibble_format(val_dt)

# Inspect
print(train_ts)
print(val_ts)

# --- 5. SAVE FILES ---

cat("Saving files...\n")
# Saving as RDS maintains the tsibble structure (keys/index)
saveRDS(train_ts, "data/train_data.rds")
saveRDS(val_ts,   "data/validation_data.rds")

cat("Done! Files saved as 'train_data.rds' and 'validation_data.rds'")