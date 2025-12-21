library(data.table)
library(lubridate)
library(zoo)
library(matrixStats)
library(lightgbm)
library(ggplot2)

path_calendar <- "AFCS-Project/calendar_afcs2025.csv"
path_prices   <- "AFCS-Project/sell_prices_afcs2025.csv"
path_train    <- "AFCS-Project/sales_train_validation_afcs2025.csv"
path_testval  <- "AFCS-Project/sales_test_validation_afcs2025.csv"   # d_1914..d_1941
path_sample   <- "AFCS-Project/sample_submission_afcs2025.csv"
path_test_eval<- "AFCS-Project/sales_test_evaluation_afcs_2025.csv"  # for private evaluation only

cal  <- fread(path_calendar)
prices <- fread(path_prices)
train_wide <- fread(path_train)  
testval_wide <- fread(path_testval)
sample_sub <- fread(path_sample)
eval_sub <- fread(path_test_eval)

head(cal)
head(prices)
head(train_wide)
head(testval_wide)
head(sample_sub)
head(eval_sub)

all(train_wide$id == testval_wide$id)

if (!"id" %in% names(train_wide)) stop("train_wide does not contain column 'id' — check the file.")

cat("Head of id column:\n")
print(head(train_wide$id, 6))

split_ids <- tstrsplit(train_wide$id, "_", fixed = TRUE)
train_wide[, c("cat_part","dept_num","item_num","state_part","store_num","set_type") := .(split_ids[[1]], split_ids[[2]], split_ids[[3]], split_ids[[4]], split_ids[[5]], split_ids[[6]])]

train_wide[, item_id := paste(cat_part, dept_num, item_num, sep = "_")]
train_wide[, dept_id := paste(cat_part, dept_num, sep = "_")]
train_wide[, cat_id  := cat_part]
train_wide[, store_id := paste(state_part, store_num, sep = "_")]
train_wide[, state_id := state_part]

print(train_wide[1:4, .(id, item_id, dept_id, cat_id, store_id)])

dcols <- grep("^d_", names(train_wide), value = TRUE)
if (length(dcols) == 0) stop("No d_ columns found in train_wide — check column names")


id_vars <- c("id","item_id","dept_id","cat_id","store_id","state_id")
train_long <- melt(train_wide, id.vars = id_vars, measure.vars = dcols,
                   variable.name = "d", value.name = "units", variable.factor = FALSE)

train_long[, d_idx := as.integer(sub("d_", "", d))]
start_date <- as.Date("2011-01-29")
train_long[, date := start_date + d_idx - 1L]

train_long[, dataset := "train"]
data_long <- train_long

cal[, date := as.Date(date)]

setkey(data_long, date)
setkey(cal, date)
data_long <- cal[data_long] 

if (!("item_id" %in% names(prices))) stop("prices file does not contain 'item_id' column")

prices[, wm_yr_wk := as.integer(wm_yr_wk)]
data_long[, wm_yr_wk := as.integer(wm_yr_wk)]

setkey(prices, store_id, item_id, wm_yr_wk)
setkey(data_long, store_id, item_id, wm_yr_wk)
data_long <- prices[data_long]

setorder(data_long, item_id, date)
data_long[, sell_price := na.locf(sell_price, na.rm = FALSE), by = item_id]
data_long[is.na(sell_price), sell_price := 0]

head(data_long)

totals <- data_long[dataset == "train", .(total_units = sum(units, na.rm = TRUE)), by = item_id][order(-total_units)]
print(head(totals,10))

daily_total <- data_long[dataset == "train", .(daily_units = sum(units, na.rm = TRUE)), by = date]
ggplot(daily_total, aes(x=date, y=daily_units)) + geom_line() + ggtitle("Total daily units sold (all items)") + theme_minimal()

ggplot(data_long[dataset == "train", .(units = sum(units, na.rm=TRUE)), by=.(date, weekday)][ , weekday := factor(weekday, levels = c("Saturday","Sunday","Monday","Tuesday","Wednesday","Thursday","Friday"))], aes(x=weekday, y=units)) +
  geom_boxplot() + ggtitle("Distribution of daily sales by weekday (aggregated)") + theme_minimal()
