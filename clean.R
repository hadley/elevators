library(nanoparquet)

elevators <- read.csv("elevators.csv", skip = 1)
elevators$X <- NULL
elevators$DV_DEVICE_STATUS_DESCRIPTION <- NULL

names(elevators) <- tolower(gsub(".", "_", names(elevators), fixed = TRUE))

# Replace "" with NA in character columns
elevators[] <- lapply(elevators, function(x) {
  if (!is.character(x)) return(x)
    x <- trimws(gsub("\\s+", " ", x))
    ifelse(x == "", NA, x)
})

# Parse date columns (YYYYMMDD format)
parse_date <- function(x) as.Date(as.character(x), format = "%Y%m%d")
elevators$dv_lastper_insp_date <- parse_date(elevators$dv_lastper_insp_date)
elevators$dv_approval_date <- parse_date(elevators$dv_approval_date)
elevators$dv_status_date <- parse_date(elevators$dv_status_date)

# Clean dv_speed_fpm: extract leading number, convert to numeric
elevators$dv_speed_fpm <- suppressWarnings(
  as.numeric(gsub("^([0-9.,]+).*", "\\1", gsub(",", "", elevators$dv_speed_fpm)))
)

# Clean dv_capacity_lbs: fix O->0, remove commas, extract leading number
elevators$dv_capacity_lbs <- gsub("[Oo]", "0", elevators$dv_capacity_lbs)
elevators$dv_capacity_lbs <- gsub(",", "", elevators$dv_capacity_lbs)
elevators$dv_capacity_lbs <- suppressWarnings(
  as.numeric(gsub("[^0-9.].*$", "", elevators$dv_capacity_lbs))
)

# Replace 0 with NA in zip_code
elevators$zip_code[elevators$zip_code == 0] <- NA

write_parquet(elevators, "elevators.parquet")
