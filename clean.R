library(nanoparquet)

elevators <- read.csv("elevators.csv", skip = 1)
elevators$X <- NULL
elevators$DV_DEVICE_STATUS_DESCRIPTION <- NULL
elevators <- elevators[!is.na(elevators$BIN), ]

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

# NA out coordinates outside NYC bounding box (lat 40.49-40.92, lon -74.27 to -73.68)
out_of_bounds <- !is.na(elevators$latitude) & (
  elevators$latitude < 40.49 | elevators$latitude > 40.92 |
  elevators$longitude < -74.27 | elevators$longitude > -73.68
)
elevators$latitude[out_of_bounds] <- NA
elevators$longitude[out_of_bounds] <- NA

# Clean zip_code: replace 0 with NA, truncate to 5 digits, convert to string
# All 9-digit zips ended in 0000 (no real +4 data), so truncation is lossless
elevators$zip_code[elevators$zip_code == 0] <- NA
elevators$zip_code <- ifelse(
  is.na(elevators$zip_code), NA,
  substr(sprintf("%05d", elevators$zip_code), 1, 5)
)

write_parquet(elevators, "elevators.parquet")
