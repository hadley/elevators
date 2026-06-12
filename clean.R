library(nanoparquet)

elevators <- read.csv("elevators.csv", skip = 1)
elevators$X <- NULL
elevators <- elevators[!is.na(elevators$BIN), ]

names(elevators) <- tolower(gsub(".", "_", names(elevators), fixed = TRUE))
elevators$dv_device_status_description <- NULL

# Replace "" with NA in character columns
elevators[] <- lapply(elevators, function(x) {
  if (!is.character(x)) {
    return(x)
  }
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
  as.numeric(gsub(
    "^([0-9.,]+).*",
    "\\1",
    gsub(",", "", elevators$dv_speed_fpm)
  ))
)

# Clean dv_capacity_lbs: fix O->0, remove commas, extract leading number
elevators$dv_capacity_lbs <- gsub("[Oo]", "0", elevators$dv_capacity_lbs)
elevators$dv_capacity_lbs <- gsub(",", "", elevators$dv_capacity_lbs)
elevators$dv_capacity_lbs <- suppressWarnings(
  as.numeric(gsub("[^0-9.].*$", "", elevators$dv_capacity_lbs))
)

# Convert dv_travel_distance to numeric feet
source("clean-travel-distance.R")
elevators$dv_travel_distance <- parse_travel_distance(
  elevators$dv_travel_distance
)

# Clean dv_floor_from and dv_floor_to.
# See floor.qmd for the analysis behind these rules.
clean_floor <- function(x) {
  # Strip leading dots ("..B" -> "B", "..1" -> "1")
  x <- sub("^\\.+", "", x)
  # Decimal values (0.1, 0.6, etc.) are not valid floor labels
  x <- ifelse(grepl("^\\d*\\.\\d+$", x), NA, x)
  # Integer floors above NYC's tallest building (~104 floors) are impossible
  int_val <- suppressWarnings(as.integer(x))
  x <- ifelse(!is.na(int_val) & int_val > 104, NA, x)
  x
}
elevators$dv_floor_from <- clean_floor(elevators$dv_floor_from)
elevators$dv_floor_to <- clean_floor(elevators$dv_floor_to)

# Swap inverted numeric floor pairs (floor_from > floor_to)
from_int <- suppressWarnings(as.integer(elevators$dv_floor_from))
to_int <- suppressWarnings(as.integer(elevators$dv_floor_to))
inverted <- !is.na(from_int) & !is.na(to_int) & from_int > to_int
elevators$dv_floor_from[inverted] <- as.character(to_int[inverted])
elevators$dv_floor_to[inverted] <- as.character(from_int[inverted])

# NA out coordinates outside NYC bounding box (lat 40.49-40.92, lon -74.27 to -73.68)
out_of_bounds <- !is.na(elevators$latitude) &
  (elevators$latitude < 40.49 |
    elevators$latitude > 40.92 |
    elevators$longitude < -74.27 |
    elevators$longitude > -73.68)
elevators$latitude[out_of_bounds] <- NA
elevators$longitude[out_of_bounds] <- NA

# Clean zip_code: replace 0 with NA, truncate to 5 digits, convert to string
# All 9-digit zips ended in 0000 (no real +4 data), so truncation is lossless
elevators$zip_code[elevators$zip_code == 0] <- NA
elevators$zip_code <- ifelse(
  is.na(elevators$zip_code),
  NA,
  substr(sprintf("%05d", elevators$zip_code), 1, 5)
)

# Replace ** with NA in dv_manufacturer (means unknown)
elevators$dv_manufacturer[elevators$dv_manufacturer == "**"] <- NA

write_parquet(elevators, "elevators.parquet")
