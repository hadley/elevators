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

# Replace 0 with NA in zip_code
elevators$zip_code[elevators$zip_code == 0] <- NA

write_parquet(elevators, "elevators.parquet")
