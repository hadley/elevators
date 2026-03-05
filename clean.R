library(nanoparquet)

elevators <- read.csv("elevators.csv", skip = 1)
elevators$X <- NULL
elevators$DV_DEVICE_STATUS_DESCRIPTION <- NULL

names(elevators) <- tolower(gsub(".", "_", names(elevators), fixed = TRUE))

# Replace "" with NA in character columns
elevators[] <- lapply(elevators, function(x) {
  if (is.character(x)) ifelse(x == "", NA, x) else x
})

# Replace 0 with NA in zip_code
elevators$zip_code[elevators$zip_code == 0] <- NA

write_parquet(elevators, "elevators.parquet")
