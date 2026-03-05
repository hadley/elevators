library(nanoparquet)

elevators <- read.csv("elevators.csv", skip = 1)
elevators$X <- NULL
elevators <- elevators[!is.na(elevators$BIN), ]

names(elevators) <- tolower(gsub(".", "_", names(elevators), fixed = TRUE))
elevators$dv_device_status_description <- NULL

write_parquet(elevators, "elevators.parquet")
