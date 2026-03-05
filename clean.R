library(nanoparquet)

elevators <- read.csv("elevators.csv", skip = 1)
elevators$X <- NULL

names(elevators) <- tolower(gsub(".", "_", names(elevators), fixed = TRUE))

write_parquet(elevators, "elevators.parquet")
