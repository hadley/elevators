# Convert dv_travel_distance to feet. The raw strings mix several formats:
# bare feet ("60"), quoted feet ("60'"), explicit units ("24FT"),
# feet-and-inches with assorted separators ("16'4\"", "73-10", "17,8",
# "307 3"), decimal feet ("116.6"), and Excel-mangled dates ("2015-11-04",
# originally "11-4"). Values with only a trailing double-quote ("188\"")
# are an ambiguous mix of feet and inches so are set to missing, as are
# non-numeric strings and implausible magnitudes. See travel-distance.qmd
# for the analysis behind these rules.
parse_travel_distance <- function(x) {
  raw <- toupper(trimws(x))
  out <- rep(NA_real_, length(raw))
  done <- is.na(raw)

  feet_inches <- function(ft, inch) ifelse(inch <= 11, ft + inch / 12, NA_real_)

  rule <- function(pattern, string, fun) {
    m <- regmatches(string, regexec(pattern, string))
    hit <- !done & lengths(m) > 1
    group <- function(i) {
      vapply(m[hit], function(p) as.numeric(p[i + 1]), numeric(1))
    }
    out[hit] <<- fun(group)
    done <<- done | hit
  }

  # Excel read feet-inches like "11-4" as dates in the year of export (2015)
  rule("^2015-(\\d{2})-(\\d{2})$", raw, \(g) feet_inches(g(1), g(2)))
  done <- done | grepl("^\\d{4}-\\d{2}-\\d{2}$", raw)

  # Space-separated feet and inches: "307 3", "18 6\""
  rule("^(\\d+) +(\\d{1,2})[\"']{0,2}$", raw, \(g) feet_inches(g(1), g(2)))

  norm <- gsub(" ", "", raw)

  # Explicit feet unit: "24FT", "6.5FT", "35FT\"", "39\"FT"
  rule("^(\\d+(\\.\\d+)?)['\"]?-?(FT|FEET)\\.?['\"]?$", norm, \(g) g(1))

  # Plain feet, with optional decimals or trailing punctuation: "60", "60'", "116.6", "137,"
  rule("^(\\d+(\\.\\d+)?)['.,-]?$", norm, \(g) g(1))

  # Feet and inches with assorted separators: "16'4\"", "73-10", "33\"6\"", "60'-4"
  rule("^(\\d+)['\",.-]{1,2}(\\d{1,2})[\"'.:/]{0,2}$", norm, \(g) {
    feet_inches(g(1), g(2))
  })

  # Travel must be positive and no taller than NYC's tallest building (~1,300 ft)
  out[!is.na(out) & (out <= 0 | out > 1300)] <- NA
  out
}
