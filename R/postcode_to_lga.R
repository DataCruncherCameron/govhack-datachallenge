# Maps postcodes to LGAs
# Provides the percentage of each postcode in a given LGA

library(tidyverse)
library(sf)

lga_merged <- st_read('../external_data/lga_polygon/shapefiles/AD_LGA_AREA_POLYGON.shp ') |>
  filter(STATE == 'VIC') |>
  group_by(OFFICIALNM, LGA_CODE) |>
  summarise(geometry = st_union(geometry)) |>
  ungroup()

postcodes <- st_read('../external_data/postcode_polygon/shapefiles/POSTCODE_POLYGON.shp') |>
  mutate(total_area = st_area(geometry))

get_intersection_postcodes <- function(lga) {
  st_intersection(lga, postcodes) |>
    mutate(intersection_area = st_area(geometry)) |>
    mutate(intersection_pct = units::drop_units(intersection_area / total_area)) |>
    filter(intersection_pct > 0.01) # Drop postcodes with less than 1% of their total area in the LGA
}

x <- get_intersection_postcodes(slice(lga_merged, 1))
for(i in 1:nrow(lga_merged)) {       # for-loop over rows
  x <- rbind(x, get_intersection_postcodes(slice(lga_merged, i)))
}

output <- x |>
  st_drop_geometry() |>
  select(OFFICIALNM, LGA_CODE, intersection_pct, POSTCODE)

write_csv(output, "../mappings/postcode_mappings.csv")
