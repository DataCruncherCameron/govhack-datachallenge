require(tidyverse)

fleet <- read_csv("external_data/Whole_Fleet_Vehicle_Registration_Snapshot_by_Postcode_Q2_2023.csv") |>
  group_by(CD_CL_FUEL_ENG) |>
  summarise(total_cars = n())

write_csv(fleet, "output_data/fleet_summary.csv")
