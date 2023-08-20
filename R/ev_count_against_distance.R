require(tidyverse)

cleanup_vistas_lgas <- function(df) {
  df |>
    mutate(OFFICIALNM = str_to_upper(LGA)) |>
    mutate(OFFICIALNM = replace(OFFICIALNM, OFFICIALNM=="MELTON SHIRE", "MELTON CITY")) |>
    mutate(OFFICIALNM = replace(OFFICIALNM, OFFICIALNM=="MORELAND CITY", "MERRI-BEK CITY"))
}

ev_proportions <- read_csv("output_data/LGA_EV_PROPORTION.csv")


# EV proportion against median vehicle travel distance
median_vehicle_travel <- read_csv("output_data/Median_Vehicle_Travel_Dist.csv") |>
  cleanup_vistas_lgas()

ev_against_vehicle <- median_vehicle_travel |>
  left_join(ev_proportions, by=join_by(OFFICIALNM==LGA)) |>
  select(OFFICIALNM, MEDIAN_DISTANCE, EV_PROPORTION)

ev_against_vehicle |>
  ggplot(aes(EV_PROPORTION, MEDIAN_DISTANCE)) +
  geom_point()

write_csv(ev_against_vehicle, "output_data/ev_against_vehicle.csv")


# EV proportion against median vehicle travel distance
median_bicycle_travel <- read_csv("output_data/Median_Bicycle_Travel_Dist.csv") |>
  cleanup_vistas_lgas()

ev_against_bicycle <- median_bicycle_travel |>
  left_join(ev_proportions, by=join_by(OFFICIALNM==LGA)) |>
  select(OFFICIALNM, MEDIAN_DISTANCE, EV_PROPORTION)

ev_against_bicycle |>
  ggplot(aes(EV_PROPORTION, MEDIAN_DISTANCE)) +
  geom_point()

write_csv(ev_against_bicycle, "output_data/ev_against_bicycle.csv")

# EV proportion against median train travel distance
median_train_travel <- read_csv("output_data/Median_Train_Travel_Dist.csv") |>
  cleanup_vistas_lgas()

ev_against_train <- median_train_travel |>
  left_join(ev_proportions, by=join_by(OFFICIALNM==LGA)) |>
  select(OFFICIALNM, MEDIAN_DISTANCE, EV_PROPORTION)

ev_against_train |>
  ggplot(aes(EV_PROPORTION, MEDIAN_DISTANCE)) +
  geom_point()

write_csv(ev_against_train, "output_data/ev_against_train.csv")

# EV proportion against train travel proportion
train_travel_prop <- read_csv("output_data/Train_Travel_Proportion_LGA.csv")

ev_against_train_prop <- train_travel_prop |>
  left_join(ev_proportions, by=join_by(LGA==LGA))

write_csv(ev_against_train_prop, "output_data/ev_against_train_prop.csv")

