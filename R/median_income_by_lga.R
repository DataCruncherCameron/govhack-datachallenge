require(tidyverse)

lgas_income <- read_csv("external_data/income_without_gender.csv")

calc_med_freq <- function(x) {median(rep(lgas_income$INCOME, x))}
median_incomes <- apply(lgas_income, 2, calc_med_freq)

output_median_incomes <- tibble(LGA=names(lgas_income), MEDIAN_INCOME=median_incomes) |>
  mutate(LGA=str_to_upper(LGA)) |>
  mutate(LGA=str_replace(LGA, fixed("BAYSIDE (VIC.)"), "BAYSIDE")) |>
  mutate(LGA=str_replace(LGA, fixed("KINGSTON (VIC.)"), "KINGSTON")) |>
  mutate(LGA=str_replace(LGA, fixed("LATROBE (VIC.)"), "LATROBE")) |>
  mutate(LGA=str_replace(LGA, fixed("MORELAND"), "MERRI-BEK")) |>
  slice(2:80)

# write_csv(output_median_incomes, "output_data/median_incomes.csv")

lga_prop <- read_csv("output_data/LGA_EV_PROPORTION.csv") |>
  mutate(LGA_MOD=str_replace(LGA, " SHIRE", "")) |>
  mutate(LGA_MOD=str_replace(LGA_MOD, " RURAL CITY", "")) |>
  mutate(LGA_MOD=str_replace(LGA_MOD, " BOROUGH", "")) |>
  mutate(LGA_MOD=str_replace(LGA_MOD, " CITY", ""))

median_incomes <- lga_prop |>
  left_join(output_median_incomes, by=join_by(LGA_MOD==LGA)) |>
  drop_na()

median_incomes |>
  select(LGA, MEDIAN_INCOME) |>
  write_csv("output_data/median_income_by_lga.csv")

median_incomes |>
  select(LGA, MEDIAN_INCOME, EV_PROPORTION) |>
  write_csv("output_data/ev_against_median_income.csv")
