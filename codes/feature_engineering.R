# ==============================================================================
# Aquaculture & Agriculture Microregion Analysis (IBGE/SIDRA)
# Create Regional Economics Indicators
# Data Pipeline: Silver (parquet) → Gold (parquet)
# ==============================================================================

# ------------------------------------------------------------------------------
# 0. Load Required Packages
# ------------------------------------------------------------------------------

load_or_install(c("arrow", "dplyr", "tidyr", "purrr", "psych"))

# ------------------------------------------------------------------------------
# 1. Subregional Metrics
# ------------------------------------------------------------------------------

# 1. Relative Participation Indicator (RPI)
microregion <- microregion |>
  dplyr::group_by(year) |>
  dplyr::mutate(relative_participation_indicator = production_value / production_value[1]) |>
  dplyr::ungroup()

# 2. Relative Participation Index of Agricultural Sector (RPI-AGRI)
microregion <- microregion |>
  dplyr::group_by(year) |>
  dplyr::mutate(relative_participation_agri = gva_agriculture_current_prices / gva_agriculture_current_prices[1]) |>
  dplyr::ungroup()

# 3. Location Quotient (LQ)
microregion <- microregion |>
  dplyr::group_by(year) |>
  dplyr::mutate(location_quotient = relative_participation_indicator / relative_participation_agri) |>
  dplyr::ungroup()

# 4. Aquaculture Intensity Index (AII)
microregion <- microregion |>
  dplyr::group_by(year) |>
  dplyr::mutate(aquaculture_intensity_index = location_quotient * relative_participation_indicator) |>
  dplyr::ungroup()

# 5. Price per kg
microregion <- microregion |>
  dplyr::group_by(year) |>
  dplyr::mutate(price_kg = (production_value * 1000) / aquaculture_production) |>
  dplyr::ungroup()

# Save intermediate Gold dataset
arrow::write_parquet(microregion, "data/gold/microregion_data.parquet")

# ------------------------------------------------------------------------------
# 2. General Metrics – Auxiliary Functions
# ------------------------------------------------------------------------------

# Generic inequality calculator
calc_ineq <- function(df, type) {
  df |>
    dplyr::group_by(year) |>
    dplyr::group_split() |>
    purrr::map_dfr(~{
      data <- .x[-1, ]  # remove first row (Brazil)
      data.frame(
        year = unique(data$year),
        production_ineq = ineq::ineq(data$production_value, type = type),
        gva_agri_ineq = ineq::ineq(data$gva_agriculture_current_prices, type = type),
        aii_ineq = ineq::ineq(data$aquaculture_intensity_index, type = type)
      )
    })
}

# Weighted Gini Index
weighted_gini <- function(df, n) {
  df <- df[order(df[[11]], decreasing = TRUE), ]
  X <- df[[11]]
  Y <- df[[10]]
  X_cum <- cumsum(X)
  Y_cum <- cumsum(Y)
  subX <- ifelse(is.na(dplyr::lag(X_cum)), X_cum, X_cum - dplyr::lag(X_cum))
  sumY <- ifelse(is.na(dplyr::lag(Y_cum)), Y_cum, Y_cum + dplyr::lag(Y_cum))
  PROD <- subX * sumY
  round(abs(1 - sum(PROD)), n)
}

# Location Concentration Index (CL)
cl_index_fun <- function(df, n) {
  A <- df[[11]]
  P <- df[[10]]
  round(0.5 * sum(abs(P - A)), n)
}

# ------------------------------------------------------------------------------
# 3. General Metrics – Calculation
# ------------------------------------------------------------------------------

# Standard inequality indices
gini_index <- calc_ineq(microregion, "Gini")
theil_index <- calc_ineq(microregion, "Theil")
atkinson_index <- calc_ineq(microregion, "Atkinson")

# Weighted Gini Index
wgini_index <- microregion[-c(1:9), ] |>
  tidyr::drop_na() |>
  dplyr::group_by(year) |>
  dplyr::group_split() |>
  purrr::map_dfr(~data.frame(
    year = unique(.x$year),
    weighted_gini = weighted_gini(.x, 7)
  ))

# CL Index
cl_index <- microregion[-c(1:9), ] |>
  tidyr::drop_na() |>
  dplyr::group_by(year) |>
  dplyr::group_split() |>
  purrr::map_dfr(~data.frame(
    year = unique(.x$year),
    cl_index = cl_index_fun(.x, 7)
  ))

# ------------------------------------------------------------------------------
# 5. Clean Environment
# ------------------------------------------------------------------------------

rm(list = setdiff(ls(), "microregion"))
