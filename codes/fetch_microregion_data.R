# ==============================================================================
# Aquaculture & Agriculture Microregion Analysis (IBGE/SIDRA)
# Fetch aquaculture production and agricultural GVA data (SIDRA tables 3940 & 5938)
# Data Pipeline: Bronze (raw parquet) → Silver (clean parquet)
# ==============================================================================

# ------------------------------------------------------------------------------
# 0. Load Required Packages
# ------------------------------------------------------------------------------

load_or_install <- function(pkgs) {
  for (pkg in pkgs) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      message(sprintf("Installing package: %s", pkg))
      install.packages(pkg, dependencies = TRUE)
    } else {
      message(sprintf("Package already loaded: %s", pkg))
    }
  }
}

load_or_install(c("sidrar", "arrow", "dplyr", "tidyr"))

# ------------------------------------------------------------------------------
# 1. Extract Raw Data (Bronze)
# ------------------------------------------------------------------------------

production_data <- sidrar::get_sidra(
  api = "/t/3940/n1/all/n9/all/v/all/p/first%209/c654/32870%2032872%2032873%2032874%2032875%2032876%2032877%2032879%2032886/d/v215%203,v1000215%205"
)

agri_data <- sidrar::get_sidra(
  api = "/t/5938/n1/all/n9/all/v/513/p/last%209/d/v513%203"
)

arrow::write_parquet(production_data, "data/bronze/production_data.parquet")
arrow::write_parquet(agri_data, "data/bronze/agri_data.parquet")

# ------------------------------------------------------------------------------
# 2. Transform Data Shape (Silver)
# ------------------------------------------------------------------------------

reshape_data <- function(data) {
  data |>
    dplyr::select(
      `Nível Territorial (Código)`,
      `Nível Territorial`,
      `Brasil e Microrregião Geográfica (Código)`,
      `Brasil e Microrregião Geográfica`,
      Ano,
      Variável,
      Valor
    ) |>
    tidyr::pivot_wider(
      names_from = Variável,
      values_from = Valor
    )
}

production_data <- reshape_data(production_data)
agri_data <- reshape_data(agri_data)

# ------------------------------------------------------------------------------
# 3. Merge Data Sources (Silver)
# ------------------------------------------------------------------------------

microregion <- dplyr::left_join(
  production_data,
  agri_data,
  by = c(
    "Nível Territorial (Código)", 
    "Nível Territorial",
    "Brasil e Microrregião Geográfica (Código)",
    "Brasil e Microrregião Geográfica",
    "Ano"
  )
)

# Rename Variables (Standardized, snake_case)
colnames(microregion) <- c(
  "territory_level_code",
  "territory_level",
  "microregion_code",
  "microregion",
  "year",
  "aquaculture_production",
  "production_value",
  "production_value_share",
  "gva_agriculture_current_prices"
)

# ------------------------------------------------------------------------------
# 4. Save Final Silver Data
# ------------------------------------------------------------------------------

arrow::write_parquet(microregion, "data/silver/microregion_data.parquet")

# ------------------------------------------------------------------------------
# 5. Clean Environment
# ------------------------------------------------------------------------------

rm(list = setdiff(ls(), c("microregion", "load_or_install")))
