# ---------------
# 0. Library Installation and Initialisation ----
options(repos = c(CRAN = "https://cran.rstudio.com"))

message("Starting ReportPro")

install.packages("readxl")
install.packages("dplyr")
install.packages("ggplot2")
install.packages("tidyr")
install.packages("openxlsx")
install.packages("corrplot")
install.packages("DBI")
install.packages("odbc")
install.packages("randomForest")
install.packages("rstudioapi")
install.packages("jsonlite")

library(readxl)
library(dplyr)
library(ggplot2)
library(tidyr)
library(openxlsx)
library(corrplot)
library(DBI)
library(odbc)
library(randomForest)
library(rstudioapi)
library(jsonlite)

# 1. Configuration ----
script_dir <- dirname(rstudioapi::getActiveDocumentContext()$path)
config_path <- file.path(script_dir, "config.json")

# Read JSON configuration file
config <- fromJSON(config_path)

# Helper function to get first non-NA value
get_first_valid <- function(values) {
  return(as.character(na.omit(values)[1]))
}

# Assign variables manually based on Excel data
company <- get_first_valid(config$CompanyName)
company_logo <- get_first_valid(config$CompanyLogo)

report_span_start_time <- as.Date(get_first_valid(config$StartTime))
report_span_end_time <- as.Date(get_first_valid(config$EndTime))
quickstats_database_name <- get_first_valid(config$QuickStatsDatabaseName)

# Takes times from the database (seconds) and divides it by this value. (3600 = hours)
time_unit <- as.numeric(get_first_valid(config$TimeUnit))

downtime_reasons_data_source <- get_first_valid(config$MainDataSource)
exclude_dtm_not_entered <- as.logical(get_first_valid(config$ExcludeDTMNotEntered))
exclude_pntr <- as.logical(get_first_valid(config$ExcludePNTR))


asset_utilisation_lower_limit <- as.integer(get_first_valid(config$UtilisationMin))
efficiency_threshold <- as.integer(get_first_valid(config$EfficiencyMin))
large_num_no_reason_entered_threshold <- as.integer(get_first_valid(config$NoReasonEnteredLimit))
minimum_operator_duration <- as.integer(get_first_valid(config$OperatorDurationMin))
operator_long_shift_duration <- as.integer(get_first_valid(config$OperatorDurationMax))


asset_utilisation_lower_limit <- as.numeric(get_first_valid(config$Value[config$Key == "asset_utilisation_lower_limit"]))
efficiency_threshold <- as.numeric(get_first_valid(config$Value[config$Key == "efficiency_threshold"]))
large_num_no_reason_entered_threshold <- as.numeric(get_first_valid(config$Value[config$Key == "large_num_no_reason_entered_threshold"]))
minimum_operator_duration <- as.numeric(get_first_valid(config$Value[config$Key == "minimum_operator_duration"]))
operator_long_shift_duration <- as.numeric(get_first_valid(config$Value[config$Key == "maximum_operator_duration"]))

# Export Data
output_directory <- get_first_valid(config$OutputDirectory)
output_file_name <- get_first_valid(config$OutputFilename)

sql <- as.logical(get_first_valid(config$OutputToSQL))
excel <- as.logical(get_first_valid(config$OutputToExcel))

by_asset <- as.logical(get_first_valid(config$SeparateReportsByAsset))

include_cover <- as.logical(get_first_valid(config$PageCover))
include_downtime <- as.logical(get_first_valid(config$PageTop10))
include_assets <- as.logical(get_first_valid(config$PageAssetSummary))
include_operators <- as.logical(get_first_valid(config$PageOperatorEfficiencies))
include_machine_learning <- as.logical(get_first_valid(config$PageOperatorPredictions))
include_recommendations <- as.logical(get_first_valid(config$PageRecommendations))
include_warnings <- as.logical(get_first_valid(config$PageWarnings))

include_r_charts <- as.logical(get_first_valid(config$PageRCharts))

# Additional derived values
output_directory_images <- paste0(output_directory, "/", "Images")
# ---------------

# ---------------
# 2. List of assets ----
print(quickstats_database_name)

con <- dbConnect(odbc::odbc(),
                 Driver = "SQL Server",
                 Server = "localhost",
                 Database = quickstats_database_name,
                 Trusted_Connection = "Yes") 

tables <- dbListTables(con)

if (by_asset) {
  unique_assets <- dbGetQuery(con, "SELECT DISTINCT [asset] FROM dbo.[ShiftBased] ORDER BY [asset]")$asset
} else {
  unique_assets <- NA  # Single iteration for all assets
}
# ---------------

# Main Loop ----
for (current_asset in unique_assets) {
  # 2.1. Load Asset Data ------
  asset_filter <- if (!is.na(current_asset) && length(current_asset) == 1) {
    paste0(" AND [asset] = '", current_asset, "'")
  } else {
    ""
  }
  
  selection_clause <- "
  SELECT TOP (10000) 
    [RecNo], 
    [asset number], 
    [asset], 
    [asset group], 
    [name], 
    [start time], 
    [end time], 
    [week number], 
    [duration], 
    [run time], 
    [slow run time], 
    [total run time], 
    [down time], 
    [reason override time], 
    [short stoppage time], 
    [pntr time], 
    [unknown time], 
    [total down time], 
    [total down time (not pntr)], 
    [available time], 
    [product count], 
    [scrap count], 
    [good product count], 
    [target ideal], 
    [target equivalent], 
    [target speed], 
    [actual speed], 
    [utilisation], 
    [availability], 
    [performance], 
    [quality], 
    [oee],
    [longest stop], 
    [longest stop time]"
  
  where_clause <- paste0(
    " WHERE (",
    "([end time] > DATEADD(SECOND, ", report_span_start_time, ", '1970-01-01') ",
    "AND [end time] < DATEADD(SECOND, ", report_span_end_time, ", '1970-01-01')) ",
    "OR ([start time] > DATEADD(SECOND, ", report_span_start_time, ", '1970-01-01') ",
    "AND [start time] < DATEADD(SECOND, ", report_span_end_time, ", '1970-01-01')))",
    asset_filter, ";"
  )
  
  if (by_asset) {
    unique_assets <- dbGetQuery(con, "SELECT DISTINCT [asset] FROM dbo.[ShiftBased]")
  } else {
    unique_assets <- data.frame(asset = NA)  # Placeholder
  }
  
  operator_data <- dbGetQuery(con, paste0(selection_clause, " FROM dbo.[OperatorBased]", where_clause))
  product_data <- dbGetQuery(con, paste0(selection_clause, " FROM dbo.[ProductBased]", where_clause))
  shift_data <- dbGetQuery(con, paste0(selection_clause, " FROM dbo.[ShiftBased]", where_clause))
  # ---------------

  # ---------------
  # 2.2. Load Downtime Data ----
  general_downtime_select_clause <- "
    SELECT 
      [duration],
      [run time],
      [slow run time],
      [total run time],
      [down time],
      [reason override time],
      [short stoppage time],
      [pntr time],
      [unknown time],
      [total down time],
      [total down time (not pntr)],
      [available time],
      [product count],
      [scrap count],
      [good product count],
      [target ideal],
      [target equivalent],
      [target speed],
      [actual speed],
      [utilisation],
      [availability],
      [performance],
      [quality],
      [oee]
  "
  
  general_downtime_query <- paste(general_downtime_select_clause, "FROM ", downtime_reasons_data_source, " WHERE [oee] IS NOT NULL")
  
  general_downtime_source_data <- dbGetQuery(con, general_downtime_query)
  
  # Prepare the column names and format them
  column.types <- dbGetQuery(con, "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='DailyBased'")
  
  ct <- column.types %>%
    mutate(cml = case_when(
      is.na(CHARACTER_MAXIMUM_LENGTH) ~ 10,
      CHARACTER_MAXIMUM_LENGTH == -1 ~ 100000,
      TRUE ~ as.double(CHARACTER_MAXIMUM_LENGTH)
    )) %>%
    arrange(cml) %>%
    pull(COLUMN_NAME)
  
  fields <- paste(paste0("[", ct, "]"), collapse=", ")
  query <- paste("SELECT", fields, "FROM", downtime_reasons_data_source, where_clause)
  daily_data <- dbGetQuery(con, query)
  
  # Dynamically extract the relevant reason and time columns from the database
  reason_columns <- dbGetQuery(con, paste("
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '", downtime_reasons_data_source, "'
      AND COLUMN_NAME LIKE 'top reason%'
      AND COLUMN_NAME NOT LIKE '%time'
    ORDER BY COLUMN_NAME
  ", sep = "")) %>%
    pull(COLUMN_NAME) %>%
    paste0("[", ., "]")
  
  time_columns <- dbGetQuery(con, paste("
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '", downtime_reasons_data_source, "'
      AND COLUMN_NAME LIKE 'top reason% time'
    ORDER BY COLUMN_NAME
  ", sep = "")) %>%
    pull(COLUMN_NAME) %>%
    paste0("[", ., "]")
  
  if(length(reason_columns) != length(time_columns)) {
    stop("The number of reason columns does not match the number of time columns.")
  }
  
  union_all_queries <- mapply(function(reason_col, time_col) {
    paste0("SELECT ", reason_col, " AS Reason, ", time_col, " AS [TimeInSeconds] FROM [", downtime_reasons_data_source, "]")
  }, reason_columns, time_columns)
  
  databases <- dbGetQuery(con, "
  SELECT name
  FROM sys.databases
  WHERE name LIKE 'prodigy_configuration_dtl_%' AND state_desc = 'ONLINE';
") %>%
    pull(name)
  
  # Initialize an empty vector to store databases with the 'Reasons' table
  databases_with_reasons <- character()
  
  # Loop over each database to check if the 'Reasons' table exists
  for (db in databases) {
    query <- paste0("
    IF EXISTS (SELECT 1 FROM ", db, ".INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Reasons')
    BEGIN
        SELECT '", db, "' AS DatabaseName;
    END;
  ")
    
    result <- dbGetQuery(con, query)
    
    # If the query returns a result, add the database name to the list
    if (nrow(result) > 0) {
      databases_with_reasons <- c(databases_with_reasons, result$DatabaseName)
    }
  }
  
  union_all_queries2 <- sapply(databases_with_reasons, function(db) {
    paste0(
      "SELECT Reason ",
      "FROM [", db, "].[dbo].[Reasons] ",
      "WHERE pntr = 1"
    )
  })
  
  # Construct the final downtime query dynamically
  downtime_query <- paste(
    "WITH DowntimeReasons AS (", 
    paste(union_all_queries, collapse = " UNION ALL "), 
    "), ReasonsList AS (",
    paste(union_all_queries2, collapse = " UNION ALL "), 
    ") ",
    "SELECT ",
    "COUNT(*) AS InstanceCount",
    ", SUM(TimeInSeconds) / ", time_unit, " AS TotalTimeInHours",
    ", AVG(TimeInSeconds) / ", time_unit, " AS MeanTime",
    ", MIN(TimeInSeconds) / ", time_unit, " AS MinTime",
    ", MAX(TimeInSeconds) / ", time_unit, " AS MaxTime",
    ", STDEVP(TimeInSeconds) / ", time_unit, " AS StdDevTime",
    ", CASE ",
    "WHEN dr.Reason IN (SELECT Reason FROM ReasonsList) THEN 1 ",
    "ELSE 0 ",
    "END AS PNTR ",
    ", Reason", # The Reason field must be last in the select statement due to odd ODBC Library bug
    "FROM DowntimeReasons dr ",
    "WHERE dr.Reason IS NOT NULL ",
    
    if (exclude_dtm_not_entered) {
      "AND dr.Reason NOT IN ('DTM Info Not Entered') "
    } else {
      ""
    },
    
    if (exclude_pntr) {
      paste(
        "AND NOT EXISTS (",
        "SELECT 1 ",
        "FROM ReasonsList rl ",
        "WHERE rl.Reason = dr.Reason",
        ") "
      )
    } else {
      ""
    },
    
    "GROUP BY dr.Reason ",
    "ORDER BY InstanceCount DESC"
  )
  
  print(downtime_query)
  
  downtime_source_data <- dbGetQuery(con, downtime_query)
  
  dbDisconnect(con)
  # ---------------
  
  # ---------------
  # 3. Top downtime reasons ----
  downtime_summary <- downtime_source_data %>%
    select(Reason, PNTR, InstanceCount, TotalTimeInHours, MeanTime, MinTime, MaxTime, StdDevTime)
  
  downtime_reasons_data <- daily_data %>%
    select(starts_with("top reason")) %>%
    mutate(across(starts_with("top reason"), as.character)) %>%
    pivot_longer(cols = everything(),
                 names_to = "reason",
                 names_pattern = "top reason (\\d+)") %>%
    rename(reason_value = value) %>%
    left_join(daily_data %>%
                select(starts_with("top reason") & ends_with("time")) %>%
                pivot_longer(cols = everything(),
                             names_to = "reason",
                             names_pattern = "top reason (\\d+) time") %>%
                rename(time = value), 
              by = "reason") %>%
    filter(!is.na(reason_value) & !is.na(time)) %>%
    group_by(reason_value) %>%
    summarise(
      TotalCount = n(),
      TotalTimeInHours = sum(time, na.rm = TRUE) / time_unit
    ) %>%
    arrange(desc(TotalTimeInHours))
  
  final_reasons <- downtime_summary %>%
    mutate(Reason = if_else(row_number() <= 10, Reason, "Other")) %>%
    group_by(Reason) %>%
    summarise(
      InstanceCount = sum(InstanceCount),
      TotalTime = sum(TotalTimeInHours)
    ) %>%
    arrange(desc(InstanceCount))
  
  # 4. Analyse operator efficiency ----
  operator_efficiency <- operator_data %>%
    group_by(`name`) %>%
    summarize(
      TotalDurationHrs = sum(`duration`, na.rm = TRUE)/ time_unit,
      TotalRunTimeHrs = sum(`run time`, na.rm = TRUE)/ time_unit,
      DowntimeHrs = sum(`down time`, na.rm = TRUE)/ time_unit,
      ShiftsWorked = n(),
      Efficiency = TotalRunTimeHrs / TotalDurationHrs * 100
    ) %>%
    filter(TotalDurationHrs > minimum_operator_duration) %>%
    arrange(desc(TotalDurationHrs))
  
  operator_efficiency_by_group <- operator_data %>%
    group_by(`asset group`, `name`) %>%
    summarize(
      TotalDurationHrs = sum(`duration`, na.rm = TRUE) / time_unit,
      TotalRunTimeHrs = sum(`run time`, na.rm = TRUE) / time_unit,
      DowntimeHrs = sum(`down time`, na.rm = TRUE) / time_unit,
      ShiftsWorked = n(),
      Efficiency = TotalRunTimeHrs / TotalDurationHrs * 100
    ) %>%
    filter(TotalDurationHrs > minimum_operator_duration) %>%
    arrange(`asset group`, desc(TotalDurationHrs))
  
  # 5. Asset summary----
  sub_shift_data <- shift_data %>%
    mutate(across(c(`duration`, `run time`, `down time`, `actual speed`, `target speed`,
                    `product count`, `scrap count`, `good product count`), as.numeric, .names = "cleaned_{col}"))
  
  asset_utilisation <- sub_shift_data %>%
    group_by(`asset`) %>%
    summarize(
      TotalDuration = sum(`cleaned_duration`, na.rm = TRUE) / time_unit,
      RunTime = sum(`cleaned_run time`, na.rm = TRUE) / time_unit,
      Downtime = sum(`cleaned_down time`, na.rm = TRUE) / time_unit,
      Availability = (TotalDuration - Downtime) / TotalDuration * 100,
      TotalProductCount = sum(`cleaned_product count`, na.rm = TRUE),
      ScrapCount = sum(`cleaned_scrap count`, na.rm = TRUE),
      GoodProductCount = sum(`cleaned_good product count`, na.rm = TRUE)
    ) %>%
    mutate(
      Utilisation = RunTime / TotalDuration * 100,
      Quality = GoodProductCount / TotalProductCount * 100,
      OEE = Utilisation * Availability * Quality / 10000
    ) %>%
    arrange(desc(Utilisation))
  
  # ---------------
  
  # ---------------
  # 6. Machine Learning Predictions ----
  # Prepare the data for training
  operator_efficiency_clean <- operator_efficiency %>%
    filter(!is.na(Efficiency)) %>%
    select(TotalDurationHrs, DowntimeHrs, ShiftsWorked, Efficiency)
  
  if (nrow(operator_efficiency_clean) >= 5 && length(unique(operator_efficiency_clean$Efficiency)) > 1) {
    
    set.seed(8)  # For reproducibility
    
    rf_model <- randomForest(
      Efficiency ~ TotalDurationHrs + DowntimeHrs + ShiftsWorked,
      data = operator_efficiency_clean, 
      importance = TRUE, 
      ntree = 100
    )
    
    # Predict efficiency
    operator_efficiency_clean$PredictedEfficiency <- predict(rf_model, operator_efficiency_clean)
    
    operator_efficiency_with_pred <- operator_efficiency %>%
      filter(!is.na(Efficiency)) %>%
      select(name, TotalDurationHrs, DowntimeHrs, ShiftsWorked, Efficiency) %>%
      mutate(PredictedEfficiency = operator_efficiency_clean$PredictedEfficiency)
    
  } else {
    warning("Not enough data or variation in Efficiency to train random forest model.")
    
    operator_efficiency_clean$PredictedEfficiency <- NA
    
    operator_efficiency_with_pred <- operator_efficiency %>%
      filter(!is.na(Efficiency)) %>%
      select(name, TotalDurationHrs, DowntimeHrs, ShiftsWorked, Efficiency) %>%
      mutate(PredictedEfficiency = NA)
  }
  
  # ---------------
  
  # ---------------
  # 7. Find Warnings ----
  # Create a data frame to collect warnings
  warnings <- data.frame(
    Issue = character(),
    Details = character(),
    stringsAsFactors = FALSE
  )
  
  # Check for missing values
  for (df_name in c("operator_data", "product_data", "shift_data")) {
    df <- get(df_name)
    if (any(is.na(df))) {
      warnings <- rbind(warnings, data.frame(
        Issue = "Missing Values",
        Details = paste("Missing values found in", df_name),
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # Check for incorrect values (negative runtime, downtime, etc.)
  incorrect_value_checks <- list(
    "Negative Run Time" = function(df) any(df$`run time` < 0, na.rm = TRUE),
    "Negative Downtime" = function(df) any(df$`down time` < 0, na.rm = TRUE),
    "Negative Duration" = function(df) any(df$`duration` < 0, na.rm = TRUE)
  )
  
  for (df_name in c("operator_data", "product_data", "shift_data")) {
    df <- get(df_name)
    for (check_name in names(incorrect_value_checks)) {
      check <- incorrect_value_checks[[check_name]]
      if (check(df)) {
        warnings <- rbind(warnings, data.frame(
          Issue = check_name,
          Details = paste(check_name, "found in", df_name),
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Check for overly long periods (greater than 12 hours)
  for (df_name in c("operator_data", "shift_data")) {
    df <- get(df_name)
    if ("start time" %in% names(df) & "end time" %in% names(df)) {
      long_periods <- df %>%
        mutate(
          PeriodHours = as.numeric(difftime(`end time`, `start time`, units = "hours"))
        ) %>%
        filter(PeriodHours > operator_long_shift_duration)
      
      if (nrow(long_periods) > 0) {
        warnings <- rbind(warnings, data.frame(
          Issue = "Overly Long Periods",
          Details = paste("Found", nrow(long_periods), "instances of periods over", operator_long_shift_duration, "hours in", df_name),
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Check for deleted downtime reasons
  deleted_reason_count <- sum(grepl("Deleted", downtime_summary$Reason, ignore.case = TRUE))
  
  if (deleted_reason_count > 0) {
    warnings <- rbind(warnings, data.frame(
      Issue = "Using deleted downtime reasons",
      Details = paste("Found", deleted_reason_count, "instances of deleted downtime reasons being used"),
      stringsAsFactors = FALSE
    ))
  }
  
  # Define a list of checks for OEE, Quality, Performance, Availability, and Utilisation > 100% or < 0
  metrics_check <- list(
    "OEE" = function(df) list(over = sum(df$oee > 100, na.rm = TRUE), under = sum(df$oee < 0, na.rm = TRUE)),
    "Quality" = function(df) list(over = sum(df$quality > 100, na.rm = TRUE), under = sum(df$quality < 0, na.rm = TRUE)),
    "Performance" = function(df) list(over = sum(df$performance > 100, na.rm = TRUE), under = sum(df$performance < 0, na.rm = TRUE)),
    "Availability" = function(df) list(over = sum(df$availability > 100, na.rm = TRUE), under = sum(df$availability < 0, na.rm = TRUE)),
    "Utilisation" = function(df) list(over = sum(df$utilisation > 100, na.rm = TRUE), under = sum(df$utilisation < 0, na.rm = TRUE))
  )
  
  # Initialize variables to store the counts for each metric
  metric_counts <- list(
    "OEE" = list(over = 0, under = 0),
    "Quality" = list(over = 0, under = 0),
    "Performance" = list(over = 0, under = 0),
    "Availability" = list(over = 0, under = 0),
    "Utilisation" = list(over = 0, under = 0)
  )
  
  # Check for instances where any metrics exceed 100% or are less than 0 across datasets
  for (check_name in names(metrics_check)) {
    for (df_name in c("operator_data", "product_data", "shift_data")) {
      # Get the dataset by name
      df <- get(df_name)
      
      # Apply the check for the current metric and update the respective counts
      counts <- metrics_check[[check_name]](df)
      
      metric_counts[[check_name]]$over <- metric_counts[[check_name]]$over + counts$over
      metric_counts[[check_name]]$under <- metric_counts[[check_name]]$under + counts$under
    }
    
    # If any record exceeds 100% or is less than 0, add a warning
    if (metric_counts[[check_name]]$over > 0) {
      warnings <- rbind(warnings, data.frame(
        Issue = paste(check_name, "- Invalid Value"),
        Details = paste(check_name, "exceeded 100% a total of", metric_counts[[check_name]]$over, "times"),
        stringsAsFactors = FALSE
      ))
    }
    
    if (metric_counts[[check_name]]$under > 0) {
      warnings <- rbind(warnings, data.frame(
        Issue = paste(check_name, "- Invalid Value"),
        Details = paste(check_name, "was less than 0% a total of", metric_counts[[check_name]]$under, "times"),
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # If no warnings, add a message indicating no issues found
  if (nrow(warnings) == 0) {
    warnings <- data.frame(
      Issue = "No Issues",
      Details = "No data issues found.",
      stringsAsFactors = FALSE
    )
  }
  
  # 8. Actionable Recommendations ----
  # 8.0. Initialise an empty data frame to store recommendations
  recommendations <- data.frame(
    Action = character(),
    Recommendation = character(),
    Severity = character(),
    Category = character(),
    stringsAsFactors = FALSE
  )
  
  # 8.1. Check for assets with low utilisation
  low_utilisation_assets <- asset_utilisation %>%
    filter(Utilisation < asset_utilisation_lower_limit) %>%
    arrange(Utilisation)
  
  if (nrow(low_utilisation_assets) > 0) {
    for (i in 1:nrow(low_utilisation_assets)) {
      recommendations <- rbind(recommendations, data.frame(
        Action = paste("Increase utilisation of asset", low_utilisation_assets$asset[i]),
        Recommendation = "Consider redistributing shifts or reassigning operators to maximise asset utilisation",
        Severity = "Medium",
        Category = "Asset Usage"
      ))
    }
  }
  
  # 8.2. Operator efficiency
  # Filter out operators
  filtered_operator_efficiency_by_group <- operator_efficiency_by_group %>%
    filter(Efficiency > 5, Efficiency < 95)
  
  # Calculate the average efficiency by asset group
  average_efficiency_by_group <- filtered_operator_efficiency_by_group %>%
    group_by(`asset group`) %>%
    summarize(AverageEfficiency = mean(Efficiency, na.rm = TRUE))
  
  operator_efficiency_comparison <- filtered_operator_efficiency_by_group %>%
    left_join(average_efficiency_by_group, by = "asset group") %>%
    mutate(
      EfficiencyDifference = AverageEfficiency - Efficiency,
      EfficiencyPercentageDifference = (EfficiencyDifference / AverageEfficiency) * 100
    )
  
  # Identify operators with efficiency below the threshold
  filtered_efficiency_operators <- operator_efficiency_comparison %>%
    filter(EfficiencyPercentageDifference > efficiency_threshold) %>%
    arrange(`asset group`, EfficiencyPercentageDifference)
  
  # Generate recommendations for low-efficiency operators
  if (nrow(filtered_efficiency_operators) > 0) {
    for (i in 1:nrow(filtered_efficiency_operators)) {
      # Determine severity based on percentage difference
      severity <- ifelse(
        filtered_efficiency_operators$EfficiencyPercentageDifference[i] > efficiency_threshold*2, "High",
        ifelse(
          filtered_efficiency_operators$EfficiencyPercentageDifference[i] > efficiency_threshold*1.5, "Medium",
          "Low"
        )
      )
      
      recommendations <- rbind(recommendations, data.frame(
        Action = paste("Improve efficiency of operator", filtered_efficiency_operators$name[i], "in asset group", filtered_efficiency_operators$`asset group`[i]),
        Recommendation = paste("Provide additional training, shift optimisation, or performance monitoring for operator", filtered_efficiency_operators$name[i], "to improve their efficiency relative to the group average."),
        Severity = severity,
        Category = "Operators"
      ))
    }
  }
  
  # 8.3. Identify assets or products with high scrap rate
  high_scrap_assets <- asset_utilisation %>%
    mutate(ScrapRate = ScrapCount / TotalProductCount * 100) %>%
    filter(ScrapRate > 10) %>%
    arrange(desc(ScrapRate))
  
  if (nrow(high_scrap_assets) > 0) {
    for (i in 1:nrow(high_scrap_assets)) {
      recommendations <- rbind(recommendations, data.frame(
        Action = paste("Reduce scrap for asset", high_scrap_assets$asset[i]),
        Recommendation = "Investigate root causes of scrap and implement corrective actions.",
        Severity = "Medium",
        Category = "Asset Usage"
      ))
    }
  }
  
  # 8.4. Check Number of 'No Reason Entered' downtime records
  if ("No Reason Entered" %in% downtime_summary$Reason) {
    no_reason_count <- downtime_summary$InstanceCount[downtime_summary$Reason == "No Reason Entered"]
    
    severity <- if (no_reason_count > large_num_no_reason_entered_threshold*2) {
      "High"
    } else if (no_reason_count > large_num_no_reason_entered_threshold*1.5) {
      "Medium"
    } else if (no_reason_count > large_num_no_reason_entered_threshold) {
      "Low"
    } else {
      NULL
    }
    
    if (no_reason_count > large_num_no_reason_entered_threshold) {
      recommendations <- rbind(recommendations, data.frame(
        Action = paste("Analyse and reduce the instances of 'No Reason Entered' in downtime logs to improve data accuracy."),
        Recommendation = "Encourage operators to provide reasons by highlighting the importance of detailed downtime data.",
        Severity = "Medium",
        Category = "Data Entry"
      ))
    }
  }
  
  # 8.5. Performance over 100%
  if (metric_counts[["Performance"]]$over > 0 || metric_counts[["Performance"]]$under > 0) {
    recommendations <- rbind(recommendations, data.frame(
      Action = paste("Analyse and review asset target speeds to improve performance accuracy."),
      Recommendation = paste("Consider adjusting asset target speeds based on performance data to better optimise efficiency and accuracy."),
      Severity = "Medium",
      Category = "Data Entry"
    ))
  }
  
  # 8.6. Warnings
  severity <- if (nrow(warnings) > 10) {
    "High"
  } else if (nrow(warnings) > 5) {
    "Medium"
  } else if (nrow(warnings) > 3) {
    "Low"
  } else {
    NULL
  }
  
  if (!is.null(severity)) {
    recommendations <- rbind(recommendations, data.frame(
      Action = "Improve data quality to reduce warnings",
      Recommendation = "Implement stricter data entry protocols to reduce warnings and errors.",
      Severity = severity,
      Category = "Data Entry"
    ))
  }
  # ---------------
  
  # ---------------
  # 9. Export Data - Excel ----
  included_sections <- c()
  included_descriptions <- c()
  
  if (include_downtime) {
    included_sections <- c(included_sections, "Downtime Reasons Statistics")
    included_descriptions <- c(included_descriptions, "Analysis of machine downtime causes and their impact on production efficiency.")
  }
  if (include_assets) {
    included_sections <- c(included_sections, "Asset Summary")
    included_descriptions <- c(included_descriptions, "Overview of asset status, usage, and performance metrics across the system.")
  }
  if (include_operators) {
    included_sections <- c(included_sections, "Operator Efficiency")
    included_descriptions <- c(included_descriptions, "Evaluation of operator performance, including productivity factors.")
  }
  if (include_machine_learning) {
    included_sections <- c(included_sections, "AI Model Predictions")
    included_descriptions <- c(included_descriptions, "Predictions generated by AI models, forecasting potential issues or trends in operations.")
  }
  if (include_recommendations) {
    included_sections <- c(included_sections, "Actionable Recommendations")
    included_descriptions <- c(included_descriptions, "Suggested actions based on data analysis, aiming to improve performance or prevent issues.")
  }
  if (include_warnings) {
    included_sections <- c(included_sections, "Warnings")
    included_descriptions <- c(included_descriptions, "Alerts about issues found within the data used.")
  }
  
  if (excel) {
    output_file <- paste0(output_directory, "/", current_asset, "/", output_file_name, ".xlsx")
    
    output_dir <- dirname(output_file)
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)  # Creates directory including any missing parent folders
    }
    
    wb <- createWorkbook()
    
    
    if (include_cover) {
      addWorksheet(wb, "Cover Page")
      
      # Write a title and some content on the cover page (horizontal arrangement using a row vector)
      writeData(wb, "Cover Page", x = c("Managerial Report"), startCol = 1, startRow = 1)
      writeData(wb, "Cover Page", x = c("Generated on:", format(Sys.Date(), "%d-%m-%Y")), startCol = 1, startRow = 2)
      writeData(wb, "Cover Page", x = c("Prepared by:", company), startCol = 1, startRow = 4)
      
      writeData(wb, "Cover Page", x = c("Date Range:", paste0(report_span_start_time, " to ", report_span_end_time)), startCol = 2, startRow = 2)
      
      writeData(wb, "Cover Page", x = c("This document includes:", company), startCol = 1, startRow = 8)
      for (i in seq_along(included_sections)) {
        writeData(wb, "Cover Page", x = included_sections[i], startCol = 1, startRow = 8 + i)
        writeData(wb, "Cover Page", x = included_descriptions[i], startCol = 2, startRow = 8 + i)
      }
      
      # Adjust column widths
      setColWidths(wb, "Cover Page", cols = 1, widths = c(40))
      setColWidths(wb, "Cover Page", cols = 2, widths = c(100))
      
      # Adjust row heights
      setRowHeights(wb, "Cover Page", rows = 1:1, heights = c(50))
      
      # Format the Cover Page (bold, large font size, center alignment)
      addStyle(wb, "Cover Page", style = createStyle(fontSize = 24, fontColour = "white", 
                                                     halign = "center", valign = "center", 
                                                     textDecoration = "bold"), 
               rows = 1, cols = 1:2)
      
      # Apply a style for the other content
      addStyle(wb, "Cover Page", style = createStyle(fontSize = 14, fontColour = "black", 
                                                     halign = "left", valign = "center"), 
               rows = 2:2, cols = 1:2)
      
      addStyle(wb, "Cover Page", style = createStyle(fontSize = 14, fontColour = "black", 
                                                     halign = "left", valign = "center"), 
               rows = 4:4, cols = 1:1)
      
      addStyle(wb, "Cover Page", style = createStyle(fontSize = 14, fontColour = "black", 
                                                     halign = "left", valign = "center"), 
               rows = 8:8, cols = 1:1)
      
      if (!is.null(company_logo) && company_logo != "" && file.exists(company_logo)) {
        insertImage(wb, "Cover Page", file = company_logo, 
                    startRow = 1, startCol = 2, width = 100*3, height = 56*3, 
                    unit = "px")
      }
    }
    
    
    
    if (include_downtime) {
      addWorksheet(wb, "Downtime Reasons Statistics")
      writeData(wb, "Downtime Reasons Statistics", downtime_summary)
      
      addWorksheet(wb, "Top 10 Downtime Reasons")
      writeData(wb, "Top 10 Downtime Reasons", final_reasons)
    }
    
    if (include_assets) {
      addWorksheet(wb, "Asset Summary")
      writeData(wb, "Asset Summary", asset_utilisation)
    }
    
    if (include_operators) {
      addWorksheet(wb, "Operator Efficiency (Overall)")
      writeData(wb, "Operator Efficiency (Overall)", operator_efficiency)
      
      addWorksheet(wb, "Operator Efficiency (Asset)")
      writeData(wb, "Operator Efficiency (Asset)", operator_efficiency_by_group)
    }
    
    if (include_machine_learning) {
      addWorksheet(wb, "Operator Efficiency Predictions")
      writeData(wb, "Operator Efficiency Predictions", operator_efficiency_with_pred)
    }
    
    if (include_recommendations) {
      addWorksheet(wb, "Actionable Recommendations")
      writeData(wb, "Actionable Recommendations", recommendations)
    }
    
    if (include_warnings) {
      addWorksheet(wb, "Warnings")
      writeData(wb, "Warnings", warnings)
    }
    
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  # 10. Export Data - SQL ----
  if (sql) {
    con <- dbConnect(odbc::odbc(),
                     Driver = "SQL Server",
                     Server = "localhost",
                     Trusted_Connection = "Yes")
    
    # Create the database if it does not exist
    query <- paste0("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '", output_file_name, "')
                      BEGIN
                          CREATE DATABASE ", output_file_name, ";
                      END;")
    dbExecute(con, query)
    dbDisconnect(con)
    
    
    con <- dbConnect(odbc::odbc(),
                     Driver = "SQL Server",
                     Server = "localhost",
                     Database = output_file_name,
                     Trusted_Connection = "Yes")
    
    dbWriteTable(con, "Downtime_Statistics", downtime_summary, overwrite = TRUE)
    dbWriteTable(con, "Top_10_Downtime_Reasons", final_reasons, overwrite = TRUE)
    dbWriteTable(con, "Operator_Efficiency_Overall", operator_efficiency, overwrite = TRUE)
    dbWriteTable(con, "Operator_Efficiency_Asset", operator_efficiency_by_group, overwrite = TRUE)
    dbWriteTable(con, "Asset_Summary", asset_utilisation, overwrite = TRUE)
    dbWriteTable(con, "Operator_Efficiency_Predictions", operator_efficiency_with_pred, overwrite = TRUE)
    dbWriteTable(con, "Actionable_Recommendations", recommendations, overwrite = TRUE)
    dbWriteTable(con, "Warnings", warnings, overwrite = TRUE)
    
    dbDisconnect(con)
  }
  # ---------------
  
  # ---------------
  # 11. Chart Creation ----
  if (!dir.exists(output_directory_images)) {
    dir.create(output_directory_images, recursive = TRUE)
  }
  
  # Downtime Reasons Bar Chart - Top 10 Downtime Reasons
  top_downtime_reasons <- downtime_summary %>%
    top_n(10, TotalTimeInHours)
  
  plot1 <- ggplot(top_downtime_reasons, aes(x = reorder(Reason, TotalTimeInHours), y = TotalTimeInHours, fill = Reason)) +
    geom_bar(stat = "identity", show.legend = FALSE) +
    coord_flip() +
    labs(
      title = "Chart 1 - Top 10 Downtime Reasons",
      x = "Reason",
      y = "Total Downtime (Hours)"
    ) +
    theme_minimal() +
    theme(axis.text.y = element_text(size = 10))
  
  # Downtime Reasons Pie Chart - Top 10 Downtime Reasons
  sorted_top_downtime_reasons <- top_downtime_reasons %>%
    arrange(desc(TotalTimeInHours)) %>%
    mutate(Reason = factor(Reason, levels = Reason))
  
  # Plotting the pie chart with updated legend text size
  plot2 <- ggplot(sorted_top_downtime_reasons, aes(x = "", y = TotalTimeInHours, fill = Reason)) +
    geom_bar(stat = "identity", width = 1, show.legend = TRUE) +
    coord_polar(theta = "y") +
    labs(title = "Chart 2 - Top 10 Downtime Reasons Distribution") +
    theme_void() +
    theme(legend.title = element_blank(),
          legend.text = element_text(size = 12))
  
  # Operator Efficiency Bar Chart
  # This chart remains as is because it was rated as very good.
  plot3 <- ggplot(operator_efficiency, aes(x = reorder(`name`, Efficiency), y = Efficiency, fill = Efficiency)) +
    geom_bar(stat = "identity", show.legend = FALSE) +
    coord_flip() +
    labs(
      title = "Chart 3 - Operator Efficiency (%)",
      x = "Operator Name",
      y = "Efficiency (%)"
    ) +
    theme_minimal() +
    theme(axis.text.y = element_text(size = 10))
  
  # Operator Efficiency by Group
  plot4 <- ggplot(operator_efficiency_by_group, aes(x = `asset group`, y = Efficiency, fill = `name`)) +
    geom_bar(stat = "identity") +
    labs(
      title = "Chart 4 - Operator Efficiency by Group",
      x = "Asset Group",
      y = "Efficiency (%)"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  # Asset Utilisation vs. Availability - Increase Point Size
  plot5 <- ggplot(asset_utilisation, aes(x = Availability, y = Utilisation, color = `asset`)) +
    geom_point(size = 3) +  # Increased point size for better visibility
    labs(
      title = "Chart 5 - Asset Utilisation vs Availability",
      x = "Availability (%)",
      y = "Utilisation (%)"
    ) +
    theme_minimal()
  
  # Total Product Count by Asset - Limit Product Count to Top 10 Assets
  top_assets <- asset_utilisation %>%
    top_n(10, TotalProductCount)
  
  plot6 <- ggplot(top_assets, aes(x = reorder(`asset`, TotalProductCount), y = TotalProductCount, fill = `asset`)) +
    geom_bar(stat = "identity", show.legend = FALSE) +
    coord_flip() +
    labs(
      title = "Chart 6 - Top 10 Assets by Total Product Count",
      x = "Asset",
      y = "Total Product Count"
    ) +
    theme_minimal() +
    theme(axis.text.y = element_text(size = 10))
  
  # Efficiency Trend - Line chart showing the trend of efficiency by shift
  plot7 <- ggplot(product_data, aes(x = `start time`, y = oee, group = `asset`, color = `asset`)) +
    geom_line() + 
    geom_point() +  # Adding points for visibility
    labs(
      title = "Chart 7 - OEE Over Time for all Assets",
      x = "Start Time",
      y = "OEE (%)"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    coord_cartesian(ylim = c(0, 100))
  
  # Metrics Boxplots
  metrics_long <- shift_data %>%
    select(performance, quality, utilisation, availability, oee) %>%
    pivot_longer(cols = everything(), names_to = "Metric", values_to = "Value")
  
  metrics_long_filtered <- metrics_long %>%
    filter(Value != 0)
  
  # Calculate dynamic y-axis limits based on the filtered data
  y_limits <- range(metrics_long_filtered$Value, na.rm = TRUE)
  
  # Create the combined boxplot with jitter
  plot8 <- ggplot(metrics_long_filtered, aes(x = Metric, y = Value, fill = Metric)) +
    geom_boxplot(outlier.shape = 16, alpha = 0.6) + # Keep outliers as points
    #geom_jitter(width = 0.2, alpha = 0.3) + # Add jitter for individual data points
    labs(
      title = "Chart 8 - Metrics Boxplots",
      x = "Metric",
      y = "Percentage"
    ) +
    scale_y_continuous(labels = scales::comma, limits = c(-50, 150)) +
    theme_minimal() +
    theme(
      legend.position = "none",
      axis.text.x = element_text(size = 10, angle = 45, hjust = 1)
    )
  
  
  # Correlation Chart - Correlation Matrix with OEE
  # Select only numeric fields for correlation calculation
  correlation_data <- general_downtime_source_data %>%
    select(
      duration,
      `run time`,
      `slow run time`,
      `total run time`,
      `down time`,
      `reason override time`,
      `short stoppage time`,
      `pntr time`,
      `unknown time`,
      `total down time`,
      `total down time (not pntr)`,
      `available time`,
      `product count`,
      `scrap count`,
      `good product count`,
      `target ideal`,
      `target equivalent`,
      `target speed`,
      `actual speed`,
      utilisation,
      availability,
      performance,
      quality,
      oee
    )
  
  # Remove columns with zero variance
  correlation_data <- correlation_data %>%
    select(where(~ sd(.) > 0))
  
  # Handle missing values by removing rows with NA values
  correlation_data <- correlation_data %>%
    drop_na()
  
  correlation_matrix <- cor(correlation_data, use = "complete.obs")
  
  png(file.path(output_directory_images, "Chart_9_Correlation_Matrix_with_OEE.png"), width = 800, height = 600)
  
  corrplot(correlation_matrix, method = "circle", type = "full",
           order = "hclust", addCoef.col = "black",
           title = "Chart 9 - Correlation Matrix with OEE", mar = c(0, 0, 3, 0),
           tl.cex = 0.8,
           cl.cex = 0.8,
           number.cex = 0.6,
           tl.col = "black",
           diag = FALSE)
  
  dev.off()
  
  # 12. Chart Output ----
  ggsave(paste0(output_directory_images, "/Chart_1_Top_10_Downtime_Reasons.png"), plot = plot1, width = 8, height = 6)
  print(plot1)
  
  ggsave(paste0(output_directory_images, "/Chart_2_Top_10_Downtime_Reason_Distribution.png"), plot = plot2, width = 8, height = 6)
  print(plot2)
  
  ggsave(paste0(output_directory_images, "/Chart_3_Operator_Efficiency.png"), plot = plot3, width = 8, height = 6)
  print(plot3)
  
  ggsave(paste0(output_directory_images, "/Chart_4_Operator_Efficiency_by_Group.png"), plot = plot4, width = 8, height = 6)
  print(plot4)
  
  ggsave(paste0(output_directory_images, "/Chart_5_Asset_Utilisation_vs_Availability.png"), plot = plot5, width = 8, height = 6)
  print(plot5)
  
  ggsave(paste0(output_directory_images, "/Chart_6_Top_10_Assets_by_Product_Count.png"), plot = plot6, width = 8, height = 6)
  print(plot6)
  
  ggsave(paste0(output_directory_images, "/Chart_7_OEE_Over_Time.png"), plot = plot7, width = 8, height = 6)
  print(plot7)
  
  ggsave(paste0(output_directory_images, "/Chart_8_Metrics_Boxplot.png"), plot = plot8, width = 8, height = 6)
  print(plot8)
  
  # plot9 saved as PNG earlier during creation
  # ---------------
}
# ---------------

# 13. Open Output Directory ----
shell.exec(output_directory)
# ---------------
