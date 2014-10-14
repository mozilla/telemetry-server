is.installed <- function(mypkg){
  is.element(mypkg, installed.packages()[,1])
}

if (!is.installed("dplyr"))
  install.packages("dplyr", repos="http://cran.rstudio.com/")

if (!is.installed("caret"))
  install.packages("caret", repos="http://cran.rstudio.com/")

library(caret)
library(dplyr)

select <- dplyr::select
undot <- function (x) gsub("\\.", " ", x)
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

addon_plot <- function(df) {
  ggplot(df, aes(factor(addon, levels=rev(unique(addon))), Estimate)) +
    geom_point() +
    geom_errorbar(width=.1, aes(ymin=Estimate-Error, ymax=Estimate+Error)) +
    coord_flip() +
    scale_y_continuous(name="Startup time overhead in ms") + scale_x_discrete(name ="Add-on") +
    theme_bw()
}

extract <- function(model) {
  coefs <- data.frame(coef(summary(model)))
  coefs %.%
    mutate(addon = trim(undot(row.names(coefs)))) %.%
    select(Estimate, Error=Std..Error, t=t.value, Pr=Pr...t.., addon) %.%
    arrange(-Estimate) %.% filter(Estimate > 0, Pr < 0.01)
}

extract_log <- function(model) {
  coefs <- data.frame(coef(summary(model)))
  coefs %.%
    mutate(addon = trim(undot(row.names(coefs)))) %.%
    select(Estimate, Error=Std..Error, t=t.value, Pr=Pr...t.., addon) %.%
    arrange(-Estimate) %.% filter(Estimate > 0, addon != "(Intercept)", Pr < 0.01) %.%
    mutate(Estimate = (exp(Estimate) - 1)*100)
}

predict_metric <- function(df, metric, prefix, log.transform=c(FALSE, TRUE)) {
  if (log.transform)
    df[[metric]] <- log(df[[metric]])

  # Partition the dataset into training and test set
  set.seed(42)
  data_partition <- createDataPartition(y = df[[metric]], p = 0.80, list = F)
  training <- df[data_partition,]
  testing <- df[-data_partition,]

  # Create model
  model <- lm(as.formula(paste(metric, "~.")), data=training)

  # Evaluate model
  prediction_train <- predict(model, training)
  cat("R2 on training set: ", R2(prediction_train, training[[metric]]), "\n")
  cat("RMSE on training set: ", RMSE(prediction_train, training[[metric]]), "\n")

  prediction_test <- predict(model, testing)
  cat("R2 on test set: ", R2(prediction_test, testing[[metric]]), "\n")
  cat("RMSE on test set: ", RMSE(prediction_test, testing[[metric]]), "\n")

  # Retrain on whole dataset
  model <- lm(as.formula(paste(metric, "~.")), data=df)

  # Pretty print results
  if (log.transform)
    result <- extract_log(model)
  else
    result <- extract(model)

  # addon_plot(result)
  write.csv(result, file=paste(prefix, metric, ".csv", sep=""))
}

args <- commandArgs(trailingOnly = TRUE)
addons <- read.csv(args[1]) %.% select(-cpucount, -memsize)

# Remove linear combinations
cmbs <- findLinearCombos(addons)$remove
if (!is.null(cmbs))
    addons <- addons[, -cmbs]

# Predict!
predict_metric(addons %.% select(-shutdown), "startup", args[2])
predict_metric(addons %.% select(-startup), "shutdown", args[2], TRUE)