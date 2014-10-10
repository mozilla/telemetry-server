is.installed <- function(mypkg){
  is.element(mypkg, installed.packages()[,1])
}

if (!is.installed("dplyr"))
  install.packages("dplyr", repos="http://cran.rstudio.com/")

if (!is.installed("caret"))
  install.packages("caret", repos="http://cran.rstudio.com/")

library(dplyr)
library(caret)

undot <- function (x) gsub("\\.", " ", x)
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

addon_plot <- function(slow_addons) {
  ggplot(slow_addons, aes(factor(addon, levels=rev(unique(addon))), Estimate)) +
    geom_point() +
    geom_errorbar(width=.1, aes(ymin=Estimate-Error, ymax=Estimate+Error)) +
    coord_flip() +
    scale_y_continuous(name="Startup time overhead in ms") + scale_x_discrete(name ="Add-on") +
    theme_bw()
}

args <- commandArgs(trailingOnly = TRUE)
addons <- read.csv(args[1])

# Partition the dataset into training and test set
set.seed(42)
data_partition <- createDataPartition(y = addons$startup, p = 0.80, list = F)
training <- addons[data_partition,]
testing <- addons[-data_partition,]

# Create model
model <- lm(startup ~ ., data=training)

# Evaluate model
prediction_train <- predict(model, training)
cat("R2 on training set: ", R2(prediction_train, training$startup), "\n")
cat("RMSE on training set: ", RMSE(prediction_train, training$startup), "\n")

prediction_test <- predict(model, testing)
cat("R2 on test set: ", R2(prediction_test, testing$startup), "\n")
cat("RMSE on test set: ", RMSE(prediction_test, testing$startup), "\n")

# Retrain on whole dataset
model <- lm(startup ~ ., data=addons)

# Pretty print results
coefs <- data.frame(coef(summary(model)))

slow_addons <- coefs %.%
  mutate(addon = trim(undot(row.names(coefs)))) %.%
  select(Estimate, Error=Std..Error, t=t.value, Pr=Pr...t.., addon) %.%
  arrange(-Estimate) %.% filter(Estimate > 0 | addon == "Default", Pr < 0.01)

slow_addons[slow_addons$addon=="(Intercept)",]$addon <- "Average startup time (Baseline)"

write.csv(slow_addons, file=args[2])