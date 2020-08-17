## =========================================
##
## CLEAN LINE ITEMS CHANGED BY MACHINE LEARNING
##
## =========================================

## Clearing memory
rm(list=ls())

## load packages (if not already in the environment)
packages.to.install <- c("DBI", "rJava", "RJDBC", "dotenv","plyr")
for (i in 1:length(packages.to.install)){
  if (!packages.to.install[i] %in% rownames(installed.packages())){
    install.packages(packages.to.install[i])
  }
}
library(DBI)
library(rJava)
library(RJDBC)
library(dotenv)
library(plyr)
options(java.parameters = "-Xmx4g" )

## source environment variables
load_dot_env("~/.env")
#assign("DRIVE", Sys.getenv("GOOGLE_DRIVE"), envir=.GlobalEnv)
assign("GITHUB", Sys.getenv("GITHUB"), envir=.GlobalEnv)
assign("USER_DAR", Sys.getenv("USER_DAR"), envir=.GlobalEnv)
assign("PASSWORD_DAR", Sys.getenv("PASSWORD_DAR"), envir=.GlobalEnv)
assign("URL_DAR", Sys.getenv("URL_DAR"), envir=.GlobalEnv)

#assign("USER_DAR", Sys.getenv("USER_DARF18"), envir=.GlobalEnv)
#assign("PASSWORD_DAR", Sys.getenv("PASSWORD_DARF18"), envir=.GlobalEnv)
#assign("URL_DAR", Sys.getenv("URL_DARF18"), envir=.GlobalEnv)

##**************************************************************************************************************************************************
## QUERY THE DB

## load PostgreSQL Driver
pgsql <- JDBC("org.postgresql.Driver", paste(GITHUB, "/General_Resources/postgres_driver/postgresql-9.4.1212.jre7.jar", sep=""), "`")

## connect to the database
con <- dbConnect(pgsql, url=URL_DAR, user=USER_DAR, password=PASSWORD_DAR)

## query function
querydb <- function(query_name){
  query <- readChar(query_name, file.info(query_name)$size)
  data <- dbGetQuery(con, query)
  return(data)
}

clean <- querydb(paste(GITHUB, "/Projects/sots-isl/scripts/2019/prework_queries/id2038_clean_line_items_2018sots.sql", sep=""))


## disconnect from database
dbDisconnect(con)

predictions <- read.csv(paste(GITHUB,"/Projects/sots-isl/data/ml_mass_update_05-14-2018.csv", sep = ""), stringsAsFactors = FALSE)


clean_predictions <- merge(x = clean, y = predictions, by = "frn_complete", all.x = TRUE, all.y = FALSE)

clean_predictions$automated_correction <- ifelse(is.na(clean_predictions$ml_changed_any),FALSE,clean_predictions$ml_changed_any)


final_df <- ddply(clean_predictions,"automated_correction",summarize,
      line_items = length(frn_complete),
      percent_line_items = length(frn_complete)/length(clean_predictions$frn_complete),
      total_line_items = length(clean_predictions$frn_complete)
)


write.csv(final_df, paste(GITHUB,"/Projects/sots-isl/data/id2038_machine_learning_line_items_2018sots.csv", sep = ""), row.names = FALSE)


