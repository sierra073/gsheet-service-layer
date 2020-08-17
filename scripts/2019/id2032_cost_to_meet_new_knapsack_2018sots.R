## =========================================
##
## COST TO MEET 1 MBPS - NEW KNAPSACK
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
#load_dot_env("C:/Users/jesch/OneDrive/Documents/.env")
load_dot_env("~/.env")
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

knapsack <- querydb(paste(GITHUB, "''scripts/2019/prework_queries/id2032_new_2019_knapsack_2018sots.sql", sep=""))
districts <- querydb(paste(GITHUB, "''scripts/2019/prework_queries/id2032_districts_new_knapsack_2018sots.sql", sep=""))

districts <- districts[districts$district_group=="Hard to Meet Group",]

## disconnect from database
dbDisconnect(con)

## load in knapsack budget function
source(paste(GITHUB, "''scripts/2019/prework_queries/fn_knapsack_budget.R", sep=""))
    
#f_knapsack(1000,"30th_percentile")
  
#f_knapsack(250,"knapsack")

#f_knapsack(10000,"30th_percentile")
  

## calculate knapsack to QA methodology  
for (i in 1:nrow(districts)){
  districts$knapsack_calc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"knapsack")
}

## doublecheck methodology
qa <- districts[(districts$knapsack_calc!= districts$knapsack_mrc),]
## only difference is w 25Mbps 

## apply to every row
for (i in 1:nrow(districts)){
  districts$median_mrc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"median")
}

for (i in 1:nrow(districts)){
  districts$p30th_mrc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"30th_percentile")
}


districts$current_annual_oop_cost_student <- districts$current_annual_oop_cost/districts$num_students

districts$extra_median_annual_oop <- (districts$median_mrc - districts$current_mrc)*12*districts$oop_rate
districts$extra_median_annual_oop_student <- (districts$median_mrc - districts$current_mrc)*12*districts$oop_rate/districts$num_students
districts$median_no_cost_group <- districts$current_mrc >= districts$median_mrc


districts$extra_p30th_annual_oop <- (districts$p30th_mrc - districts$current_mrc)*12*districts$oop_rate
districts$extra_p30th_annual_oop_student <- (districts$p30th_mrc - districts$current_mrc)*12*districts$oop_rate/districts$num_students
districts$p30th_no_cost_group <- districts$current_mrc >= districts$p30th_mrc



f_summary_stats <- function(column) {
 
  ddply(districts,column,summarize,
       districts = length(district_id),
       current_annual_oop_median = median(current_annual_oop_cost),
       current_annual_oop_student_median = median(current_annual_oop_cost_student),
       current_annual_oop_student_weightedavg = sum(current_annual_oop_cost)/sum(num_students),
       extra_knapsack18median_oop = median(extra_median_annual_oop[median_no_cost_group==FALSE]),
       extra_knapsack18median_student_median = median(extra_median_annual_oop_student[median_no_cost_group==FALSE]),
       extra_knapsack18median_student_weightedavg = sum(extra_median_annual_oop[median_no_cost_group==FALSE])/sum(num_students[median_no_cost_group==FALSE]),
       districts_knapsack18median_no_cost = length(district_id[median_no_cost_group==TRUE])/length(district_id),
       extra_knapsack18p30th_oop = median(extra_p30th_annual_oop[p30th_no_cost_group==FALSE]),
       extra_knapsack18p30th_oop_student_median = median(extra_p30th_annual_oop_student[p30th_no_cost_group==FALSE]),
       extra_knapsack18p30th_oop_student_weightedavg = sum(extra_p30th_annual_oop[p30th_no_cost_group==FALSE])/sum(num_students[p30th_no_cost_group==FALSE]),
       districts_knapsack18p30th_no_cost = length(district_id[p30th_no_cost_group==TRUE])/length(district_id)
       )
}

## note that current numbers will reflect metrics for entire subset 
## and not ones that need to pay more at the median or 30th percentile

df_size <- f_summary_stats("size")
colnames(df_size)[colnames(df_size)=="size"] <- "group"


df_discount_rate <- f_summary_stats("c1_discount_rate")
colnames(df_discount_rate)[colnames(df_discount_rate)=="c1_discount_rate"] <- "group"

districts$group <- "All" 

df_all <- f_summary_stats("group")

df_combo <- rbind(df_all,df_size)
df_combo <- rbind(df_combo,df_discount_rate)



write.csv(df_combo, paste(GITHUB,"/''data/id2032_cost_to_meet_new_knapsack_2018sots.csv", sep = ""), row.names = FALSE)

