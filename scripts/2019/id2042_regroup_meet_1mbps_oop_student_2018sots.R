## =========================================
##
## COST TO MEET 1 MBPS - ANNUAL OOP PER STUDENT
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
#assign("USER_DAR", Sys.getenv("USER_DAR"), envir=.GlobalEnv)
#assign("PASSWORD_DAR", Sys.getenv("PASSWORD_DAR"), envir=.GlobalEnv)
#assign("URL_DAR", Sys.getenv("URL_DAR"), envir=.GlobalEnv)

assign("USER_DAR", Sys.getenv("USER_DARF18"), envir=.GlobalEnv)
assign("PASSWORD_DAR", Sys.getenv("PASSWORD_DARF18"), envir=.GlobalEnv)
assign("URL_DAR", Sys.getenv("URL_DARF18"), envir=.GlobalEnv)

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


## disconnect from database
dbDisconnect(con)

## load in knapsack budget function
source(paste(GITHUB, "''scripts/2019/prework_queries/fn_knapsack_budget.R", sep=""))

## apply to every row
for (i in 1:nrow(districts)){
  districts$median_mrc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"median")
}

for (i in 1:nrow(districts)){
  districts$p30th_mrc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"30th_percentile")
}

## calculating current oop per student and median "knapsack" cost to meet 1 mbps oop
districts$median_no_cost_group <- districts$current_mrc >= districts$median_mrc

districts$current_annual_oop_cost_student <- districts$current_annual_oop_cost/districts$num_students

districts$extra_median_annual_oop <- (districts$median_mrc - districts$current_mrc)*12*districts$oop_rate
districts$extra_median_annual_oop_student <- (districts$median_mrc - districts$current_mrc)*12*districts$oop_rate/districts$num_students

districts$median_annual_oop <- districts$median_mrc*12*districts$oop_rate
districts$median_annual_oop_student <- districts$median_mrc*12*districts$oop_rate/districts$num_students




## reclassifying districts - removing model_consortia group since its so small and not compelling
districts$district_regroup <- ifelse(districts$district_group=="Hard to Meet Group"&districts$median_no_cost_group==FALSE,"Pay More to Meet",
                                     ifelse(districts$district_group=="Peer Deal","Peer Deal",
                                            ifelse(districts$median_no_cost_group==TRUE,"New Knapsack",
                                                   ifelse(districts$district_group=="Model Consortia","Pay More to Meet","Error"))))


##doublechecking it matches breakdown in MMT 2035
ddply(districts,.(district_regroup,district_group,median_no_cost_group),summarize,
      districts = length(district_id))

districts$district_regroup_combo <- ifelse(districts$district_group=="Hard to Meet Group"&districts$median_no_cost_group==FALSE,"Pay More to Meet",
                                           ifelse(districts$median_no_cost_group==TRUE|districts$district_group=="Peer Deal","Peer Deal or New Knapsack",
                                                  ifelse(districts$district_group=="Model Consortia","Pay More to Meet","Error")))


f_current_oop <- function(data,column) {
  
  ddply(data,column,summarize,
        districts = length(district_id),
        #current_annual_oop_median = median(current_annual_oop_cost),
        current_annual_oop_student_median = median(current_annual_oop_cost_student),
        current_annual_oop_student_weightedavg = sum(current_annual_oop_cost)/sum(num_students)
  )
}

f_pay_more_oop <- function(data ,column) {
  
  ddply(data,column,summarize,
        districts = length(district_id),
        #current_annual_oop_median = median(current_annual_oop_cost),
        current_annual_oop_student_median = median(current_annual_oop_cost_student),
        current_annual_oop_student_weightedavg = sum(current_annual_oop_cost)/sum(num_students),
        
        #extra_knapsack18median_oop = median(extra_median_annual_oop),
        extra_knapsack18median_student_median = median(extra_median_annual_oop_student),
        extra_knapsack18median_student_weightedavg = sum(extra_median_annual_oop)/sum(num_students),
        
        current_plus_extra_student_median = median(current_annual_oop_cost_student) + median(extra_median_annual_oop_student),
        current_plus_extra_student_weightedavg = sum(current_annual_oop_cost)/sum(num_students) + sum(extra_median_annual_oop)/sum(num_students),
        
        #knapsack18median_oop = median(median_annual_oop),
        knapsack18median_student_median = median(median_annual_oop_student),
        knapsack18median_student_weightedavg = sum(median_annual_oop)/sum(num_students)
  )
}

## summary metrics for new district groupings and combination of peer deal & knapsack group
df_regroup <- f_current_oop(districts,"district_regroup")

df_combo <- f_current_oop(districts,"district_regroup_combo")
df_combo <- df_combo[df_combo$district_regroup_combo=="Peer Deal or New Knapsack",]

colnames(df_combo)[colnames(df_combo)=="district_regroup_combo"] <- "district_regroup"


## calculating new cost to meet w knapsack numbers for pay more districts
df_pay_more <- f_pay_more_oop(districts[districts$district_regroup=="Pay More to Meet",],"district_regroup")



## combine
df_final <- rbind(df_regroup,df_combo)
df_final <- merge(x = df_final, y = df_pay_more, all = TRUE)

write.csv(df_final, paste(GITHUB,"/''data/id2042_regroup_meet_1mbps_oop_student_2018sots.csv", sep = ""), row.names = FALSE)

