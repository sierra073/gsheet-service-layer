## =========================================
##
## REGROUP DISTRICTS THAT NEED TO UPGRADE TO MEET 1 MBPS
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

knapsack <- querydb(paste(GITHUB, "/Projects/sots-isl/scripts/2019/prework_queries/id2032_new_2019_knapsack_2018sots.sql", sep=""))
districts <- querydb(paste(GITHUB, "/Projects/sots-isl/scripts/2019/prework_queries/id2030_who_needs_to_spend_more_2018sots.sql", sep=""))
population <- querydb(paste(GITHUB, "/Projects/sots-isl/scripts/2019/prework_queries/id2035_districts_not_meeting_extrap_2018sots.sql", sep=""))


## disconnect from database
dbDisconnect(con)


## load in knapsack budget function
source(paste(GITHUB, "Projects/sots-isl/scripts/2019/prework_queries/fn_knapsack_budget.R", sep=""))


## calculate knapsack to QA methodology  
for (i in 1:nrow(districts)){
  districts$knapsack_calc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"knapsack")
}

## doublecheck methodology
qa <- districts[(districts$knapsack_calc!= districts$knapsack_mrc),]
## only difference are districts below 50 Mbps 



## apply to every row
for (i in 1:nrow(districts)){
  districts$median_mrc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"median")
}

for (i in 1:nrow(districts)){
  districts$p30th_mrc[i] <- f_knapsack(districts$projected_bw_fy2019_cck12[i],"30th_percentile")
}




districts$extra_median_annual_erate_funding <- (districts$median_mrc - districts$current_mrc)*12*(districts$discount_rate)
districts$extra_median_annual_erate_funding_student <- districts$extra_median_annual_erate_funding/districts$num_students
districts$median_no_cost_group <- districts$current_mrc >= districts$median_mrc


## reclassifying districts - removing model_consortia group since its so small and not compelling
districts$district_regroup <- ifelse(districts$district_group=="Hard to Meet Group"&districts$median_no_cost_group==FALSE,"Pay More to Meet",
                                     ifelse(districts$district_group=="Peer Deal","Peer Deal",
                                            ifelse(districts$median_no_cost_group==TRUE,"New Knapsack",
                                                   ifelse(districts$district_group=="Model Consortia","Pay More to Meet","Error"))))

## checking breakdown 
ddply(districts,c("district_regroup","district_group","median_no_cost_group"),summarize,
      districts = length(district_id))


## original grouping i used that combined peer deal and knapsack group
#districts$district_regroup2 <- ifelse(districts$district_group=="Hard to Meet Group"&districts$median_no_cost_group==FALSE,"Pay More to Meet",
#                                     ifelse(districts$median_no_cost_group==TRUE|districts$district_group=="Peer Deal","Peer Deal or New Knapsack",
#                                            ifelse(districts$district_group=="Model Consortia","Model Consortia","Error")))


## checking the regrouping is correct
ddply(districts,.(district_regroup,district_group,median_no_cost_group),summarize,
      districts = length(district_id))



sample_size_d <- length(districts$district_id)
sample_size_s <- sum(districts$num_students)

final_df <- ddply(districts,"district_regroup",summarize,
      sample_districts = length(district_id),
      districts_p = length(district_id)/sample_size_d,
      extrap_districts = round((length(district_id)/sample_size_d)*population$districts_not_meeting_extrap),
      sample_students = sum(num_students),
      students_p = sum(num_students)/sample_size_s,
      extrap_students = round((sum(num_students)/sample_size_s)*population$students_not_meeting_extrap),
      sample_extra_erate_funding = sum(extra_median_annual_erate_funding),
      extra_erate_funding_student = sum(extra_median_annual_erate_funding)/sum(num_students)
)

## zero out cost metrics 
final_df$sample_extra_erate_funding[final_df$district_regroup != "Pay More to Meet"] <- 0
final_df$extra_erate_funding_student[final_df$district_regroup != "Pay More to Meet"] <- 0

final_df$extrap_extra_erate_funding <- final_df$extra_erate_funding_student*final_df$extrap_students



write.csv(final_df, paste(GITHUB,"/Projects/sots-isl/data/id2035_regroup_meet1mbps.csv", sep = ""), row.names = FALSE)
