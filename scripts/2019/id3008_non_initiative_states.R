## =========================================
##
## QUERY DATA FROM THE DB
##
## =========================================

## Clearing memory
rm(list=ls())

##wdir = getwd()
##setwd(wdir)

## load packages (if not already in the environment)
packages.to.install <- c("DBI", "rJava", "RJDBC", "dotenv")
for (i in 1:length(packages.to.install)){
  if (!packages.to.install[i] %in% rownames(installed.packages())){
    install.packages(packages.to.install[i])
  }
}
library(DBI)
library(rJava)
library(RJDBC)
library(dotenv)
options(java.parameters = "-Xmx4g" )

## source environment variables

load_dot_env("~/.env")
assign("GITHUB", Sys.getenv("GITHUB"), envir=.GlobalEnv)
ghub=GITHUB
ghub=gsub('\\\\','/',ghub)
assign("USER_DAR", Sys.getenv("USER_DAR"), envir=.GlobalEnv)
assign("PASSWORD_DAR", Sys.getenv("PASSWORD_DAR"), envir=.GlobalEnv)
assign("URL_DAR", Sys.getenv("URL_DAR"), envir=.GlobalEnv)

##source("~/GitHub/ficher/General_Resources/common_functions/source_env.R")
##source_env("~/.env")


## query function
querydb <- function(query_name){
  query <- readChar(query_name, file.info(query_name)$size)
  data <- dbGetQuery(con, query)
  return(data)
}

##**************************************************************************************************************************************************
## QUERY THE DB

## load PostgreSQL Driver

pgsql <- JDBC("org.postgresql.Driver", paste(ghub,"/General_Resources/postgres_driver/postgresql-9.4.1212.jre7.jar", sep=""), "`")

## connect to the database: ONYX
con <- dbConnect(pgsql, url=URL_DAR, user=USER_DAR, password=PASSWORD_DAR)

## state initiative breakedown query for Insights Meeting

df <- querydb(paste(ghub,"/Projects/sots-isl/scripts/2019/id3006_state_initiatives_breakdowns.sql", sep=""))

## disconnect from database
dbDisconnect(con)


states = unique(df$state_code)
years = unique(df$funding_year)

dfx = df[,-c(3:10,13:18)] ##remove columns of not-interest
colnames(dfx)=c('state_code','funding_year','med_bwps','med_costpermeg','active_initiative')


for (j in 1:length(dfx$state_code)){ #coherce t/f into booleans
  if(dfx$active_initiative[j] == 'f'){
    dfx$active_initiative[j]= 'FALSE'
  } else {
    dfx$active_initiative[j]= 'TRUE'
  }
}

dfx$active_initiative = as.logical(dfx$active_initiative)

#isolate numbers for states with NO initiatives at all
no_init = rep(0,51)
for (i in 1:length(dfx$state_code)){
  for (s in 1:length(states)){
    if(dfx$state_code[i]==states[s]){
      no_init[s] = no_init[s] + dfx$active_initiative[i]

    }
  }
}
no_initx = numeric()
for (s in 1:length(no_init)){
  no_initx = c(no_initx,rep(no_init[s],5))
}
omission = numeric()
for(p in 1:length(no_initx)){
  if(no_initx[p] != 0){
    omission = c(omission,p)
  }
}
dfxx = dfx[-omission,]


deltabw = numeric()
state2 = unique(dfxx$state_code)
for (i in 1:length(dfxx$state_code)){
  for(s in 1:length(state2)){
    if(dfxx$funding_year[i] == 2019 & dfxx$state_code == state2[s]){
      deltabw = rbind.data.frame( deltabw,
                  c( (dfxx$med_bwps[i-3]-dfxx$med_bwps[i-4]),
                     (dfxx$med_bwps[i-2]-dfxx$med_bwps[i-3]),
                     (dfxx$med_bwps[i-1]-dfxx$med_bwps[i-2]),
                     (dfxx$med_bwps[i]-dfxx$med_bwps[i-1])) )
    }
  }
}
deltabw = cbind.data.frame(state2,deltabw)
colnames(deltabw)=c('state_code','bw16','bw17','bw18','bw19')

deltacost = numeric()
for (i in 1:length(dfxx$state_code)){
  for(s in 1:length(state2)){
    if(dfxx$funding_year[i] == 2015 & dfxx$state_code == state2[s]){
      deltacost = rbind.data.frame( deltacost,
                                    c( (dfxx$med_costpermeg[i+1]-dfxx$med_costpermeg[i]),
                                       (dfxx$med_costpermeg[i+2]-dfxx$med_costpermeg[i+1]),
                                       (dfxx$med_costpermeg[i+3]-dfxx$med_costpermeg[i+2]),
                                       (dfxx$med_costpermeg[i+4]-dfxx$med_costpermeg[i+3]) ) )
    }
  }
}
delta_all = cbind.data.frame(deltabw,deltacost)
colnames(delta_all)=c('state_code','bw16','bw17','bw18','bw19','cost16','cost17','cost18','cost19')



##calculate mean/median for raw values (Pricing)
tmeans=numeric()
tmeds=numeric()
for (j in 2:9){
  tmeans[j-1]=mean(delta_all[,j],na.rm=T)
  tmeds[j-1]=median(delta_all[,j],na.rm=T)
}
tmeans
tmeds

pcts=numeric() ##Percent differences for mean/median price differences
med_pct=numeric()
for(t in 6:8){
  pcts[t-1]= (tmeans[t]-tmeans[t-1])/tmeans[t-1]
  med_pct[t-1]= (tmeds[t]-tmeds[t-1])/tmeds[t-1]
}
pcts #% change in mean price/meg
med_pct #% change in median price/meg

#write.csv(med_pct,file=paste(ghub,'/Projects/sots-isl/data/id3003_yoy_price_pct.csv',sep=''),row.names=T)


deltaframe = data.frame()
deltaframe=df2[,1:2]
deltaframe[,3]= df2$bwps_cur - df2$bwps_m1 #differences for bandwidth per student
deltaframe[,4]= df2$bwps_p1 - df2$bwps_cur
deltaframe[,5]= df2$bwps_p2 - df2$bwps_p1

deltaframe[,6]= df2$cpm_cur - df2$cpm_m1 #differences for cost per meg
deltaframe[,7]= df2$cpm_p1 - df2$cpm_cur
deltaframe[,8]= df2$cpm_p2 - df2$cpm_p1
colnames(deltaframe)=c('state_code','funding_year','bwps_0','bwps_1','bwps_2','cpm_0','cpm_1','cpm_2')

#deltaframe = deltaframe[-(c(1,17)),] #omit AK and WY

means=numeric()
meds=numeric()
for (j in 3:8){
  means[j-2]=mean(deltaframe[,j],na.rm = T)
  meds[j-2]=median(deltaframe[,j],na.rm = T)
}
means #mean change in bwps / $/meg
meds #median change in bwps / $/meg


write.csv(delta_all[,1:5],file=paste(ghub,'/Projects/sots-isl/data/id3009_nis_bwps.csv',sep=''),row.names=F)
#write.csv(delta_all[,c(1,6:9)],file=paste(ghub,'/Projects/sots-isl/data/id3008_nis_cpm.csv',sep=''),row.names=F)


plot(x=0:2,y=deltaframe[1,3:5],type='l',ylim=c(0,800))
for(k in 2:length(deltaframe[,1])){
  lines(x=0:2,y=deltaframe[k,3:5],type='l')
}
lines(x=0:2, y=means[1:3],type='l',col='red',lwd=2)
lines(x=0:2, y=meds[1:3],type='l',col='blue',lwd=2)

plot(x=0:2,y=deltaframe[1,6:8],type='l',ylim=c(-10,2))
for(k in 2:length(deltaframe[,1])){
  lines(x=0:2,y=deltaframe[k,6:8],type='l')
}
lines(x=0:2, y=means[4:6],type='l',col='red',lwd=2)
lines(x=0:2, y=meds[4:6],type='l',col='blue',lwd=2)
