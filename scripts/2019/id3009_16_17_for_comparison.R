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
pgsql <- JDBC("org.postgresql.Driver", paste(ghub,"/General_Resources/postgres_driver/postgresql-9.4.1212.jre7.jar",sep=''), "`")

## connect to the database: ONYX
con <- dbConnect(pgsql, url=URL_DAR, user=USER_DAR, password=PASSWORD_DAR)

## state initiative breakedown query for Insights Meeting
df <- querydb(paste(ghub,"/Projects/sots-isl/scripts/2019/id3006_state_initiatives_breakdowns.sql",sep=''))

## disconnect from database
dbDisconnect(con)


states = unique(df$state_code)
years = unique(df$funding_year)

dfx = df[,-c(3:10,13:18)] ##remove columns of not-interest
colnames(dfx)=c('state_code','funding_year','med_bwps','med_costpermeg','active_initiative')

for (i in 1:length(dfx$state_code)){
  if(dfx$active_initiative[i]=='t'){
    dfx$active_initiative[i]=TRUE
  } else {
    dfx$active_initiative[i]=FALSE
  }
}
dfx$active_initiative=as.logical(dfx$active_initiative) ##coherce character to boolean

df_trunc=data.frame()
minus1 = data.frame()
plus1 = data.frame()
plus2 = data.frame()

for (s in 1:length(states)){
  for (y in 2:length(years)){
    for (i in 1:length(dfx$state_code)){
      if( (dfx$state_code[i]==states[s]) & (dfx$funding_year[i]==years[y]) ){
        if( (dfx$active_initiative[i]==T) & (dfx$active_initiative[i-1]==F) ){
          minus1 = rbind(minus1,c(dfx$med_bwps[i-1],dfx$med_costpermeg[i-1]))
          plus1 = rbind(plus1,c(dfx$med_bwps[i+1],dfx$med_costpermeg[i+1]))
          plus2 = rbind(plus2,c(dfx$med_bwps[i+2],dfx$med_costpermeg[i+2]))
          df_trunc= rbind(df_trunc,dfx[i,])
        }
      }
    }
  }
}
df_trunc = cbind(df_trunc,minus1,df_trunc$med_bwps,df_trunc$med_costpermeg,plus1,plus2)
colnames(df_trunc)= c('state_code','funding_year','med_bwps','med_costpermeg','active_inititative',
                      'bwps_m1','cpm_m1','bwps_cur','cpm_cur','bwps_p1','cpm_p1','bwps_p2','cpm_p2')
df2=data.frame()
df2 = cbind.data.frame(as.character(df_trunc$state_code),as.numeric(df_trunc$funding_year),
            as.numeric(df_trunc$bwps_m1),as.numeric(df_trunc$bwps_cur),
            as.numeric(df_trunc$bwps_p1),as.numeric(df_trunc$bwps_p2),
            as.numeric(df_trunc$cpm_m1),as.numeric(df_trunc$cpm_cur),
            as.numeric(df_trunc$cpm_p1),as.numeric(df_trunc$cpm_p2))
colnames(df2)= c('state_code','funding_year','bwps_m1','bwps_cur','bwps_p1','bwps_p2','cpm_m1','cpm_cur','cpm_p1','cpm_p2')
df2 = as.data.frame(df2)

df2=df2[-c(1,17),] #remove AK, WY

##separate into 16 launch and 17 launch

l16=data.frame()
l17=data.frame()

for (l in 1:length(df2$state_code)){
  if(df2$funding_year[l]==2016){
    l16=rbind(l16,df2[l,])
  } else if (df2$funding_year[l]==2017){
    l17=rbind(l17,df2[l,])
  }
}


#take differences between years
table_diff = function(inputable){
export=data.frame()
export=cbind.data.frame(inputable[,1])
export[,2]=inputable[,2]
export[,3]=inputable[,4]-inputable[,3]
export[,4]=inputable[,5]-inputable[,4]
export[,5]=inputable[,6]-inputable[,5]
colnames(export)=c('state_code','funding_year','bw_m1','bw_cur','bw_p1')
return(export)
}

diff16=table_diff(l16) #execute function for 16 and 17 groups
diff17=table_diff(l17)


mean16=numeric() #average the differences
mean17=numeric()
for(c in 3:5){
  mean16[c-2]=mean(diff16[,c])
  mean17[c-2]=mean(diff17[,c])
}
mean16
mean17

init_means = rbind.data.frame(mean16,mean17)
colnames(init_means) = c('year-1','year','year+1')

write.csv(init_means,file=paste(ghub,'/Projects/sots-isl/data/id3009_16_17_for_comparison.csv',sep=''),row.names=F)

barplot(mean16,ylim=c(0,300), col=rgb(.8,.1,.05,.5))
barplot(mean17,add=T, col=rgb(.05,.8,.05,.5))


##calculate mean/median for raw values (Pricing)
# tmeans=numeric() 
# tmeds=numeric()
# for (j in 7:10){
#   tmeans[j-6]=mean(df2[,j],na.rm=T)
#   tmeds[j-6]=median(df2[,j],na.rm=T)
# }
# tmeans
# tmeds

pcts=numeric() ##Percent differences for mean/median price differences
med_pct=numeric()
for(t in 2:4){
  pcts[t-1]= (tmeans[t]-tmeans[t-1])/tmeans[t-1]
  med_pct[t-1]= (tmeds[t]-tmeds[t-1])/tmeds[t-1]
}
pcts
med_pct

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
  means[j-2]=mean(deltaframe[,j])
  meds[j-2]=median(deltaframe[,j])
}
means
meds

write.csv(deltaframe[,1:5],file=paste(ghub,'/Projects/sots-isl/data/id3004_yoy_bw_deltas.csv',sep=''),row.names=T)
#write.csv(deltaframe[,c(1:2,6:8)],file='~/GitHub/ficher/Projects/sots-isl/data/yoy_price_deltas.csv',row.names=T)



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
