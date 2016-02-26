#### sparkR Init
Sys.setenv(SPARK_HOME="c:/spark-1.6.0-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
library(magrittr)
sc <- sparkR.init(master="local[*]")
sqlContext <- sparkRSQL.init(sc)

####### How to use ###########################################################
# 1. assign source.file variable 
# 2. run script
# 3. result --> anomalies
##############################################################################

#### libraries
#install.packages("devtools")
#devtools::install_github("twitter/AnomalyDetection")
library(AnomalyDetection)
library(dplyr)
library(foreach)
#setwd("c:/users/hohkim.KOREANAIR/Projects/SalesAnomaly")

#### next item should be designated before running program!!!
source.file <- "../data/sales_test.csv"
begin.date = as.Date("2013-01-01")
last.date = as.Date("2016-01-10")  # current detection date

######### For reference ######################################################
#agt1$timestamp <- as.POSIXlt(agt1$timestamp, format="%Y-%m-%d %H:%M:%S")
# df: timestamp (must have data & time info), value
# res = AnomalyDetectionTs(df, max_anoms = 0.02, direction = "pos", 
#                         alpha = 0.05, e_value = TRUE, 
#                         longterm = TRUE, piecewise_median_period_weeks = 8, 
#                         plot = FALSE, ylabel = "Sales(USD)")
# optimal parameter -->
#    max_anoms = 0.02 or 0.03, piecewise_median_period_weeks = 8
###############################################################################

#### Helper functions

## get agent info: region, branch, agent_no
get.agent.info <- function(df) {
  distinct(df, agent_no)[, 1:3]
}

## get unique agents vector
get.agents <- function(df) {
  distinct(df, agent_no)[, 3]
}

#### Construct TS data.frame Module ###########################################

# read csv file cantains all agent's sparse sales record --> dataframe
# input: sales_test.cvs --> region, branch, agent_no, dae, nett_amt
# ouiput: agent_no, timestamp, nett_amt, region, branch 

make.agent.ts.data <- function(sparse.df) {
  timestamp <- as.Date(begin.date:last.date, origin="1970-01-01")
  all.dates <- data.frame(timestamp)
  sparse.df$date <- as.Date(sparse.df$date)
  full.df <- merge(all.dates, sparse.df, by.x = "timestamp",  
                   by.y = "date", all.x = TRUE)
  full.df[is.na(full.df)] <- 0
  full.df$timestamp <- paste(full.df$timestamp, "23:00:00")  # add time info
  return(full.df)
}

make.all.ts.data <- function(sales.source, agents) {
  all.sales <- NULL
  for (agent in agents) {
    temp1 <- sales.source %>%
      filter(agent_no == agent) %>%
      select(date, agent_no, nett_amt)
    temp2 <- make.agent.ts.data(temp1)
    all.sales <- rbind(all.sales, temp2)
  }
  all.sales <- merge(all.sales, agent.table, by = "agent_no")
}

#### Anomaly Detection Module #################################################
get.last.anoms.agent <- function(df, agent, p.weeks=16) {
  #t_df_agt = df[df$agt_no == agent_no, 2:3]
  df = df %>%
    filter(agent_no == agent) %>%
    select(timestamp, nett_amt)
  res = try({AnomalyDetectionTs(df, max_anoms = 0.02, direction = "pos", only_last = 'day',
                                alpha = 0.05, e_value = TRUE, longterm = TRUE, 
                                piecewise_median_period_weeks = p.weeks, plot = FALSE)},
            silent = TRUE)
  if (class(res) == "try-error") {
    # TO DO: make it to retry with increasing p.weeks (24,32,64)
    error.info <- rbind(error.info,
                        data.frame(agent_no=agent, message = res[1]))
    # print(paste("error : ", agent))
  }
  else
    return(res$anoms)
}

get.anoms.agent <- function(df, agent, p.weeks=16) {
  #t_df_agt = df[df$agt_no == agent_no, 2:3]
  df = df %>%
         filter(agent_no == agent) %>%
         select(timestamp, nett_amt)
  res = try({AnomalyDetectionTs(df, max_anoms = 0.02, direction = "pos", 
                           alpha = 0.05, e_value = TRUE, longterm = TRUE, 
                           piecewise_median_period_weeks = p.weeks, plot = FALSE)},
            silent = TRUE)
  if (class(res) == "try-error") {
    # TO DO: make it to retry with increasing p.weeks (24,32,64)
    error.info <- rbind(error.info,
                        data.frame(agent_no=agent, message = res[1]))
    # print(paste("error : ", agent))
  }
  else
    return(res$anoms)
}

get.last.anoms.all <- function(df, agents, p.weeks=16) {
  anomalies <- NULL
  for (agent in agents) {
    temp_anoms <- get.last.anoms.agent(df, agent, p.weeks)
    if (dim(temp_anoms)[1] != 0)
      anomalies <- rbind(anomalies, data.frame(agent, temp_anoms))
  }
  return(anomalies)
}

get.anoms.all <- function(df, agents, p.weeks=16) {
  anomalies <- NULL
  for (agent in agents) {
    temp_anoms <- get.anoms.agent(df, agent, p.weeks)
    anomalies <- rbind(anomalies, data.frame(agent, temp_anoms))
  }
  return(anomalies)
}
#### Detection Process ######################################################

## step 1. Set up variables
sales.source <- read.csv(source.file)
agent.table <- get.agent.info(sales.source)
agents <- get.agents(sales.source)
error.info <- data.frame(agent_no=NULL, message=NULL)

## step 2. Make Time Series date
full.sales <- make.all.ts.data(sales.source, agents)
full.sales <- arrange(full.sales, region, branch, agent_no, timestamp)

## Step 3. Detect Anomalies
anomalies.all <- get.anoms.all(full.sales, agents, 16)
anomalies.last <- get.last.anoms.all(full.sales, agents, 16)

head(anomalies.all)

