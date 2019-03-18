######################
#   Elastic Import   #
######################
## To be defined: 
# charDate = The indexed to be queried
# outputFields = The fields you would like as output
# query = The Elastic query (in Lucene query syntax format)

###################################################################################################################################
#library(lubridate)
#library(stringr)

# Defining ES query
#query <- "info.type:SearchAndNavigationMessage" #AND (soortRequest:S OR (soortRequest:R AND singleResult:true)) AND NOT searchUrl:*'kw_'* AND NOT matchMode:'ANY'AND NOT zoekTermen:[000000 TO 999999]"

#Assign relevant list(s) to outputfields
#outputFields <- c("_id", "user.salesDistrictOmschrijving","user.klantNummer","user.hoofdSegment", "zoekTermen", "soortRequest", "timestamp_event", "searchUrl", "browser.ipAddress")

#Defining indices to query (last 30 days)
#charDate <- ymd(Sys.Date())- days(0:4)-days(1)
#charDate <- str_replace(charDate, "-", ".")
#charDate <- str_replace(charDate, "-", ".")
###################################################################################################################################



library(elastic)
library(data.table)
library(plyr)
library(dplyr)
library(httr)

#Setting up, and testing connection to ES
source("/home/c.stienen/Info/Elastic Connection.R")

#Loop for querying multiple indices, sequentially 
{
  elasticImpFun <- function(indexDates, check.rows = TRUE){
    print("Importing data: this may take a while..")
    print("Progress is shown in percentage, per imported index.")
    listofres <- list()
    listofdfs <- list()
    pb <- txtProgressBar(min = 0, max = length(indexDates), style = 3)
    for(i in 1:length(indexDates)){
      indexName <- (paste("commerce_reporting-",indexDates[i], sep=""))
      dfES <- as.data.frame(Search(index = indexName, type='logs', 
                                   asdf = TRUE, 
                                   lowercase_expanded_terms = NULL,
                                   #shard_request_cache = TRUE,
                                   analyze_wildcard = "true",
                                   lenient = "true",
                                   search_type = "query_then_fetch",
                                   time_scroll = '20m',
                                   q = query, 
                                   source = (outputFields),
                                   size = 1000000))
      #listofres[[i]] <- res
      #df <- as.data.frame(scroll(res$'_scroll_id', asdf = TRUE, row.names = TRUE, fill = TRUE))
      Sys.sleep(0.1)
      setTxtProgressBar(pb,i)
      #df$id <- rownames(df) 
      #melt(df)
      listofdfs[[i]] <- dfES
    }
    print("Importing Elastic Search data: done!")
    return(listofdfs)
    close(pb)
  }
}

#Importing data per index (values in Chardate), and selecting only '_source' variables
system.time(dfES <- rbindlist(elasticImpFun(indexDates = charDate)))
dfES <- dfES %>% select(contains("_source"))
names(dfES) <- (names(dfES) %>%
                  gsub("hits.hits._source.", "", .))
