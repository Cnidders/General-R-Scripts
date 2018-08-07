######################
#   Elastic Import   #
######################
## To be defined: 
# charDate = The indexed to be queried
# outputFields = The fields you would like as output
# query = The Elastic query (in Lucene query syntax format)

library(elastic)

#Setting up, and testing connection to ES
source("/home/c.stienen/Info/Elastic Connection.R")

#Loop for querying multiple indices, sequentially 
elasticImpFun <- function(indexDates, check.rows = TRUE){
  print("Importing data: this may take a while..")
  print("Progress is shown in percentage, per imported index.")
  listofres <- list()
  listofdfs <- list()
  pb <- txtProgressBar(min = 0, max = length(indexDates), style = 3)
  for(i in 1:length(indexDates)){
    indexName <- (paste("commerce_reporting-",indexDates[i], sep=""))
    df <- as.data.frame(Search(index = indexName, type='logs', asdf = TRUE, 
                               lowercase_expanded_terms = NULL,
                               shard_request_cache = TRUE,
                               analyze_wildcard = TRUE,
                               #time_scroll = '20m',
                               q = query, 
                               source = (outputFields),
                               size = 1000000))
    #listofres[[i]] <- res
    #df <- as.data.frame(scroll(res$'_scroll_id', asdf = TRUE, row.names = TRUE, fill = TRUE))
    Sys.sleep(0.1)
    setTxtProgressBar(pb,i)
    #df$id <- rownames(df) 
    #melt(df)
    listofdfs[[i]] <- df
  }
  print("Importing data: done!")
  return(listofdfs)
  close(pb)
}

#Importing data per index (values in Chardate), and selecting only '_source' variables
system.time(df <- rbindlist(elasticImpFun(indexDates = charDate),fill = TRUE))
df <- df %>% select(contains("_source"))
names(df) <- (names(df) %>%
                gsub("hits.hits._source.", "", .))

#Sorting df on column names (alphabetically)
sortdf <- df[ ,sort(names(df))]
df <- setcolorder(df, sortdf)
df$Klantnummer5 <- substring(df$user.klantNummer, 1, 5)

#Removing all superfluous attributes
{
  rm(charDate)
  rm(outputFields)
  rm(query)
  rm(sortdf)
  rm(elasticImpFun)
  rm(eshost)
  rm(esname)
  rm(espwd)
}