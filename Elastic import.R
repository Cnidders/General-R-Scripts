######################
#   Elastic Import   #
######################
## To be defined: 
{
# charDate = The indexes to be queried
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
}
  
library(elastic)
library(data.table)
library(plyr)
library(dplyr)
library(httr)
library(tidyr)

#Setting needed functions for returning empty df's
#Original function from bgoldst stackoverflow (edited by CSN)
#https://stackoverflow.com/questions/26177565/converting-nested-list-to-dataframe/26178034
{
  tl <- function(e) { if (is.null(e)) return(NULL); ret <- typeof(e); if (ret == 'list' && !is.null(names(e))) ret <- list(type='namedlist') else ret <- list(type=ret,len=length(e)); ret; };
mkcsv <- function(v) paste0(collapse=',',v);
keyListToStr <- function(keyList) paste0(collapse='','/',sapply(keyList,function(key) if (is.null(key)) '*' else paste0(collapse=',',key)));

extractLevelColumns <- function(
  nodes, ## current level node selection
  ..., ## additional arguments to data.frame()
  keyList=list(), ## current key path under main list
  sep = "{", ## optional string separator on which to join multi-element vectors; if NULL, will leave as separate columns
  mkname=function(keyList,maxLen) paste0(collapse='.',if (is.null(sep) && maxLen == 1L) keyList[-length(keyList)] else keyList) ## name builder from current keyList and character vector max length across node level; default to dot-separated keys, and remove last index component for scalars
) {
  cat(sprintf('extractLevelColumns(): %s\n',keyListToStr(keyList)));
  if (length(nodes) == 0L) return(list()); ## handle corner case of empty main list
  tlList <- lapply(nodes,tl);
  typeList <- do.call(c,lapply(tlList,`[[`,'type'));
  type <- typeList[1L];
  if (type == 'namedlist') { ## hash; recurse
    allKeys <- unique(do.call(c,lapply(nodes,names)));
    ret <- do.call(c,lapply(allKeys,function(key) extractLevelColumns(lapply(nodes,`[[`,key),...,keyList=c(keyList,key),sep=sep,mkname=mkname)));
  } else if (type == 'list') { ## array; recurse
    lenList <- do.call(c,lapply(tlList,`[[`,'len'));
    maxLen <- max(lenList,na.rm=T);
    allIndexes <- seq_len(maxLen);
    ret <- do.call(c,lapply(allIndexes,function(index) extractLevelColumns(lapply(nodes,function(node) if (length(node) < index) NULL else node[[index]]),...,keyList=c(keyList,index),sep=sep,mkname=mkname))); ## must be careful to translate out-of-bounds to NULL; happens automatically with string keys, but not with integer indexes
  } else if (type%in%c('raw','logical','integer','double','complex','character')) { ## atomic leaf node; build column
    lenList <- do.call(c,lapply(tlList,`[[`,'len'));
    maxLen <- max(lenList,na.rm=T);
    if (is.null(sep)) {
      ret <- lapply(seq_len(maxLen),function(i) setNames(data.frame(sapply(nodes,function(node) if (length(node) < i) NA else node[[i]]),...),mkname(c(keyList,i),maxLen)));
    } else {
      ## keep original type if maxLen is 1, IOW don't stringify
      ret <- list(setNames(data.frame(sapply(nodes,function(node) if (length(node) == 0L) NA else if (maxLen == 1L) node else paste(collapse=sep,node)),...),mkname(keyList,maxLen)));
    }; ## end if
  } else stop(sprintf('error: unsupported type %s at %s.',type,keyListToStr(keyList)));
  if (is.null(ret)) ret <- list(); ## handle corner case of exclusively empty sublists
  ret;
}; ## end extractLevelColumns()
## simple interface function
flattenList <- function(mainList,...) do.call(cbind,extractLevelColumns(mainList,...))
}

#Setting up, and testing connection to ES
source("/home/c.stienen/Info/Elastic Connection.R")

#Loop for querying multiple indices, sequentially 
{
  elasticImpFun <- function(indexDates, check.rows = TRUE){
  print("Importing data: this may take a while..")
  listofdfs <- list()
  pb <- txtProgressBar(min = 0, max = length(indexDates), style = 3)
  for(i in 1:length(indexDates)){
    indexName <- (paste("commerce_reporting-",indexDates[i], sep=""))
    dfES <- (Search(index = indexName, type='logs', 
                               asdf = TRUE, 
                               lowercase_expanded_terms = NULL,
                               shard_request_cache = TRUE,
                               from = 0,
                               track_scores = "true",
                               analyze_wildcard = "true",
                               lenient = "true",
                               search_type = "dfs_query_then_fetch",
                               time_scroll = '20m',
                               q = query, 
                               source = (outputFields),
                               size = 1000000))
    Sys.sleep(0.1)
    setTxtProgressBar(pb,i)
    listofdfs[[i]] <- as.list(dfES)
  }
  print("Importing data: done!")
  return(listofdfs)
  close(pb)
  }
}

#Importing data per index (values in Chardate), and selecting only '_source' variables
system.time(dfES <- flattenList(elasticImpFun(indexDates = charDate)))
dfES <- dfES %>% select(contains("_source"))
names(dfES) <- (names(dfES) %>%
                gsub("hits.hits._source.", "", .))
dfES <- separate_rows(dfES, outputFields, sep = "\\{", convert = TRUE)
dfES <- subset(dfES, !is.na(dfES$timestamp_event))

#Sorting df on column names (alphabetically)
dfES <- dfES[ ,sort(names(dfES))]

#Remove all superfluous attributes
  rm(outputFields, query, elasticImpFun, eshost, esname, espwd, extractLevelColumns, flattenList, keyListToStr, mkcsv, tl)
     
     
     