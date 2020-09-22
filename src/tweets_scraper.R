# References: 
# 1. https://www.rdocumentation.org/packages/rtweet/versions/0.7.0/topics/search_tweets
# 2. https://mran.microsoft.com/snapshot/2016-10-05/web/packages/rtweet/rtweet.pdf
# 3. https://cran.r-project.org/web/packages/AzureStor/vignettes/intro.html
# 4. https://blog.revolutionanalytics.com/2018/12/azurestor.html

#Clear the Workspace
rm(list = ls())
setwd(getwd())
print(paste0("Current Working Directory: ",getwd()))

library(rtweet)
library(stringr)
library(yaml)
library(optparse)
library(AzureStor)

# Reading Arguments
option_list = list(
  make_option(c("-d", "--database"), type="character", default="local",
              help="storage - local or azure", metavar="character"),
  make_option(c("-l", "--location"), type="character", default="usa", 
              help='semi-colon(;) separated list of locations. For example:
              "California, USA;Texas, Austin, USA"', metavar="character"),
  make_option(c("-n", "--ntweets"), type="integer", default=100, 
              help="number of tweets (default= %default)", metavar="character"),
  make_option(c("-c", "--config"), type="character", default="config/config_abhishek.yml", 
              help="mention config file and its path", metavar="character")
); 

opt_parser = OptionParser(option_list=option_list);
args = parse_args(opt_parser);

if (is.null(args$database)){
  print_help(opt_parser)
  stop("At least one argument must be supplied (azure or local storage)", call.=FALSE)
}

if (is.null(args$config)){
  print_help(opt_parser)
  stop("Mention config file and its path", call.=FALSE)
}

if (is.null(args$location)){
  print_help(opt_parser)
  stop("Atleast one country must be supplied (if more than one, then use comma to seperate)", call.=FALSE)
}

## Retrieving Twitter Security keys from Config file
config = yaml.load_file(args$config)

## Twitter authentication via web browser
token <- create_token(
  app = config$twitter_app,
  consumer_key = config$api_key,
  consumer_secret =config$api_secret_key,
  access_token = config$access_token,
  access_secret = config$access_token_secret)

# Set Google Maps Key in the environment
Sys.setenv("GOOGLE_MAPS_KEY" = config$google_api_key)
# Use below command to set the environment variables manually
# usethis::edit_r_environ()

# Search Query - Hashtag or keyword separated by 'OR'
query = config$query
locations_list = strsplit(args$location, ';')[[1]]

for(i in 1:length(locations_list))
{ 
  location = gsub("\\s+"," ",trimws(tolower(locations_list[i])))
  loc = gsub("[^0-9_a-zA-Z]+", "", gsub("\\s+","_",trimws(tolower(locations_list[i]))))
  
  # Replacing multiple spaces with single space
  file_query = gsub("\\s+", " ", trimws(tolower(query)))
  # Keeping only maxium of two keywords to get an idea what's keywords have been used just by looking 
  # at the files name.
  if (grepl(" or ", file_query) == TRUE) {
    file_query = paste(strsplit(file_query, " or ")[[1]][1:2], collapse = '_')
  } else {
    file_query = paste(strsplit(file_query, " or ")[[1]][1:1], collapse = '_')
  }
  
  # Location Variable and Status ID Paths
  geocode = lookup_coords(location, config$google_api_key)
  latest_status_file = paste0(file_query,"_last_tweet_id_",loc,".csv")
  earliest_status_file = paste0(file_query,"_first_tweet_id_",loc,".csv")
  
  # Defining Storage Path
  if(args$database=='azure') {
    
    bl_endp_key <- storage_endpoint(config$blob_endpoint,  # Blob Storage Endpoint
                                    sas=config$sas_key)
    cont <- storage_container(bl_endp_key, config$container) # Contianer
    path = paste0(config$azure_path,"/",loc)
    old_file_query = file_query
    file_query = str_replace_all(file_query, "#", "%23")
  } else {
    path = paste0(config$local_path,"/",loc)
  }
  
  latest_status_id_path = paste0(path,"/",paste0(file_query,"_last_tweet_id_",loc,".csv"))
  earliest_status_id_path = paste0(path,"/",paste0(file_query,"_first_tweet_id_",loc,".csv"))
  
  file_exist = FALSE
  if (args$database=='azure'){
    saved_status_id <- try(
                          download_blob(
                              cont,
                              latest_status_id_path,
                              paste0("intermediary/",latest_status_file), overwrite=TRUE),
                          silent=TRUE)
    if (class(saved_status_id) != "try-error") {
      download_blob(cont, latest_status_id_path , paste0("intermediary/",latest_status_file), overwrite=TRUE)
      saved_status_id <- read.csv(paste0("intermediary/",latest_status_file))
      download_blob(cont, earliest_status_id_path , paste0("intermediary/",earliest_status_file), overwrite=TRUE)
      saved_max_status_id <- read.csv(paste0("intermediary/",earliest_status_file))
      file_exist = TRUE  
    } else {
      message('RUNNING GIVEN QUERY COMBINATION FOR THE FIRST TIME!')
    }
  } else {
    saved_status_id <- try(read.csv(latest_status_id_path), silent=TRUE)
    if (class(saved_status_id) != "try-error") {
      saved_status_id <- read.csv(latest_status_id_path)
      saved_max_status_id <- read.csv(earliest_status_id_path)
      file_exist = TRUE
    } else{
      message('RUNNING GIVEN QUERY COMBINATION FOR THE FIRST TIME!')
    }
  }
  
  # Script To extract Tweets Using Rtweets
  retryonratelimit = FALSE
  if (args$ntweets>18000) {
    retryonratelimit = TRUE
  }
  print('FETCHING TWEETS...')
  if (file_exist) {
    since_status_id = saved_status_id$status_id[1]
    max_status_id = saved_max_status_id$status_id[1]
    
    since_status_id = gsub("[^0-9.-]", '', since_status_id)
    max_status_id = gsub("[^0-9.-]", '', max_status_id)
    tweets <- search_tweets(query, n = args$ntweets,
                            retryonratelimit = retryonratelimit,
                            geocode = geocode, since_id = since_status_id)
    # Imp attributes: since_id = since_status_id, max_id = max_status_id
  } else {
    tweets <- search_tweets(query, n = args$ntweets,
                            retryonratelimit = retryonratelimit,
                            geocode = geocode)
  }
  
  until_date <- str_replace(str_replace_all(head(tweets$created_at, 1),c(':'),c('')),' ','_')
  from_date <- str_replace(str_replace_all(tail(tweets$created_at, 1),c(':'),c('')),' ','_')
  
  status_id <- head(tweets$status_id, 1)
  latest_tweet_status_df <- data.frame(query,loc,status_id)
  
  status_id <- tail(tweets$status_id, 1)
  earliest_tweet_status_df <- data.frame(query,loc,status_id)
  
  print('STORING TWEETS...')
  if (args$database=='azure') {
    w_con = textConnection("foo1", "w")
    write.csv(latest_tweet_status_df, w_con)
    textConnectionValue(w_con)
    r_con = textConnection(textConnectionValue(w_con))
    upload_blob(cont, r_con, latest_status_id_path)
    close(w_con)
    close(r_con)

    w_con = textConnection("foo2", "w")
    write.csv(earliest_tweet_status_df, w_con)
    textConnectionValue(w_con)
    r_con = textConnection(textConnectionValue(w_con))
    upload_blob(cont, r_con, earliest_status_id_path)
    close(w_con)
    close(r_con)
  
    save_as_csv(
      tweets,
      paste0("intermediary/", old_file_query, "_tweets_",loc,".csv"),
      prepend_ids = TRUE,
      na = "",
      fileEncoding = "UTF-8"
    )

    upload_blob(cont,
                paste0("intermediary/", old_file_query, "_tweets_",loc,".csv"),
                paste0(path,"/", file_query, "_", from_date, "_", until_date,"_",loc,".csv"))

  } else {
    dir.create(config$local_path, showWarnings = FALSE)
    dir.create(path, showWarnings = FALSE)
    write.csv(latest_tweet_status_df, latest_status_id_path)
    write.csv(earliest_tweet_status_df, earliest_status_id_path)
    save_as_csv(
      tweets,
      paste0(path,"/",file_query, "_", from_date, "_", until_date,"_",loc,".csv"),
      prepend_ids = TRUE,
      na = "",
      fileEncoding = "UTF-8"
    )
  }
  
  print(paste0('DONE FETCHING TWEETS FOR LOCATION: ',loc))
}
