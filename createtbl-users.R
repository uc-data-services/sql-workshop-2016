# Read posts.xml and create the questions and questions_tags tables
# for "security", the database host, user, and password are read from environment
# variables


library("xml2")
library("dplyr")
library("stringr")
library("RPostgreSQL")

rm(list = ls())
dbhost <- Sys.getenv("PGSERVER")
dbuser <- Sys.getenv("PGUSER")
dbpasswd <- Sys.getenv("PGPASSWD")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, 
                 dbname = "stackoverflow", 
                 host = dbhost, 
                 port = 5432, 
                 user = dbuser, 
                 password = dbpasswd)

file <- "original_data/stats/Users.xml"
dat <- file(description = file, open = "r")
invisible(readLines(con = dat, n = 2))
max_iters <- 5000
actual_iter <- 0
chunk_size <- 500
total_posts <- 0
while (TRUE) {
  
  actual_iter <- actual_iter + 1
  if (actual_iter %% 1000 == 0) {
    message("iter ", actual_iter)
    message("total_posts ", total_posts)
   
  }
  
  tmplines <- readLines(con = dat, n = chunk_size, encoding = "UTF-8")
  
  if (length(tmplines) == 0) {
    message("bye!")
    break
  } 
  
  if (str_detect(tmplines[length(tmplines)], "</users>")) {
    message("Yay last chunk!")
    tmplines <- tmplines[-length(tmplines)]
  }
  
  total_posts <- total_posts + length(tmplines)
  
  x <- read_html(paste(tmplines, collapse = ""))
  
  rows <- x %>% xml_find_one("body") %>% xml_find_all("row")
  
  df <- data_frame(id = rows %>% xml_attr("id"),
                   creationdate = rows %>% xml_attr("creationdate"),
                   lastaccessdate = rows %>% xml_attr("lastaccessdate"),
                   location = rows %>% xml_attr("location"),
                   reputation = rows %>% xml_attr("reputation"),
                   displayname = rows %>% xml_attr("displayname"),
                   upvotes = rows %>% xml_attr("upvotes"),
                   downvotes = rows %>% xml_attr("downvotes"),
                   age = rows %>% xml_attr("age"),
                   accountid = rows %>% xml_attr("accountid"))
	
  df$id <- as.numeric(df$id)			   
  df$reputation <- as.numeric(df$reputation)	
  df$upvotes <- as.numeric(df$upvotes)
  df$downvotes <- as.numeric(df$downvotes)
  df$accountid <- as.numeric(df$accountid)
  
  

  dbWriteTable(conn = con, name = "users", as.data.frame(df),
              row.names = FALSE, append = TRUE)
  
  
}

dbDisconnect(con)
db <- dbConnect(drv, dbname = "stackoverflow", host = "doemo.lib.berkeley.edu", 
                port = 5432, user = "hdekker", password = "gammd5.13")
dbGetQuery(db, "select count(1) from users")
dbDisconnect(db)

