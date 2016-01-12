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
db <- dbConnect(drv, 
                 dbname = "stackoverflow", 
                 host = dbhost, 
                 port = 5432, 
                 user = dbuser, 
                 password = dbpasswd)

file <- "original_data/stats/PostLinks.xml"
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
  
  if (str_detect(tmplines[length(tmplines)], "</postlinks>")) {
    message("Yay last chunk!")
    tmplines <- tmplines[-length(tmplines)]
  }
  
  total_posts <- total_posts + length(tmplines)
  
  x <- read_html(paste(tmplines, collapse = ""))
  
  rows <- x %>% xml_find_one("body") %>% xml_find_all("row")
  
  df <- data_frame(id = rows %>% xml_attr("id"),
                   creationdate = rows %>% xml_attr("creationdate"),
                   postid = rows %>% xml_attr("postid"),
                   relatedpostid = rows %>% xml_attr("relatedpostid"),
                   linktypeid = rows %>% xml_attr("linktypeid"))
	
  df$id <- as.numeric(df$id)			   
  df$postid <- as.numeric(df$postid)	
  df$relatedpostid <- as.numeric(df$relatedpostid)
  df$linktypeid <- as.numeric(df$linktypeid)

  dbWriteTable(conn = db, name = "postlinks", as.data.frame(df),
              row.names = FALSE, append = TRUE)
  
}

dbDisconnect(db)
db <- dbConnect(drv, 
                dbname = "stackoverflow", 
                host = dbhost, 
                port = 5432, 
                user = dbuser, 
                password = dbpasswd)

dbGetQuery(db, "select count(1) from postlinks")
dbDisconnect(db)

