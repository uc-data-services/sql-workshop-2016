
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
file <- "original_data/stats/Posts.xml"
dat <- file(description = file, open = "r")
invisible(readLines(con = dat, n = 2))
max_iters <- 5000
actual_iter <- 0
chunk_size <- 500
total_posts <- 0
total_questions <- 0
last.rowid <- 0
while (TRUE) {
  
  actual_iter <- actual_iter + 1
  if (actual_iter %% 1000 == 0) {
    message("iter ", actual_iter)
    message("total_posts ", total_posts)
    message("total_questions ", total_questions)
  }
  
  tmplines <- readLines(con = dat, n = chunk_size, encoding = "UTF-8")
  
  if (length(tmplines) == 0) {
    message("bye!")
    break
  } 
  
  if (str_detect(tmplines[length(tmplines)], "</posts>")) {
    message("Yay last chunk!")
    tmplines <- tmplines[-length(tmplines)]
  }
  
  total_posts <- total_posts + length(tmplines)
  
  x <- read_html(paste(tmplines, collapse = ""))
  
  rows <- x %>% xml_find_one("body") %>% xml_find_all("row")
  
  df <- data_frame(questionid = rows %>% xml_attr("id"),
                   answerid = rows %>% xml_attr("acceptedanswerid"))
                   
  df$questionid <- as.numeric(df$id)			   
  df$answerid <- as.numeric(df$answerid)

  dbWriteTable(conn = con, name = "acceptedanswers", as.data.frame(df),
               row.names = FALSE, append = TRUE)
}

dbDisconnect(con)