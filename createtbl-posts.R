# Read posts.xml and create the questions and questions_tags tables

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
  
  posttypeids <- x %>%  xml_find_all("body") %>% xml_find_all("row") %>% xml_attr("posttypeid")

  # build and save the 'questions' table  
  qrows <- rows[posttypeids == "1"]
  
  total_questions <- total_questions + length(rows)
  
  df <- data_frame(questionid = qrows %>% xml_attr("id"),
                   creationdate = qrows %>% xml_attr("creationdate"),
                   score = qrows %>% xml_attr("score"),
                   viewcount = qrows %>% xml_attr("viewcount"),
                   title = qrows %>% xml_attr("title"),
                   ownerid = qrows %>% xml_attr("owneruserid"),
                   tags = qrows %>% xml_attr("tags"))
	df$id <- as.numeric(df$id)			   
	df$creationdate <- format(df$creationdate, format="%Y-%m-%d %H:%M:%S" )
	df$score <- as.numeric(df$score)
	df$viewcount <- as.numeric(df$viewcount)
	
	dbWriteTable(conn = con, name = "questions", as.data.frame(df),
              row.names = FALSE, append = TRUE)

	# parse the tags out from the questions and save in the questions_tags table  
  df2 <- df %>% select(id, tags) %>% group_by(id) %>% do({
    data_frame(tag = str_split(.$tags, "<|>") %>% unlist() %>% setdiff(c("")))
  }) %>% ungroup()
  df2$id <- as.numeric(df2$id)
  
  # create a row id that to use as primary key
  start <- last.rowid + 1
  end <- last.rowid + nrow(df2)
  df2$rowid <- c(start:end)
  last.rowid <- end
  
  # rename the columns
  names(df2) <- c("questionid", "tag", "rowid")
  dbWriteTable(conn = con, name = "questions_tags", as.data.frame(df2),
               row.names = FALSE, append = TRUE)
  
# build the 'answers' table
  arows <- rows[posttypeids == "2"]
  
  df3 <- data_frame(answerid = arows %>% xml_attr("id"),
                   questionid = arows %>% xml_attr("parentid"),
                   creationdate = arows %>% xml_attr("creationdate"),
                   score = qrows %>% xml_attr("score"),
                   ownerid = qrows %>% xml_attr("owneruserid"))
  df3$answerid <- as.numeric(df$answerid)			   
  df3$creationdate <- format(df$creationdate, format="%Y-%m-%d %H:%M:%S" )
  df3$score <- as.numeric(df$score)
  df3$viewcount <- as.numeric(df$viewcount)
  
  dbWriteTable(conn = con, name = "questions", as.data.frame(df),
               row.names = FALSE, append = TRUE)
  
  }

dbDisconnect(con)


### test 
db <- dbConnect(drv, dbname = "stackoverflow", host = "doemo.lib.berkeley.edu", 
port = 5432, user = "hdekker", password = "gammd5.13")
dbGetQuery(db, "select count(1) from questions")
dbGetQuery(db, "select count(1) from questions_tags")
dbDisconnect(db)
