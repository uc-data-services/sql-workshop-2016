library("RPostgreSQL")
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