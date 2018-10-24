library("RJDBC")
library("DBI")
library("rJava")
DIR <- "/home/tina/jaguar/lib/"

drv <- try(JDBC("com.jaguar.jdbc.JaguarDriver", paste0(DIR, 'jaguar-jdbc-2.0.jar')))
#.jaddClassPath(paste(DIR, "jaguar-jdbc-2.0.jar"))
drv <- JDBC(driverClass = "com.jaguar.jdbc.JaguarDriver")
con <- dbConnect(drv, "jdbc:jaguar://192.168.7.120:8875/test", "admin", "jaguarjaguarjaguar")

args=commandArgs(T)
csvfile <- args[1]
#write sql to create new table
shellcommand <- paste("./createTableFromCSV.sh ", csvfile)
x <- system(shellcommand, intern = TRUE)
sql <- paste(x,sep ="")
#sql
dbSendQuery(con,sql)


#load csv into database
dir <- getwd()
tablename <- tools::file_path_sans_ext(csvfile)
loadcsv <- paste("load ",dir,"/",csvfile," into ", tablename,";", sep ="")
dbSendQuery(con,loadcsv)


dbDisconnect(con)
