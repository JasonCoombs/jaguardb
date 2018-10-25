RJDBC test log:

Part 0: Connect using JDBC
drv <- try(JDBC("com.jaguar.jdbc.JaguarDriver", paste0(DIR, 'jaguar-jdbc-2.0.jar')))
#.jaddClassPath(paste(DIR, "jaguar-jdbc-2.0.jar"))
drv <- JDBC(driverClass = "com.jaguar.jdbc.JaguarDriver")
con <- dbConnect(drv, "jdbc:jaguar://192.168.7.120:8875/test", "admin", "jaguarjaguarjaguar")
dbListTables(con)

Part 1: CRUD
1.1 Create Table
dbSendQuery(con, "create table rtest (key: id char(16), value: addr char(16);")


1.2 Select from table

# Sol1: select all
rs <- dbSendQuery(con,"select * from test;")
summary(rs)
dbFetch(rs)
dbClearResult(rs)

# Sol2: select all
mydata <- dbGetQuery(con, "select * from test;")
mydata
mydata[1:1, 1:2]

# Sol3: select by conditions
dbGetQuery(con,"select uid, addr from test where uid = 1;")

1.3 Insert into table
dbSendStatement(con,"insert into test(uid, addr) values ('5','sf');")

1.4 Update record
dbSendStatement(con,"update test set addr = 'la' where uid = 3;")

1.5 Delete record
dbSendStatement(con,"delete from test where uid = 5;")

1.6 Drop table
dbRemoveTable(con, "rtest")
#OR
dbSendQuery(con, "drop table rtest;");



Part 2:  Load CSV 
2.1 read csv using R
csvfile <-read.csv('Sales.csv', header = TRUE)

2.1 Using dbWriteTable() --- only load header and keeps inserting same record into database
sales <-read.csv('Sales.csv', header = TRUE)
dbWriteTable(con,"sales", sales, overwrite = TRUE)

2.3 Using sendQuery() to send the generated sql 
#get the csvfile name from bash file
args=commandArgs(T)
csvfile <- args[1]
#write sql to create new table
shellcommand <- paste("./createTableFromCSV.sh ", csvfile)
x <- system(shellcommand, intern = TRUE)
sql <- paste(x,sep ="")
dbSendQuery(con,sql)
#load csv into database
dir <- getwd()
tablename <- tools::file_path_sans_ext(csvfile)
loadcsv <- paste("load ",dir,"/",csvfile," into ", tablename,";", sep ="")
dbSendQuery(con,loadcsv)



Part 3: Analysis data using R

3.1 Select from Database and summary
table <- dbGetQuery(con, "select * from grocery;")
summary(table)
# the percentage of data missing in the table
sum(is.na(table)) / (nrow(table) *ncol(table))
# check duplicate rows
cat("The number of duplicated rows are", nrow(table) - nrow(unique(table)))


3.2 Get partial data from table 
table[1:2,]
table[1:2]
table[which.min(table$unit_price),]
table[which.max(table$unit_price),]
head(table)
tail(table)

3.3 Data type convertion if necessary
table$units_sold <- as.numeric(as.character(table$units_sold))
table$order_date <- as.Date(table$order_date,format='%m/%d/%Y')
table[] <- lapply(table, function(x) as.numeric(as.character(x)))
str(table)

3.4 R Analysis
### linear regression ###
x <- c(table$unit_price)
y <- c(table$units_sold)
relation <- lm(y~x) 
print(summary(relation))  
         
### multiple regression ###
rel <- lm(table$unit_price~table$units_sold+table$unit_cost,data = table)
print(summary(rel))










