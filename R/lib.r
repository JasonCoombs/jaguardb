library("RJDBC")
library("DBI")
library("rJava")
DIR <- "/home/tina/jaguar/lib/"

drv <- try(JDBC("com.jaguar.jdbc.JaguarDriver", paste0(DIR, 'jaguar-jdbc-2.0.jar')))
#.jaddClassPath(paste(DIR, "jaguar-jdbc-2.0.jar"))
drv <- JDBC(driverClass = "com.jaguar.jdbc.JaguarDriver")
con <- dbConnect(drv, "jdbc:jaguar://192.168.7.120:8875/test", "admin", "jaguarjaguarjaguar")

table <- dbGetQuery(con, "select * from grocery;")
#mydata <- as.data.frame(unclass(table))
#str(table)
col_names <- names(table)
table[,col_names] <- lapply(table[,col_names], factor)
str(table)

library(arules,lib.loc="/home/tina/jaguar/doc/rcon/rlib",warn.conflicts = FALSE)

# Association analysis using Package arules
apriori(table, parameter=list(support=0.01,confidence=0.2))
eclat(table, parameter = list(support = 0.05),control = list(verbose=FALSE))
frequentsets=eclat(table,parameter=list(support=0.05,maxlen=10))
inspect(sort(frequentsets,by="support")[1:10])
#Alwasys disconnect to the database as last step
dbDisconnect(con)


#install.packages("plyr", lib="/home/tina/jaguar/doc/rcon/rlib")
#library(plyr,lib.loc="/home/tina/jaguar/doc/rcon/rlib")

#install.packages("psych", lib="/home/tina/jaguar/doc/rcon/rlib")
#library(psych,lib.loc="/home/tina/jaguar/doc/rcon/rlib")



dbDisconnect(con)



