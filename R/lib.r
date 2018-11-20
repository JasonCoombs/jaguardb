library("RJDBC")
library("DBI")
library("rJava")
DIR <- "/home/tina/jaguar/lib/"
#install.packages("arules", lib="//home/tina/jaguar/lib/rlib")
library(arules,lib.loc="/home/tina/jaguar/lib/rlib",warn.conflicts = FALSE)

#install.packages("tidyr", lib="//home/tina/jaguar/lib/rlib")
#library(tidyr, lib.loc="//home/tina/jaguar/lib/rlib")

install.packages("doParallel", lib="/home/tina/jaguar/lib/rlib")
library(doParallel, lib.loc="/home/tina/jaguar/lib/rlib")

drv <- try(JDBC("com.jaguar.jdbc.JaguarDriver", paste0(DIR, 'jaguar-jdbc-2.0.jar')))
#.jaddClassPath(paste(DIR, "jaguar-jdbc-2.0.jar"))
drv <- JDBC(driverClass = "com.jaguar.jdbc.JaguarDriver")
con <- dbConnect(drv, "jdbc:jaguar://192.168.7.120:8875/test", "admin", "jaguarjaguarjaguar")


#select from jaguar
table <- dbGetQuery(con, "select * from grocery;")
col_names <- names(table)
table[,col_names] <- lapply(table[,col_names], factor)
#summary(table)

# Association analysis using Package arules
apriori(table, parameter=list(support=0.01,confidence=0.2))
eclat(table, parameter = list(support = 0.05),control = list(verbose=FALSE))

cl <- makeCluster(detectCores())
registerDoParallel(cl)

dbDisconnect(con)



