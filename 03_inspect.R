# install.packages("sparklyr")

library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "yarn-client")

user_tbl <- spark_read_csv(sc, "user", "earcloud/user.tsv", header = FALSE, delimiter = "\t")

user_tbl

user_tbl %>% group_by(V2) %>% summarize(n())

spark_disconnect(sc)