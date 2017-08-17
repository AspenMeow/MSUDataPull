### msudatapull package Documents

msudatapull is a collection of functions that allows to connect to the various data tables in MSUData. The functions are designed to allow users to pass a list of pids or a dataframe to get back the values of the derived fields such as finanical aid, first gen, etc.

#### Install Package

``` r
devtools::install_github("chendi1020/msudatapull")
```

#### Load Package to the enviroment

``` r
library(msudatapull)
```

#### Check the function documentation

``` r
?msudatacon
```

#### In order to connect to MSUDATA, the msudatacon function always needs to be called first. The connection is done by JDBC installed in following directory

``` r
drv <- RJDBC::JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","S:/Utilities/Microsoft JDBC Driver 4.0 for SQL Server/sqljdbc_4.0/enu/sqljdbc4.jar")
```
