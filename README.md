### msudatapull package Documents

msudatapull is an R package with a collection of functions that perform the data querying and processing on the flyer. The functions are designed to allow users to pass a list of pids or a dataframe to get back the values of the derived fields such as finanical aid, first gen, etc.

#### 1.Installation

To install this package from github please first install and load the devtools package

``` r
devtools::install_github("chendi1020/msudatapull")
```

#### 2.Load Package to the enviroment

``` r
library(msudatapull)
```

#### 3.In order to connect to MSUDATA, the msudatacon function always needs to be called first. The connection is done by JDBC installed in following directory

##### The function will call another function to prompt the password window

``` r
drv <- RJDBC::JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","S:/Utilities/Microsoft JDBC Driver 4.0 for SQL Server/sqljdbc_4.0/enu/sqljdbc4.jar")
msudatacon(userid = "chendi4")
```

#### 4.Check the function documentation

``` r
?msudatacon
```

### Functions

-   **msudatacon** set up connection to msudata
-   **cohort** find which cohort the population specified is from (extend PAG)
-   **firstgen\_pull** admission first gen status for population specified (extend PAG)
-   **honor\_pull** honor SCLR status for population specified (extend PAG)
-   **mjrclass\_pull** primary major class code by Pid and term for population specified
-   **FA\_pull** FA information by Pid and AidYr for population specified
-   **gndr\_race\_pull** gender and race/ethnicity pull from SISFull or SISInfo for population specified (extend PAG)
-   **Preliminary.PFS2** provide the 1st Spring and 2nd Fall persistence for undergraduate fall entering cohort including summer starters from SISFrzn extracts
-   **Full.PFS2** provide the 1st Spring and 2nd Fall persistence for undergraduate fall entering cohort including summer starters from SISFull
-   **pregpa\_pull** provide MSU calculated GPA (zero recoded as Null) for the population specified
-   **firstATL\_pull** provide 1st WRA course and Grade for the population specified. If multiple WRA in same first term, lowest course code is chosen
-   **firsttermgpa\_pull** provide first term term GPA for the population specified (if term gpa credit is zero, recode to null)
-   **indicator\_pull** provide indicator values on scale from 1-7 for the population specified (null for level other than AT and UN)
-   **rsadress\_pull** provide residence address country, state, county from SISPRSN for the population specified. If SISFrzn used, it uses qtrterm extract to get the status as entry term.
