### msudatapull package Documents

msudatapull is an R package with a collection of functions that perform the data querying and processing on the flyer. The functions are designed to allow users to pass a list of pids or a dataframe to get back the values of the derived fields such as finanical aid, first gen, etc.

#### 1.Installation from MSUgitlab

``` r
devtools::install_git(
  "https://chendi4@gitlab.msu.edu/chendi4/msudatapull.git", 
  credentials = git2r::cred_user_pass("chendi4", .rs.askForPassword("Your Gitlab Password:"))
)
```

Note: to successfully install the package:

-   check the installation and load of devtool package

``` r
if(! 'devtools ' %in% installed.packages()){install.packages('devtools')}
```

-   check your JAVA home and R version, 64 bit on both

``` r
Sys.getenv("JAVA_HOME")
Sys.getenv("R_ARCH")
```

-   Step up your PATH varaible and JAVA home to locate jvm.dll [check if jvm.dll is in your windows PATH variable](https://stackoverflow.com/questions/7019912/using-the-rjava-package-on-win7-64-bit-with-r)

setting JAVA home in R

``` r
Sys.setenv(JAVA_HOME='C:/Program Files/Java/jre7')
```

##### Alternatively, clone the repository to your local directory using git and install from your local directory

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
-   **term.enroll** provide unit record data on term enrollment including the official enrollment if specifying SISFrzn
-   **adm.coll** provide all majors associated with undergraduates and whether major has been admitted to college or not based on SISFull
