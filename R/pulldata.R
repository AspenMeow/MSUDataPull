#' msudatacon is a function to allow connect to MSUDATA using JDBC 
#' 
#' @param userid a character value to indiate userid for MSUDATA
#' @param password a character value to indicate the password
#' 
#' @return MSUDATA JDBC connection
#' 
#' @importFrom RJDBC JDBC
#' @importFrom RJDBC dbConnect
#' 
#' @export
msudatacon <- function(userid, password){
        drv <- RJDBC::JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","S:/Utilities/Microsoft JDBC Driver 4.0 for SQL Server/sqljdbc_4.0/enu/sqljdbc4.jar") 
        MSUDATA <<- RJDBC::dbConnect(drv, "jdbc:sqlserver://msudata.ais.msu.edu", userid,password)
}

#' cohort is a function to pull the entering cohorts for defined population. Fall cohort including summer starters
#' 
#' @param pidlist a character vector of Pids interested
#' 
#' @return a dataframe with Pid, cohort, Entry Term, AidYr Info
#'
#' @export
cohort <- function(ds='SISFull', pidlist){
        #hegis cohort including summer starts for UNFRST for UNTRANS for old students the problem has not been fixed in SISFrzn prior to 1144
        ls <- length(pidlist)
        if (ls <=1000) {
                pidchar<-paste(shQuote(pidlist, type="csh"), collapse=", ")
                dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( " select a.Pid, a.Term_Seq_Id as Entry_Term_Seq_Id, a.Term_Code as Entry_Term_Code, substring(a.Term_Code,1,1) as Trm, 
                                                                substring(a.Term_Seq_Id,2,2) as ch,
                                                                (case when  c.System_Rgstn_Status in ('R','C','E','W') then 'Y' else 'N' end ) as SuTrm
                                
                                                             from ",ds,".dbo.SISPLVT as a 
                                                             inner join (

                                                                select distinct Pid, min(Term_Seq_Id) as minterm
                                                                from " , ds,".dbo.SISPLVT
                                                                 where Pid in (", pidchar, ") and 
                                                                 Student_Level_Code='UN' 
                                                                and Primary_Lvl_Flag='Y' 
                                                                and System_Rgstn_Status in ('R','C','E','W')
                                                                group by Pid
                                                           
                                                           ", ") as b 
                                                           on a.Pid=b.Pid and a.Term_Seq_Id=b.minterm
                                                           left join ",ds,".dbo.SISPLVT as c
                                                           on a.Pid=c.Pid and a.Term_Seq_Id=c.Term_Seq_Id-2 and a.Student_Level_Code=c.Student_Level_Code
                                                             and a.Primary_Lvl_Flag=c.Primary_Lvl_Flag
                                                           where a.Student_Level_Code='UN' 
                                                                and a.Primary_Lvl_Flag='Y' 
                                                           and a.System_Rgstn_Status in ('R','C','E','W') "  ,sep="")
                                          
                )
                dat$COHORT<- ifelse(as.numeric( dat$ch) >=68, 1900+ as.numeric( dat$ch), 2000+as.numeric( dat$ch) )
                dat$ENTRANT_SUMMER_FALL <- ifelse(dat$Trm == 'F' | (dat$Trm=='U' & dat$SuTrm=='Y'), 'Y', 'N')
                dat$AidYr <- ifelse(dat$Trm=='U', dat$COHORT, dat$COHORT+1)
                dat[,c('Pid','COHORT', 'ENTRANT_SUMMER_FALL','Entry_Term_Seq_Id','Entry_Term_Code','AidYr')]
                
        }
        
        else {
                lsg <- ceiling( ls/1000)
                pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
                dat1 <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( " select a.Pid, a.Term_Seq_Id as Entry_Term_Seq_Id, a.Term_Code as Entry_Term_Code ,substring(a.Term_Code,1,1) as Trm, 
                                                                substring(a.Term_Seq_Id,2,2) as ch,
                                                                (case when  c.System_Rgstn_Status in ('R','C','E','W') then 'Y' else 'N' end ) as SuTrm
                                
                                                             from ",ds,".dbo.SISPLVT as a 
                                                             inner join (

                                                                select distinct Pid, min(Term_Seq_Id) as minterm
                                                                from " , ds,".dbo.SISPLVT
                                                                 where Pid in (", pidchar, ") and 
                                                                 Student_Level_Code='UN' 
                                                                and Primary_Lvl_Flag='Y' 
                                                                and System_Rgstn_Status in ('R','C','E','W')
                                                                group by Pid
                                                           
                                                           ", ") as b 
                                                           on a.Pid=b.Pid and a.Term_Seq_Id=b.minterm
                                                           left join ",ds,".dbo.SISPLVT as c
                                                           on a.Pid=c.Pid and a.Term_Seq_Id=c.Term_Seq_Id-2 and a.Student_Level_Code=c.Student_Level_Code
                                                             and a.Primary_Lvl_Flag=c.Primary_Lvl_Flag
                                                           where a.Student_Level_Code='UN' 
                                                                and a.Primary_Lvl_Flag='Y' 
                                                           and a.System_Rgstn_Status in ('R','C','E','W') "  ,sep="")
                                                  
                        )
                        dat1 <- rbind(dat1, dat)
                        
                }
                dat1$COHORT<- ifelse(as.numeric( dat1$ch) >=68, 1900+ as.numeric( dat1$ch), 2000+as.numeric( dat1$ch) )
                dat1$ENTRANT_SUMMER_FALL <- ifelse(dat1$Trm == 'F' | (dat1$Trm=='U' & dat1$SuTrm=='Y'), 'Y', 'N')
                dat1$AidYr <- ifelse(dat1$Trm=='U', dat1$COHORT, dat1$COHORT+1)
                dat1[,c('Pid','COHORT', 'ENTRANT_SUMMER_FALL','Entry_Term_Seq_Id','Entry_Term_Code','AidYr')]
        }
}


#' firstgen_pull is a function to return students with the first generation status
#' 
#' @param ds A character string to indicate the SIS source : SISFull or SISInfo
#' @param pidlist a character vector to input the population interested identifying by Pid
#' 
#' @return this function returns a dataframe with only firstGen students
#' 
#' @importFrom RJDBC dbGetQuery
#' 
#' @examples 
#' \dontrun{firstgen_pull(ds='SISFull', pidlist=PAG$PID)}
#' 
#' @export
firstgen_pull <- function(ds='SISFull', pidlist){
        #convert character vector to the comma separated string
        ls <- length(pidlist)
        if (ls <=1000) {
                pidchar<-paste(shQuote(pidlist, type="csh"), collapse=", ")
                dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct Pid, 'FGEN' as FGEN
                                  from " , ds,".dbo.sisaprs
                                   where Pid in (", pidchar, ") and 
                                         (Spcl_Qual_Code_1='FGEN' OR Spcl_Qual_Code_2='FGEN' OR Spcl_Qual_Code_3='FGEN' OR 
				Spcl_Qual_Code_4='FGEN' OR Spcl_Qual_Code_5='FGEN' OR Spcl_Qual_Code_6='FGEN' OR Spcl_Qual_Code_7='FGEN' OR Spcl_Qual_Code_8='FGEN' OR 
				Spcl_Qual_Code_9='FGEN' OR Spcl_Qual_Code_10='FGEN') ", sep="")
                                   
                )
                dat     
        }
        else{
                lsg <- ceiling( ls/1000)
                pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
                dat1 <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct Pid, 'FGEN' as FGEN
                                  from " , ds,".dbo.sisaprs
                                   where Pid in (", pidchar, ") and 
                                         (Spcl_Qual_Code_1='FGEN' OR Spcl_Qual_Code_2='FGEN' OR Spcl_Qual_Code_3='FGEN' OR 
				Spcl_Qual_Code_4='FGEN' OR Spcl_Qual_Code_5='FGEN' OR Spcl_Qual_Code_6='FGEN' OR Spcl_Qual_Code_7='FGEN' OR Spcl_Qual_Code_8='FGEN' OR 
				Spcl_Qual_Code_9='FGEN' OR Spcl_Qual_Code_10='FGEN') ", sep="")
                                           
                       )
                        
                        #dat <- data.frame(Pid=pidp[[i]], index=i )
                        dat1 <- rbind(dat1, dat)
                        
                }
                dat1
        }
        
      
}


#' firstgen_add is a function to add students with the first generation status to the dataframe provided
#' 
#' @param ds A character string to indicate the SIS source : SISFull or SISInfo
#' @param maindat A dataframe with identifer as either Pid or PID
#' 
#' @return this function add another column FGEN to the original dataframe to indicate first gen status
#' 
#' @examples 
#' \dontrun{firstgen_add(ds='SISFull', maindat=PAG)}
#' 
#' @export
firstgen_add <- function(ds='SISFull', maindat){
        if (sum(names(maindat)=='PID')>0){
                pidlist <- maindat$PID
                firstgends <- firstgen_pull(pidlist=pidlist)
               dat<- merge(maindat, firstgends, by.x = 'PID', by.y = 'Pid', all.x = T) 
        }
        else if (sum(names(maindat)=='Pid')>0){
                pidlist <- maindat$Pid
                firstgends <- firstgen_pull(pidlist=pidlist)
              dat<-  merge(maindat, firstgends, by.x = 'Pid', by.y = 'Pid', all.x = T) 
        }
        
        dat$FGEN <- ifelse(is.na(dat$FGEN), 'N','Y')
        dat
        
}