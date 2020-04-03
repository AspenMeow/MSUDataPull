#' MSUDATA connection
#' 
#' \code{msudatacon} is a function to allow connect to MSUDATA using JDBC
#' 
#' @param userid a character value to indiate userid for MSUDATA
#' 
#' @return MSUDATA JDBC connection established in global environment
#' 
#' @importFrom RJDBC JDBC
#' @importFrom RJDBC dbConnect
#' 
#' @export
msudatacon <- function(userid){
        psswd <- .rs.askForPassword("MSUDATA Database Password:")
        drv <- RJDBC::JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","O:/Apps/Microsoft SQL Server/Microsoft JDBC Driver 4.0 for SQL Server/sqljdbc_4.0/enu/sqljdbc4.jar") 
        MSUDATA <<- RJDBC::dbConnect(drv, "jdbc:sqlserver://msudata.ais.msu.edu", userid,psswd)
}

#' Entering cohort Info
#'
#' cohort is a function to get the entering cohorts for defined population including all levels. Fall cohort including summer starters
#'
#' @param ds A character string to indicate the SIS source  SISFrzn or SISFull or SISInfo
#' @param ex a character string to indicate which flavor of the SISFrzn extract to use,value can only be within QRTRTERM,FIRSTDAY,ENDTERM 
#' @param pidlist a character vector of Pids interested
#' 
#' @return a dataframe with Pid, student_level, cohort, Entry Term, AidYr Info
#'
#' @export
cohort <- function(ds='SISFull', ex='QRTRTERM',pidlist){
        #hegis cohort including summer starts for UNFRST for UNTRANS for old students the problem has not been fixed in SISFrzn prior to 1144
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISFrzn','SISInfo','SISFull'))
                stop("ds must be specificed as SISFrzn, SISInfo or SISFull")
        if (! ex %in% c('QRTRTERM','FIRSTDAY','ENDTERM') )
                stop("ex value must be within 'QRTRTERM','FIRSTDAY','ENDTERM'")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        if (ds %in% c("SISFull","SISInfo")){
                dat1 <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( " select a.Pid, a.Student_Level_Code,a.Term_Seq_Id as Entry_Term_Seq_Id, a.Term_Code as Entry_Term_Code ,substring(a.Term_Code,1,1) as Trm, 
                                                                substring(a.Term_Seq_Id,2,2) as ch,
                                                                (case when  c.System_Rgstn_Status in ('R','C','E','W') then 'Y' else 'N' end ) as SuTrm
                                
                                                             from ",ds,".dbo.SISPLVT as a 
                                                             inner join (

                                                                select distinct Pid,Student_Level_Code, min(Term_Seq_Id) as minterm
                                                                from " , ds,".dbo.SISPLVT
                                                                 where Pid in (", pidchar, ") 
                                                                and Primary_Lvl_Flag='Y' 
                                                                and System_Rgstn_Status in ('R','C','E','W')
                                                                group by Pid,Student_Level_Code
                                                           
                                                           ", ") as b 
                                                           on a.Pid=b.Pid and a.Term_Seq_Id=b.minterm and a.Student_Level_Code=b.Student_Level_Code
                                                           left join ",ds,".dbo.SISPLVT as c
                                                           on a.Pid=c.Pid and a.Term_Seq_Id=c.Term_Seq_Id-2 and a.Student_Level_Code=c.Student_Level_Code
                                                             and a.Primary_Lvl_Flag=c.Primary_Lvl_Flag
                                                           where  a.Primary_Lvl_Flag='Y' 
                                                           and a.System_Rgstn_Status in ('R','C','E','W') "  ,sep="")
                                                  
                        )
                        dat1 <- rbind(dat1, dat)
                        
                }  
        }
        else {
                dat1 <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( " select a.Pid, a.Student_Level_Code,a.Frzn_Term_Seq_Id as Entry_Term_Seq_Id, a.Frzn_Term_Code as Entry_Term_Code ,
                                                                   substring(a.Frzn_Term_Code,1,1) as Trm, 
                                                                   substring(a.Frzn_Term_Seq_Id,2,2) as ch,
                                                                   (case when  c.System_Rgstn_Status in ('R','C','E','W') then 'Y' else 'N' end ) as SuTrm
                                                                   
                                                                   from ",ds,".dbo.MSUPLVT_",ex," as a 
                                                                   inner join (
                                                                   
                                                                   select distinct Pid,Student_Level_Code, min(Frzn_Term_Seq_Id) as minterm
                                                                   from " , ds,".dbo.MSUPLVT_",ex," 
                                                                   where Pid in (", pidchar, ") 
                                                                   and Primary_Lvl_Flag='Y' 
                                                                   and System_Rgstn_Status in ('R','C','E','W')
                                                                   group by Pid,Student_Level_Code
                                                                   
                                                                   ", ") as b 
                                                                   on a.Pid=b.Pid and a.Frzn_Term_Seq_Id=b.minterm and a.Student_Level_Code=b.Student_Level_Code
                                                                   left join ",ds,".dbo.MSUPLVT_",ex," as c
                                                                   on a.Pid=c.Pid and a.Frzn_Term_Seq_Id=c.Frzn_Term_Seq_Id-2 and a.Student_Level_Code=c.Student_Level_Code
                                                                   and a.Primary_Lvl_Flag=c.Primary_Lvl_Flag
                                                                   where  a.Primary_Lvl_Flag='Y' 
                                                                   and a.System_Rgstn_Status in ('R','C','E','W') "  ,sep="")
                                                  
                        )
                        dat1 <- rbind(dat1, dat)
                        
                }
        }
               
                dat1$COHORT<- ifelse(as.numeric( dat1$ch) >=68, 1900+ as.numeric( dat1$ch), 2000+as.numeric( dat1$ch) )
                dat1$ENTRANT_SUMMER_FALL <- ifelse( dat1$Trm=='U' & dat1$SuTrm=='Y', 'Y', 'N')
                dat1$AidYr <- ifelse(dat1$Trm=='U', dat1$COHORT, dat1$COHORT+1)
                dat1[,c('Pid','COHORT', 'ENTRANT_SUMMER_FALL','Entry_Term_Seq_Id','Entry_Term_Code','AidYr','Student_Level_Code')]
        
}

#' Admission First Gen 
#'
#' firstgen_pull is a function to return student records with the admission first generation status from SISAPRS.first_gen is available after entering 2008 for UN and AT
#' 
#' @param ds A character string to indicate the SIS source : SISFull or SISInfo
#' @param pidlist a character vector to input the population interested identifying by Pid
#' 
#' @return this function returns a dataframe with only firstGen students
#' 
#' @importFrom RJDBC dbGetQuery
#' 
#' @export
firstgen_pull <- function(ds='SISFull', pidlist){
        #convert character vector to the comma separated string
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISInfo','SISFull'))
                stop("ds must be specificed as only SISFull ir SISInfo")
        ls <- length(pidlist)
        
        #else{
                lsg <- ceiling( ls/1000)
                pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
                dat1 <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct Pid,
                                (case when Spcl_Qual_Code_1='FGEN' OR Spcl_Qual_Code_2='FGEN' OR Spcl_Qual_Code_3='FGEN' OR 
				Spcl_Qual_Code_4='FGEN' OR Spcl_Qual_Code_5='FGEN' OR Spcl_Qual_Code_6='FGEN' OR Spcl_Qual_Code_7='FGEN' OR Spcl_Qual_Code_8='FGEN' OR 
				Spcl_Qual_Code_9='FGEN' OR Spcl_Qual_Code_10='FGEN' then 'Y' else 'N' end ) as FGEN
                                  from " , ds,".dbo.sisaprs
                                   where Pid in (", pidchar, ") 
                                         ", sep="")
                                           
                       )
                        
                        #dat <- data.frame(Pid=pidp[[i]], index=i )
                        dat1 <- rbind(dat1, dat)
                        
                }
                #cohortds <- cohort(pidlist=dat1)
                dat1 <- merge(dat1,  cohort(pidlist=pidlist)[,c('Pid','COHORT','Student_Level_Code')], by='Pid')
                #dat1[dat1$COHORT>=2008, c('Pid','FGEN')]
                dat1$First_Gen <- ifelse(dat1$COHORT<2008 | ! dat1$Student_Level_Code %in% c('AT','UN'), NA, ifelse(dat1$FGEN=='Y','Y','N'))
                dat1
                
        #}
        
      
}

#' Honor and Academic Scholar
#'
#' honor_pull is a function to get student with the honor or academic scholar combined status by term
#' 
#' @param ds A character string to indicate the SIS source : SISFull or SISInfo
#' @param pidlist a character vector to input the population interested identifying by Pid
#' 
#' @return this function returns a dataframe with combined honor or academic scholar status by term
#' 
#' @importFrom RJDBC dbGetQuery
#' @importFrom reshape2 dcast
#' 
#' @export
honor_pull <- function(ds='SISFull', pidlist){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISInfo','SISFull'))
                stop("ds must be only specificed as SISFull or SISInfo")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        dat1 <- data.frame()
        for (i in seq(lsg)){
                pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct Pid , Student_Level_Code,Major_Code, Term_Code, Term_Seq_Id
                                  from " , ds,".dbo.SISPMJR
                                   where Pid in (", pidchar, ")    ", sep="")
                                          
                )
                
                dat1 <- rbind(dat1, dat)
                dat1$Major_Code <- ifelse(dat1$Major_Code %in% c('HONR', 'SCLR') ,dat1$Major_Code,'Others')
        }
        dat1 <- reshape2::dcast(dat1, Pid +  Student_Level_Code+Term_Code+Term_Seq_Id~ Major_Code,value.var = 'Pid', length )
        if (any(names(dat1)=='HONR') &  any(names(dat1)=='SCLR') ){
                dat1$honorStatus <- ifelse(dat1$HONR>=1 & dat1$SCLR>=1,'HONR & SCLR', ifelse(dat1$HONR>=1, 'HONR only', ifelse(dat1$SCLR>=1, 'SCLR only','Neither')))
        }
        else if (any(names(dat1)=='HONR')) {
                dat1$honorStatus <- ifelse(dat1$HONR>=1,'HONR only','Neither')
        }
        else if (any(names(dat1)=='SCLR')) {
                dat1$honorStatus <- ifelse(dat1$SCLR>=1,'SCLR only','Neither')
        }
        else{
                dat1$honorStatus <- 'Neither'
        }
        
        dat1[,c('Pid','Student_Level_Code','Term_Code','Term_Seq_Id','honorStatus')]
}

#' Primary Major Class Info by Term
#'
#' mjrclass_pull is a function to get student term info on class code and the primary major
#' 
#' @param ds a character string to indicate the SIS source : SISFull or SISInfo or SISFrzn
#' @param ex a character string to indicate which flavor of the SISFrzn extract to use,value can only be within QRTRTERM,FIRSTDAY,ENDTERM
#' @param pidlist a character vector to input the population interested identifying by Pid
#' 
#' @return this function returns a dataframe primary major, college, dept, and class code by term
#' 
#' @importFrom RJDBC dbGetQuery
#' 
#' @export
mjrclass_pull <- function(ds='SISFull', ex='QRTRTERM',pidlist){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISFrzn','SISInfo','SISFull'))
                stop("ds must be specificed as SISFrzn, SISInfo or SISFull")
        if (! ex %in% c('QRTRTERM','FIRSTDAY','ENDTERM') )
                stop("ex value must be within 'QRTRTERM','FIRSTDAY','ENDTERM'")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        if (ds=="SISFrzn"){
                dat1 <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct s.Pid, s.Student_Level_Code, s.Frzn_Term_Seq_Id, s.Class_Code, s.First_Term_At_Lvl, s.Primary_Major_Code,
                                                              m.Short_Desc as Mjr_Short_Desc, m.Coll_Code, m.Dept_Code,
                                        m.Long_Desc as Mjr_Long_Desc, d.Short_Name as Dept_Short_Name, d.Full_Name as Dept_Full_Name,
                                 c.Short_Name as Coll_Short_Name, c.Full_Name as Coll_Full_Name
                                                           from ",ds,".dbo.MSUPLVT_",ex," s
                                                           inner join SISInfo.dbo.MAJORMNT m
                                                           on s.Primary_Major_Code=m.Major_Code
                                                           inner join SISInfo.dbo.DEPT d 
                                                           on m.Dept_Code=d.Dept_Code and m.Coll_Code=d.Coll_Code
                                                           inner join SISInfo.dbo.COLLEGE c 
                                                           on c.Coll_Code=m.Coll_Code
                                                           where s.Pid in (", pidchar, ")  and  s.Primary_Lvl_Flag='Y' and 
                                                           s.System_Rgstn_Status in ('C','R','E','W')
                                                           ")
                                                  
                        )
                        
                        dat1 <- rbind(dat1, dat)
                        
                }  
        }
        else {
                dat1 <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct s.Pid, s.Student_Level_Code, s.Term_Seq_Id, s.Class_Code, s.First_Term_At_Lvl, s.Primary_Major_Code,
                                                              m.Short_Desc as Mjr_Short_Desc, m.Coll_Code, m.Dept_Code,
                                        m.Long_Desc as Mjr_Long_Desc, d.Short_Name as Dept_Short_Name, d.Full_Name as Dept_Full_Name,
                                 c.Short_Name as Coll_Short_Name, c.Full_Name as Coll_Full_Name
                                                           from ",ds,".dbo.SISPLVT s
                                                           inner join SISInfo.dbo.MAJORMNT m
                                                           on s.Primary_Major_Code=m.Major_Code
                                                           inner join SISInfo.dbo.DEPT d 
                                                           on m.Dept_Code=d.Dept_Code and m.Coll_Code=d.Coll_Code
                                                           inner join SISInfo.dbo.COLLEGE c 
                                                           on c.Coll_Code=m.Coll_Code
                                                           where s.Pid in (", pidchar, ")  and  s.Primary_Lvl_Flag='Y' and 
                                                           s.System_Rgstn_Status in ('C','R','E','W')
                                                           ")
                                                  
                        )
                        
                        dat1 <- rbind(dat1, dat)
                        
                }  
        }
        
        dat1
}

#' Financial Aid Related Info
#'
#' FA_pull is a function to get Pell grant, FAFSA 1st Gen, and other FA related Info by Pid and AidYr 
#' 
#' @param pidlist a character vectors of Pids
#' 
#' @return a data frame with Pid, entry cohort, and FA info by AidYr. Note: FA in PAG after 2000
#' 
#' @importFrom dplyr filter
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr summarise
#' @importFrom magrittr "%>%"
#' 
#' @export
FA_pull <- function(pidlist){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        FA <- data.frame()
        for (i in seq(lsg)){
                pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                SISFin <- RJDBC::dbGetQuery(MSUDATA, paste0( " select sam1.Pid, 
	   			sam1.AidYr, 
                                                      sam1.DepStf,
                                                      awd1.AidId, 
                                                      pgm1.TypeAid, 
                                                      pgm1.ActNr, 
                                                      awd1.TotPaid, 
                                                      sam2.Aa, 
                                                      sam4.Efc, 
                                                      sam4.SBudget,
                                                      snp.Ap_Pfedlvl,
                                                      snp.Ap_Pmedlvl, 
                                                      case 
                                                      when snp.Ap_Pfedlvl in ('C') then 'N'
                                                      when snp.Ap_Pfedlvl in ('') then
                                                      case
                                                      when snp.Ap_Pmedlvl in ('C') then 'N' 
                                                      when snp.Ap_Pmedlvl in ('') then 'X' 
                                                      when snp.Ap_Pmedlvl in ('H','M') then 'Y'
                                                      when snp.Ap_Pmedlvl in ('N') then 'O' 
                                                      else 'W' 
                                                      end
                                                      when snp.Ap_Pfedlvl is null then 'Z'  
                                                      when snp.Ap_Pfedlvl in ('H','M') then 
                                                      case
                                                      when snp.Ap_Pmedlvl in ('C') then 'N'
                                                      when snp.Ap_Pmedlvl in ('') then 'Y'
                                                      when snp.Ap_Pmedlvl in ('H','M') then 'Y'
                                                      when snp.Ap_Pmedlvl in ('N') then 'O'
                                                      else 'W'
                                                      end
                                                      when snp.Ap_Pfedlvl in ('N') then 
                                                      case
                                                      when snp.Ap_Pmedlvl in ('C') then 'N'
                                                      when snp.Ap_Pmedlvl in ('') then 'O'
                                                      when snp.Ap_Pmedlvl in ('H','M') then 'O'
                                                      when snp.Ap_Pmedlvl in ('N') then 'O'
                                                      else 'W'
                                                      end
                                                      else 'W'
                                                      end as First_Gen,
                                                      case
                                                      when snp.Ap_Pfedlvl is null and snp.Ap_Pmedlvl = '' then 'F'
                                                      when snp.Ap_Pfedlvl = '' and snp.Ap_Pmedlvl is null then 'M'
                                                      when snp.Ap_Pfedlvl is null and snp.Ap_Pmedlvl is null then 'B'
                                                      when snp.Ap_Pfedlvl = '' and snp.Ap_Pmedlvl = '' then 'N'
                                                      else ''
                                                      end as First_Gen_Null_Test
                                                      from  SISFin.dbo.SAMSAM1 sam1 
                                                      left join SISFin.dbo.SAMAWD1 awd1 
                                                      on  sam1.Pid=awd1.Pid and sam1.AidYr=awd1.AidYr
                                                      left join SISFin.dbo.SAMPGM1 pgm1
                                                      on awd1.AidId=pgm1.AidId and pgm1.AidYr=awd1.AidYr
                                                      left join SISFin.dbo.SAMSAM2 sam2
                                                      on awd1.Pid = sam2.Pid and awd1.AidYr = sam2.AidYr 
                                                      left join SISFin.dbo.SAMSAM4 sam4
                                                      on awd1.Pid=sam4.Pid and awd1.AidYr=sam4.AidYr
                                                      left join SISFin.dbo.SAMBIO1 bio1 
                                                      on awd1.Pid=bio1.Pid
                                                      left join SISFin.dbo.SAMSNP snp
                                                      on bio1.Sid=snp.Ap_Sid and snp.Ap_AidYr=awd1.AidYr
                                                      where sam1.Pid in (", pidchar,")", sep=""
                                                      
                ))
                SamFrzn <- RJDBC::dbGetQuery(MSUDATA, paste0( " select sam1.Pid, 
                                                       sam1.AidYr,
                                                       sam1.DepStf, 
                                                       awd1.AidId, 
                                                       pgm1.TypeAid, 
                                                       pgm1.ActNr, 
                                                       awd1.TotPaid, 
                                                       sam2.Aa, 
                                                       sam4.Efc, 
                                                       sam4.SBudget,
                                                       snp.Ap_Pfedlvl,
                                                       snp.Ap_Pmedlvl,
                                                       case 
                                                       when snp.Ap_Pfedlvl in ('C') then 'N'
                                                       when snp.Ap_Pfedlvl in ('') then
                                                       case
                                                       when snp.Ap_Pmedlvl in ('C') then 'N' 
                                                       when snp.Ap_Pmedlvl in ('') then 'X' 
                                                       when snp.Ap_Pmedlvl in ('H','M') then 'Y' 
                                                       when snp.Ap_Pmedlvl in ('N') then 'O'
                                                       else 'W' 
                                                       end
                                                       when snp.Ap_Pfedlvl is null then 'Z'  
                                                       when snp.Ap_Pfedlvl in ('H','M') then 
                                                       case
                                                       when snp.Ap_Pmedlvl in ('C') then 'N'
                                                       when snp.Ap_Pmedlvl in ('') then 'Y'
                                                       when snp.Ap_Pmedlvl in ('H','M') then 'Y'
                                                       when snp.Ap_Pmedlvl in ('N') then 'O'
                                                       else 'W'
                                                       end
                                                       when snp.Ap_Pfedlvl in ('N') then 
                                                       case
                                                       when snp.Ap_Pmedlvl in ('C') then 'N'
                                                       when snp.Ap_Pmedlvl in ('') then 'O'
                                                       when snp.Ap_Pmedlvl in ('H','M') then 'O'
                                                       when snp.Ap_Pmedlvl in ('N') then 'O'
                                                       else 'W'
                                                       end
                                                       else 'W'
                                                       end as First_Gen,
                                                       case
                                                       when snp.Ap_Pfedlvl is null and snp.Ap_Pmedlvl = '' then 'F'
                                                       when snp.Ap_Pfedlvl = '' and snp.Ap_Pmedlvl is null then 'M'
                                                       when snp.Ap_Pfedlvl is null and snp.Ap_Pmedlvl is null then 'B'
                                                       when snp.Ap_Pfedlvl = '' and snp.Ap_Pmedlvl = '' then 'N'
                                                       else ''
                                                       end as First_Gen_Null_Test
                                                       from  SAMFrzn.dbo.SAMSAM1_Frzn sam1 
                                                       left join SAMFrzn.dbo.SAMAWD1_Frzn awd1 
                                                       on  sam1.Pid=awd1.Pid and sam1.AidYr=awd1.AidYr
                                                       left join SAMFrzn.dbo.SAMPGM1_Frzn pgm1
                                                       on awd1.AidId=pgm1.AidId and pgm1.AidYr=awd1.AidYr
                                                       left join SAMFrzn.dbo.SAMSAM2_Frzn sam2
                                                       on awd1.Pid = sam2.Pid and awd1.AidYr = sam2.AidYr 
                                                       left join SAMFrzn.dbo.SAMSAM4_Frzn sam4
                                                       on awd1.Pid=sam4.Pid and awd1.AidYr=sam4.AidYr
                                                       left join SISFin.dbo.SAMBIO1 bio1 
                                                       on awd1.Pid=bio1.Pid
                                                       left join SAMFrzn.dbo.SAMSNP_Frzn snp
                                                       on bio1.Sid=snp.Ap_Sid and snp.Ap_AidYr=awd1.AidYr
                                                       where sam1.Pid in (", pidchar,")", sep=""
                                                       
                ))
                FA1 <- rbind(SISFin, SamFrzn)
                FA <- rbind(FA, FA1)
        }
        
        #pidchar<-paste(shQuote(pidlist, type="csh"), collapse=", ")
        
        FA <- FA  %>%filter(! is.na(DepStf) & DepStf != ' ' )%>% mutate(Budget= ifelse(! is.na(Aa), SBudget, NA),
                            EFC= ifelse( ! is.na(Aa), Efc, NA ),
                            Need= ifelse(is.na(Aa), NA, ifelse(SBudget-Efc<0, 0,SBudget-Efc ) ),
                            AidId= trimws(AidId),
                            TypeAid= trimws(TypeAid),
                            Pell= ifelse(TypeAid=='E', TotPaid, 0),
                            SEOG= ifelse(grepl('^SEOG', AidId) | grepl('^TSEG', AidId) | grepl('^USEG', AidId) | grepl('^YSEG', AidId), TotPaid,0 ),
                            MSU_General_Fund_Grants= ifelse(TypeAid %in% c('G','F','S','P') & grepl('^11', ActNr), TotPaid,0 ),
                            Other_Grants= ifelse(TypeAid %in% c('G','F','S','P') & ! grepl('^11', ActNr) & 
                                                        ! grepl('^SEOG', AidId) & ! grepl('^TSEG', AidId) &
                                                         ! grepl('^USEG', AidId) & ! grepl('^YSEG', AidId), TotPaid,0 ),
                            Sub_Stafford= ifelse(TypeAid %in% c('B','X'), TotPaid,0),
                            Unsub_Stafford= ifelse(TypeAid %in% c('C','H','Y','1'), TotPaid,0),
                            Perkins= ifelse(AidId %in% c('PERK','PERM','UPEK','YPEK','UPEM','YPEM'), TotPaid,0),
                            Parent_PLUS= ifelse(TypeAid %in% c('D','Z'), TotPaid,0),
                            Other_Student_Loans= ifelse(TypeAid %in% c('L','I','3') & ! AidId %in% c('PERK','PERM','UPEK','YPEK','UPEM','YPEM'), TotPaid,0 ),
                            Work_Study=ifelse(TypeAid=='W', TotPaid,0)) %>%
                 group_by(Pid, AidYr, Aa, EFC, Budget, Need,First_Gen, Ap_Pfedlvl, Ap_Pmedlvl ) %>% 
                summarise(Pell= sum(Pell), SEOG= sum(SEOG),MSU_General_Fund_Grants=sum(MSU_General_Fund_Grants),
                          Other_Grants=sum(Other_Grants),Sub_Stafford=sum(Sub_Stafford),  Unsub_Stafford=sum(Unsub_Stafford),
                          Perkins=sum(Perkins), Parent_PLUS=sum(Parent_PLUS),Other_Student_Loans=sum(Other_Student_Loans),
                          Work_Study=sum(Work_Study)) %>%
                mutate(Pell_f= ifelse(Pell>0, 'Y','N'),
                       SEOG_f= ifelse(SEOG>0, 'Y','N'),
                       Total_FinAid= Pell+SEOG+MSU_General_Fund_Grants+Other_Grants+Sub_Stafford+Unsub_Stafford+Perkins+
                               Parent_PLUS+Other_Student_Loans+Work_Study,
                       Need_Based_FinAid=Pell+SEOG+MSU_General_Fund_Grants+Other_Grants+Sub_Stafford+Perkins+Work_Study,
                       Need_Based_FinAid_f= ifelse((Pell+SEOG+MSU_General_Fund_Grants+Other_Grants+Sub_Stafford+Perkins+Work_Study) > 0, 'Y','N'),
                       Need_Met_wFA= ifelse(Need<=0, 'N', ifelse((Pell+SEOG+MSU_General_Fund_Grants+Other_Grants+Sub_Stafford+
                                                                          Unsub_Stafford+Perkins+Parent_PLUS+Other_Student_Loans+Work_Study)>= Need, 'Y','N')),
                       Need_Met_wNBFA= ifelse(Need<=0, 'N', ifelse((Pell+SEOG+MSU_General_Fund_Grants+Other_Grants+Sub_Stafford+Perkins+
                                                                            Work_Study)>= Need,'Y','N')))
        FA$AidYr  <- as.numeric(FA$AidYr)
        pidtb <- cohort(pidlist = pidlist)
        #within 1st Aidyr
        pidtb1st <- merge(pidtb, FA, by=c('Pid','AidYr'), all.x = T)
        names(pidtb1st)[! names(pidtb1st) %in% c('Pid','COHORT','AidYr','ENTRANT_SUMMER_FALL','Entry_Term_Seq_Id','Entry_Term_Code','Student_Level_Code')] <-
            paste0(names(pidtb1st)[! names(pidtb1st) %in% c('Pid','COHORT','AidYr','ENTRANT_SUMMER_FALL','Entry_Term_Seq_Id','Entry_Term_Code','Student_Level_Code')], '_1st', sep="")
        
        #any year
        pidtb <- merge(pidtb, FA, by='Pid')
        pidtb <- pidtb %>% mutate(Pell_any= ifelse(is.na(Pell_f),'N', ifelse(AidYr.x<= AidYr.y & Pell_f=='Y','Y','N' )))
        
        pidtb <- pidtb %>% group_by(Pid) %>% summarise(Pell_Any_Year= max(Pell_any, na.rm=T))
        pidtb1st <- merge(pidtb1st, pidtb, by='Pid', all.x = T)
        pidtb1st %>% mutate(Pell_1st_Yr= ifelse(is.na(Pell_f_1st),'U',Pell_f_1st),
                                        Pell_Any_Year= ifelse(is.na(Pell_Any_Year),'U', Pell_Any_Year),
                            First_Gen_FA_1st_Yr= ifelse(is.na(First_Gen_1st) | First_Gen_1st %in% c('X','Z'), 'U', First_Gen_1st))
}

#' Gender and Race Ethnicity
#'
#' gndr_race_pull is a function to provide gender, IPEDS ethnicity for the Pids 
#' 
#' @param ds A character string to indicate the SIS source : SISFull or SISInfo or SISFrzn
#' @param pidlist A character vector of Pids
#' @param ex a character string to indicate which flavor of the SISFrzn extract to use,value can only be within QRTRTERM,FIRSTDAY,ENDTERM
#' 
#' @return A data frame with Pids, Gender, Ethnicity and Ctzn Code. If ds is SISFrzn, the status is by term
#' 
#' @export
gndr_race_pull <- function(ds='SISFull', pidlist,ex="QRTRTERM"){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISFrzn','SISInfo','SISFull'))
                stop("ds must be specificed as SISFrzn, SISInfo or SISFull")
        if (! ex %in% c('QRTRTERM','FIRSTDAY','ENDTERM') )
                stop("ex value must be within 'QRTRTERM','FIRSTDAY','ENDTERM'")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        
        if(ds=='SISFrzn'){
                dat <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat1 <- RJDBC::dbGetQuery(MSUDATA,  paste0("select distinct p.Pid, p.Gndr_Flag, e.Ipeds_Flag,p.Ctzn_Code,
                                                                 p.Frzn_Term_Seq_Id,p.Adr_State_Code,p.Adr_Cnty_Code,p.Adr_Cntry_Code,
                                                                   (case when p.Ctzn_Code = 'NOTC' then 'International' 
                                                                   when e.Ipeds_Flag in ('10','11','6') then 'Asian/Hawaii/PI'
                                                                   else i.Short_Desc end) as Ethnicity
                                                                   from ", ds,".dbo.SISPRSN_",ex," p 
                                                                   left join ",ds,".dbo.SISPETHN_",ex," e 
                                                                   on p.Pid=e.Pid and p.Frzn_Term_Seq_Id=e.Frzn_Term_Seq_Id
                                                                   left join SISInfo.dbo.IPEDS i 
                                                                   on e.Ipeds_Flag=i.IPEDS_Flag
                                                                   where p.Pid in (
                                                                   ", pidchar,")", sep=""
                                                                   
                        ))
                        dat <- rbind(dat, dat1)
                }   
        }
        else {
                dat <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat1 <- RJDBC::dbGetQuery(MSUDATA,  paste0("select distinct p.Pid, p.Gndr_Flag, e.Ipeds_Flag,p.Ctzn_Code,
                                                      p.Adr_State_Code,p.Adr_Cnty_Code,p.Adr_Cntry_Code,
                                                    (case when p.Ctzn_Code = 'NOTC' then 'International' 
                                                          when e.Ipeds_Flag in ('10','11','6') then 'Asian/Hawaii/PI'
                                                        else i.Short_Desc end) as Ethnicity
                                                    from ", ds,".dbo.SISPRSN p 
                                                    left join ",ds,".dbo.SISPETHN e 
                                                    on p.Pid=e.Pid 
                                                    left join SISInfo.dbo.IPEDS i 
                                                    on e.Ipeds_Flag=i.IPEDS_Flag
                                                    where p.Pid in (
                                                    ", pidchar,")", sep=""
                                                                   
                        ))
                        dat <- rbind(dat, dat1)
                }    
        }
        
        
        dat
}

#' Preliminary Undergrad Persistence Rate
#'
#' Preliminary.PFS2 is a function to get the 1st Spring and 2nd Fall persistence for undergraduate fall entering cohort including summer starters from SISFrzn extracts
#' 
#' @param cohortex a character string to indicate the fall entering cohort was built from which flavor of the SISFrzn extract,value can only be within QRTRTERM,FIRSTDAY,ENDTERM
#' @param subenrlex a character string to indicate the subsequent enroll and Award population was pulled from which flavor of the SISFrzn extract,value can only be within QRTRTERM,FIRSTDAY,ENDTERM
#' @param cohortterm a numeric vector to indicate the fall entering cohort 1st Fall term e.g 1144
#' @param output indicate the output format, aggregate or list
#' 
#' @return aggregate output returns the persistence rate by entering cohort and lvl entry status. list returns the detail data frame
#' 
#' @importFrom dplyr ungroup
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr summarise
#' @importFrom dplyr ungroup
#' @importFrom magrittr "%>%"
#' 
#' @export
Preliminary.PFS2 <- function(cohortex='QRTRTERM', subenrlex='FIRSTDAY', cohortterm, output="aggregate"){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if ( any(substr(as.character(cohortterm),4,4) != '4'))
                stop('cohorterm must be numeric fall terms vector')
        
        if (! cohortex %in% c('QRTRTERM','FIRSTDAY','ENDTERM') | ! subenrlex %in% c('QRTRTERM','FIRSTDAY','ENDTERM'))
                stop("cohortex and subenrlex value must be within 'QRTRTERM','FIRSTDAY','ENDTERM'")
        
        if (! output %in% c('aggregate','list'))
                stop("output value must be either aggregate or list")
        cohort <- RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct a.Pid, a.Lvl_Entry_Status, a.Frzn_Term_Seq_Id,Int_Grad_TermId,
                                              (case when b.Pid is null then 0 else 1 end ) as enrlFS2,
                                              (case when b1.Pid is null then 0 else 1 end ) as enrlSS1
                                              from SISFrzn.dbo.SISPLVT_",cohortex," a
                                              left join 
                                              ( select *
                                              from   SISFrzn.dbo.SISPLVT_",subenrlex,"   
                                              where System_Rgstn_Status in ('C','R','E','W') and Student_Level_Code='UN' and Primary_Lvl_Flag='Y') b 
                                              on a.Pid=b.Pid and a.Frzn_Term_Seq_Id=b.Frzn_Term_Seq_Id-10 
                                              left join (
                                              select distinct Pid, PAWD.Intended_Award_Term, t.Term_Seq_Id as Int_Grad_TermId
                                              from SISFrzn.dbo.SISPAWD_",subenrlex," as PAWD
                                              inner join SISInfo.dbo.Term as t
                                              on PAWD.Intended_Award_Term=t.Term_Code
                                              inner join SISInfo.dbo.MAJORMNT m 
                                              on PAWD.Major_Code=m.Major_Code
                                              inner join SISInfo.dbo.AWRDTYPE w
                                               on w.Degree_Type_Code=m.Award_Type
                                              where PAWD.Award_Stat_Code in ('CONF','RECM') and PAWD.Student_Level_Code='UN' and w.Prog_Purpose='2.4'
                                              ) c
                                              on a.Pid=c.Pid and a.Frzn_Term_Seq_Id<=c.Int_Grad_TermId
                                              left join 
                                              ( select *
                                              from   SISFrzn.dbo.SISPLVT_",subenrlex,"   
                                              where System_Rgstn_Status in ('C','R','E','W') and Student_Level_Code='UN' and Primary_Lvl_Flag='Y') b1 
                                              on a.Pid=b1.Pid and a.Frzn_Term_Seq_Id=b1.Frzn_Term_Seq_Id-2
                                              where a.Frzn_Term_Seq_Id in (",paste(cohortterm, collapse = ","),") and a.Student_Level_Code='UN' 
                                              and a.System_Rgstn_Status in ('C','R','E','W')
                                              and a.Primary_Lvl_Flag='Y'
                                              and a.Hegis_Cohort_New='Y' 
                                              
                                              "))
        
     cohort <- cohort %>% mutate(AwdbyFS2=  ifelse(is.na(Int_Grad_TermId),0, ifelse( as.numeric(Frzn_Term_Seq_Id)<= as.numeric(Int_Grad_TermId) &
                                                                       as.numeric(Frzn_Term_Seq_Id)+10> as.numeric(Int_Grad_TermId),1,0)),
                                 AwdbySS1=  ifelse(is.na(Int_Grad_TermId),0, ifelse( as.numeric(Frzn_Term_Seq_Id)<= as.numeric(Int_Grad_TermId) &
                                                                                             as.numeric(Frzn_Term_Seq_Id)+2> as.numeric(Int_Grad_TermId),1,0)))%>%
                group_by(Pid, Lvl_Entry_Status, Frzn_Term_Seq_Id, enrlFS2, enrlSS1)%>% summarise(AwdbyFS2= max(AwdbyFS2, na.rm=T),
                                                                                        AwdbySS1=max(AwdbySS1,na.rm=T))%>%
                mutate(PFS2= ifelse(AwdbyFS2==1 | enrlFS2==1,1,0),
                       PSS1= ifelse(AwdbySS1==1 | enrlSS1==1,1,0))
        
      if (output=='aggregate'){
              cohort%>% group_by(Frzn_Term_Seq_Id, Lvl_Entry_Status)%>% summarise(HC=n_distinct(Pid), PSS1=mean(PSS1)*100, PFS2= mean(PFS2)*100)%>% ungroup()
      }  
     else if (output=='list'){
             as.data.frame(cohort)
     }
        
}

#' Undergrad Persistence Rate SISFull
#' 
#' Full.PFS2 is a function to get the 1st Spring and 2nd Fall persistence for undergraduate fall entering cohort including summer starters from SISFull
#' 
#' @param cohortterm a numeric vector to indicate the fall entering cohort 1st Fall term e.g 1144
#' @param output indicate the output format, aggregate or list
#' 
#' @return aggregate output returns the persistence rate by entering cohort and lvl entry status. list returns the detail data frame
#' 
#' @importFrom dplyr ungroup
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr summarise
#' @importFrom dplyr ungroup
#' @importFrom magrittr "%>%"
#' 
#' @export
Full.PFS2 <- function(cohortterm, output="aggregate"){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if ( any(substr(as.character(cohortterm),4,4) != '4'))
                stop('cohorterm must be numeric fall terms vector')
        
        
        if (! output %in% c('aggregate','list'))
                stop("output value must be either aggregate or list")
        
        cohort <- RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct a.Pid, a.Lvl_Entry_Status, a.Term_Seq_Id,Int_Grad_TermId,
                                              (case when b.Pid is null then 0 else 1 end ) as enrlFS2,
                                              (case when b1.Pid is null then 0 else 1 end ) as enrlSS1
                                              from SISFull.dbo.SISPLVT a
                                              left join 
                                              ( select *
                                              from   SISFull.dbo.SISPLVT
                                              where System_Rgstn_Status in ('C','R','E','W') and Student_Level_Code='UN' and Primary_Lvl_Flag='Y') b 
                                              on a.Pid=b.Pid and a.Term_Seq_Id=b.Term_Seq_Id-10 
                                              left join (
                                              select distinct Pid, PAWD.Intended_Award_Term, t.Term_Seq_Id as Int_Grad_TermId
                                              from SISFull.dbo.SISPAWD as PAWD
                                              inner join SISInfo.dbo.Term as t
                                              on PAWD.Intended_Award_Term=t.Term_Code
                                              inner join SISInfo.dbo.MAJORMNT m 
                                              on PAWD.Major_Code=m.Major_Code
                                              inner join SISInfo.dbo.AWRDTYPE w
                                               on w.Degree_Type_Code=m.Award_Type
                                              where PAWD.Award_Stat_Code in ('CONF','RECM') and PAWD.Student_Level_Code='UN' and w.Prog_Purpose='2.4'
                                              ) c
                                              on a.Pid=c.Pid and a.Term_Seq_Id<=c.Int_Grad_TermId
                                              left join 
                                              ( select *
                                              from   SISFull.dbo.SISPLVT
                                              where System_Rgstn_Status in ('C','R','E','W') and Student_Level_Code='UN' and Primary_Lvl_Flag='Y') b1 
                                              on a.Pid=b1.Pid and a.Term_Seq_Id=b1.Term_Seq_Id-2 
                                              where a.Term_Seq_Id in (",paste(cohortterm, collapse = ","),") and a.Student_Level_Code='UN'
                                              and a.System_Rgstn_Status in ('C','R','E','W')
                                              and a.Primary_Lvl_Flag='Y'
                                              and a.Hegis_Cohort_New='Y' 
                                              
                                              "))
        cohort <- cohort %>% mutate(AwdFS2=  ifelse(is.na(Int_Grad_TermId),0, ifelse( as.numeric(Term_Seq_Id)<= as.numeric(Int_Grad_TermId) &
                                                                                           as.numeric(Term_Seq_Id)+10> as.numeric(Int_Grad_TermId),1,0)),
                                    AwdSS1=  ifelse(is.na(Int_Grad_TermId),0, ifelse( as.numeric(Term_Seq_Id)<= as.numeric(Int_Grad_TermId) &
                                                                                              as.numeric(Term_Seq_Id)+2> as.numeric(Int_Grad_TermId),1,0)))%>%
                group_by(Pid, Lvl_Entry_Status, Term_Seq_Id, enrlFS2, enrlSS1)%>% summarise(AwdbyFS2= max(AwdFS2, na.rm=T),
                                                                                            AwdbySS1= max(AwdSS1, na.rm=T))%>%
                mutate(PFS2= ifelse(AwdbyFS2==1 | enrlFS2==1,1,0),
                       PSS1= ifelse(AwdbySS1==1 | enrlSS1==1,1,0))
        if (output=='aggregate'){  
          cohort%>% group_by(Term_Seq_Id, Lvl_Entry_Status)%>% summarise(HC=n_distinct(Pid), PSS1= mean(PSS1)*100,PFS2= mean(PFS2)*100)%>%ungroup()
        }
        else if (output=='list'){
               as.data.frame( cohort)
        }
        
}

#' MSU Calcuated GPA 
#'
#' pregpa_pull is a function to pull MSU calculated GPA from SISFull or SISInfo
#' 
#' @param ds A character string to indicate the SIS source SISFull or SISInfo
#' @param pidlist A character vector of Pids
#' 
#' @return A data frame with Pids, max value of predGPA if multiple. give null if hsgpa is zero
#' 
#' @export
pregpa_pull<- function(ds='SISFull', pidlist){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISFull','SISInfo'))
                stop("ds must be either SISFull or SISInfo")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        dat <- data.frame()
        for (i in seq(lsg)){
                pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                dat1 <- RJDBC::dbGetQuery(MSUDATA,  paste0("select distinct Pid, 
                                                           max((case when hs_gpa=0 then null else hs_gpa end) )as Pred_GPA
                                                    
                                                    from ", ds,".dbo.SISAGPA 
                                                    
                                                    where  hs_gpa_type_code in ('PRED') and  Pid in (
                                                    ", pidchar,") 
                                                    group by Pid ", sep=""
                                                    
                ))
                dat <- rbind(dat, dat1)
        }
        dat
        
}

#' First WRA course and Grade
#'
#' firstATL_pull is a function to pull First ATL Course and Grade
#' 
#' @param ds A character string to indicate the SIS source  SISFull or SISInfo
#' @param pidlist A character vector of Pids
#' 
#' @return A data frame with Pids, Term_Seq_Id when first ATL was taken, 1st ATL and Grade
#' 
#' @importFrom dplyr ungroup
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr filter
#' @importFrom dplyr select
#' @importFrom magrittr "%>%"
#' 
#' @export
firstATL_pull <- function(ds='SISFull', pidlist){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISInfo','SISFull'))
                stop("ds must be specificed as SISInfo or SISFull")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        dat <- data.frame()
        for (i in seq(lsg)){
                pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                dat1 <- RJDBC::dbGetQuery(MSUDATA,  paste0("select distinct Pid,  Term_Seq_Id,Subj_Code,Crse_Code, Grade_Code
                                                    
                                                    from ", ds,".dbo.SISPCRS
                           where    Primary_Lvl_Flag='Y'
                    and ((Subj_Code in ('ATL','WRA') and left(Crse_Code,1) in ('1', 'Q1')) or 
                    (Subj_Code = 'MC' and Crse_Code in ('111','Q111', '112')) or 
                   (Subj_Code in ( 'LBS','LB') and Crse_Code in ('133')) or
                   (Subj_Code = 'AL' and Crse_Code in ('192','192H'))) and  Pid in (
                                                    ", pidchar,") 
                                                     ", sep=""
                                                    
                ))
                dat <- rbind(dat, dat1)
        }
        
        dat<-dat %>% group_by(Pid)%>% mutate(minterm=min(Term_Seq_Id))%>%filter(minterm==Term_Seq_Id)%>%select(-c(minterm)) %>%ungroup()
        dat<-as.data.frame(dat)%>% group_by(Pid)%>% mutate(mincode=min(Crse_Code))%>% filter(Crse_Code==mincode)%>%select(-c(mincode))%>%ungroup()
        as.data.frame(dat)
}

#' First Term GPA
#'
#' firsttermgpa_pull is a function to pull First Term GPA by student level
#' 
#' @param ds A character string to indicate the SIS source  SISFull or SISInfo
#' @param pidlist A character vector of Pids
#' 
#' @return A data frame with Pids, student_level,first_term_seq_id, termGPA, firstterm GPA credits
#' 
#' @importFrom dplyr ungroup
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr filter
#' @importFrom dplyr rename
#' @importFrom dplyr select
#' @importFrom magrittr "%>%"
#' 
#' @export
firsttermgpa_pull <- function(ds='SISFull', pidlist){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISInfo','SISFull'))
                stop("ds must be specificed as SISInfo or SISFull")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        dat <- data.frame()
        for (i in seq(lsg)){
                pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                dat1<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct s.Pid, s.Student_Level_Code, s.Term_Seq_Id, 
                                                             s.Msu_Lt_Gpa_Credits,
                                                             (case when s.Msu_Lt_Gpa_Credits=0 then null else Msu_Lt_Grd_Pt_Avg end) as TermGPA
                                                           
                                                           from ", ds,".dbo.SISPLVT s
                                                           
                                                           where s.Pid in (", pidchar, ")  and  s.Primary_Lvl_Flag='Y' and 
                                                           s.System_Rgstn_Status in ('C','R','E','W')
                                                           ")
                                          
                )
                
                dat <- rbind(dat, dat1)
        }
        
        dat<-dat %>% group_by(Pid,Student_Level_Code )%>% mutate(minterm=min(Term_Seq_Id))%>%filter(minterm==Term_Seq_Id)%>% 
                rename(FirstTerm=Term_Seq_Id, Firsttermcredit=Msu_Lt_Gpa_Credits, FirsttermGPA=TermGPA)%>%
                select(-c(minterm)) %>%ungroup()
        as.data.frame(dat)
}

#' Performance Indicator
#'
#' indicator_pull is a function to pull indicator and atrisk for UN and AT from SISFull
#' 
#' @param pidlist A character vector of Pids
#' 
#' @return A data frame with Pids indicator atrisk FirsttermGPA student_level pred_GPA 1stATL
#' 
#' @importFrom dplyr mutate
#' @importFrom magrittr "%>%"
#' 
#' @export
indicator_pull <- function( pidlist){
        dat <- data.frame(Pid=pidlist)
        predgpa <- pregpa_pull(pidlist = pidlist)
        dat <- merge(dat, predgpa, by='Pid', all.x = T)
        gpa1st<-firsttermgpa_pull(pidlist = pidlist)
        dat <- merge(dat, gpa1st, by='Pid', all.x = T)
        ATL1st <- firstATL_pull(pidlist = pidlist)
         merge(dat, ATL1st, by='Pid', all.x = T) %>% filter(Student_Level_Code %in% c('UN','AT'))%>% mutate(indicator= ifelse(Student_Level_Code %in% c('UN','AT'), 4,NA),
                                                                 indicator=ifelse(! is.na(indicator) & FirsttermGPA>=2.5 & ! is.na(FirsttermGPA), indicator+1, indicator),
                                                                 indicator=ifelse(! is.na(indicator) & FirsttermGPA<1.6 & ! is.na(FirsttermGPA), indicator-1, indicator),
                                                                 indicator=ifelse(! is.na(indicator) & Grade_Code %in% c('3.0','3.5','4.0') & ! is.na(Grade_Code), indicator+1, indicator),
                                                                 indicator=ifelse(! is.na(indicator) & Grade_Code %in% c('0.5', '1.0', '1.5','0.0') & ! is.na(Grade_Code), indicator-1, indicator),
                                                                 indicator=ifelse(! is.na(indicator) & Pred_GPA>=2.4 & ! is.na(Pred_GPA), indicator+1, indicator),
                                                                 indicator=ifelse(! is.na(indicator) & Pred_GPA<2.0 & ! is.na(Pred_GPA), indicator-1, indicator),
                                                            atrisk= ifelse(Student_Level_Code %in% c('UN','AT'),'N',NA),
                                                            atrisk= ifelse(atrisk=='N' & ((Pred_GPA<2.0 & ! is.na(Pred_GPA)) | (FirsttermGPA<1.6 & ! is.na(FirsttermGPA)) |
                                                                                      (Grade_Code %in% c('0.5', '1.0', '1.5','0.0') & ! is.na(Grade_Code)) ),'Y', atrisk))
}

#' Residence Adress Country State County
#'
#' rsadress_pull is a function to pull the residence address from prsn default ds shows address in entry term
#' 
#' @param ds A character string to indicate the SIS source  SISFrzn or SISFull or SISInfo
#' @param pidlist A character vector of Pids
#' 
#' @return A data frame with Pids, student_level_code and entry Term residence address country county state
#' 
#' @importFrom dplyr select
#' @importFrom magrittr "%>%"
#'  
#' @export
rsadress_pull <- function(ds='SISFrzn', ex="QRTRTERM" ,pidlist){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISFrzn','SISInfo','SISFull'))
                stop("ds must be specificed as SISFrzn, SISInfo or SISFull")
        if (! ex %in% c('QRTRTERM','FIRSTDAY','ENDTERM') )
                stop("ex value must be within 'QRTRTERM','FIRSTDAY','ENDTERM'")
        ls <- length(pidlist)
        lsg <- ceiling( ls/1000)
        pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
        
        if (ds=='SISFrzn'){
                dat <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat1<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct p.Pid, p.Frzn_Term_Seq_Id as Entry_Term_Seq_Id,p.ADR_CNTRY_CODE ,c.Full_Name as ADR_CNTRY_NM ,   
                                                                    p.ADR_CNTY_CODE ,d.Full_Name as ADR_CNTY_NM,
                                                                    p.Adr_State_Code , e.Full_Name as ADR_State_NM 
                                                                    from ", ds,".dbo.SISPRSN_",ex," p
                                                                    left join SISInfo.dbo.COUNTRY c
                                                                    on p.Adr_Cntry_Code=c.Cntry_Code
                                                                    left join SISInfo.dbo.COUNTY d
                                                                    on p.ADR_CNTY_CODE=d.Cnty_Code
                                                                    left join SISInfo.dbo.STATE e
                                                                    on p.Adr_State_Code=e.State_Code and p.Adr_Cntry_Code=e.Cntry_Code 
                                                                    where p.Pid in (", pidchar, ")  
                                                                    ")
                                                   
                        )
                        
                        dat <- rbind(dat, dat1)
                }
                ds <- cohort(pidlist = pidlist)
                merge(ds,dat, by=c('Pid','Entry_Term_Seq_Id'), all.x=T)%>% select(Pid, Student_Level_Code,ADR_CNTRY_CODE, ADR_CNTRY_NM,ADR_CNTY_CODE,ADR_CNTY_NM,Adr_State_Code,ADR_State_NM)
        }
        else {
                dat <- data.frame()
                for (i in seq(lsg)){
                        pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
                        dat1<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct p.Pid, p.ADR_CNTRY_CODE ,c.Full_Name as ADR_CNTRY_NM ,   
                                       p.ADR_CNTY_CODE ,d.Full_Name as ADR_CNTY_NM,
                                       p.Adr_State_Code , e.Full_Name as ADR_State_NM 
                                       from ", ds,".dbo.SISPRSN p
                                        left join SISInfo.dbo.COUNTRY c
                                                           on p.Adr_Cntry_Code=c.Cntry_Code
                                                            left join SISInfo.dbo.COUNTY d
                                                            on p.ADR_CNTY_CODE=d.Cnty_Code
                                                            left join SISInfo.dbo.STATE e
                                                            on p.Adr_State_Code=e.State_Code and p.Adr_Cntry_Code=e.Cntry_Code 
                                           where p.Pid in (", pidchar, ")  
                                                           ")
                                                   
                        )
                        
                        dat <- rbind(dat, dat1)
                }
                dat
        }
   
}

#' Term Enrollment Student List
#'
#' term.enroll is a function to get enrollment unit record data including official Frzn enrollment  
#' 
#' @param ds A character string to indicate the SIS source  SISFrzn or SISFull or SISInfo
#' @param ex a character string to indicate which flavor of the SISFrzn extract to use,value can only be within QRTRTERM,FIRSTDAY,ENDTERM
#' @param termid a numeric vector to specify which term or terms to pull the data from
#' 
#' @return A data frame with enrolled Pids and term related info
#' 
#' @export
term.enroll <- function(ds='SISFrzn',  ex="QRTRTERM", termid){
        if(! exists('MSUDATA') )
                stop("You need to connect to msudata first using msudatacon")
        if (! ds %in% c('SISFrzn','SISInfo','SISFull'))
                stop("ds must be specificed as SISFrzn, SISInfo or SISFull")
        if (! ex %in% c('QRTRTERM','FIRSTDAY','ENDTERM') )
                stop("ex value must be within 'QRTRTERM','FIRSTDAY','ENDTERM'")

        
        if (ds=='SISFrzn'){
               
                        dat1<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct a.Pid, a.Frzn_Term_Seq_Id, a.Frzn_Term_Code, 
                                                                   a.Student_Level_Code,a.Time_Status, a.Class_Code, a.Hegis_Cohort_New,a.Lvl_Entry_Status,
                                                                   p.Gndr_Flag, e.Ipeds_Flag,p.Ctzn_Code,
                                                                   (case when p.Ctzn_Code = 'NOTC' then 'International' 
                                                                   when e.Ipeds_Flag in ('10','11','6') then 'Asian/Hawaii/PI'
                                                                   else i.Short_Desc end) as Ethnicity
                                                                      from ",ds,".dbo.MSUPLVT_",ex," a   
                                                                      left join ", ds,".dbo.SISPRSN_",ex," p
                                                                      on a.Pid=p.Pid and a.Frzn_Term_Seq_Id=p.Frzn_Term_Seq_Id
                                                                      left join ",ds,".dbo.SISPETHN_",ex," e 
                                                                   on p.Pid=e.Pid and p.Frzn_Term_Seq_Id=e.Frzn_Term_Seq_Id
                                                                   left join SISInfo.dbo.IPEDS i 
                                                                   on e.Ipeds_Flag=i.IPEDS_Flag
                                                                    where   a.System_Rgstn_Status in ('C','R','E','W') and a.student_level_code not in ('HG', 'HU', 'TE')
                                                                      and a.Primary_Lvl_Flag='Y' and a.Frzn_Term_Seq_Id in (",paste(termid, collapse = ","),")
                                                                    ")
                                                   
                        )
                        dat1
                        
                
        }
        else {
                dat1<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select distinct Pid, Term_Seq_Id, Term_Code, Student_Level_Code,Time_Status, Class_Code, Hegis_Cohort_New,Lvl_Entry_Status
                                                                      from ",ds,".dbo.SISPLVT   
                                                                    where   System_Rgstn_Status in ('C','R','E','W') and student_level_code not in ('HG', 'HU', 'TE')
                                                                      and Primary_Lvl_Flag='Y' and Term_Seq_Id in (",paste(termid, collapse = ","),")
                                                                    ")
                )
                gndr <- gndr_race_pull(ds=ds, pidlist = unique(dat1$Pid))
                merge(dat1,gndr,by='Pid', all.x=T)
  
        }
        
        
        
}


#' Major admit to college
#'
#' admt.coll is a function to pull undergraduates all majors by term and major college from SISFull, and indicate whether major is admitted to college or not  
#' 
#' @param termid default is All to pull all available terms from SISFUll or a character vector to specify term to pull the major from
#' 
#' @return A data frame with enrolled Pids and major by term and admit to college status
#' 
#' @export
admt.coll <- function(pidlist, termid="All"){
  if(! exists('MSUDATA') )
    stop("You need to connect to msudata first using msudatacon")

  
  ls <- length(pidlist)
  lsg <- ceiling( ls/1000)
  pidp <- split(pidlist, ceiling(seq_along(pidlist)/1000))
  
  dat <- data.frame()
  for (i in seq(lsg)){
    pidchar<-paste(shQuote(pidp[[i]], type="csh"), collapse=", ")
    PAPL<-   RJDBC::dbGetQuery(MSUDATA, paste0( "select a.Pid,  a.Major_Code,a.Coll_Aplcn_Dcsn_Code, a.Coll_Efctv_Term_code,
                        t.Term_Seq_Id as CollEfctvTermId, a.Aplcn_End_Term_Code, t1.Term_Seq_Id as AplcnEndTermId,a.Student_Level_Code,
                                                a.Admit_Term_Code, m.coll_code, m.Long_Desc as mjr_desc, c.Full_Name as mjr_coll_name
                                                FROM SISFull.dbo.SISPAPL a
                                                inner join SISInfo.dbo.TERM t
                                                on a.Coll_Efctv_Term_code=t.Term_Code
                                                left join SISInfo.dbo.TERM t1
                                                on a.Aplcn_End_Term_Code=t1.Term_Code
                                                inner join SISInfo.dbo.MAJORMNT m
                                                on a.Major_Code=m.Major_Code 
                                                inner join SISInfo.dbo.College c 
                                                on m.Coll_Code = c.Coll_Code
                                                where Coll_Aplcn_Dcsn_Code = 'ADMT' 
                                                and a.Pid in (
                                                ",pidchar,") " ,sep="")
                               
    )
    PAPL <- PAPL %>% mutate(AplcnEndTermId = ifelse(is.na(AplcnEndTermId),9999, AplcnEndTermId))
    SISPMJR <- RJDBC::dbGetQuery(MSUDATA, paste0( "select p.*, m.Long_Desc as mjr_desc, c.coll_code, c.Full_Name as mjr_coll_name
                      from SISFull.dbo.SISPMJR p 
                       left join SISInfo.dbo.MAJORMNT m
                    on p.Major_Code=m.Major_Code 
                      left join SISInfo.dbo.College c 
                      on m.Coll_Code = c.Coll_Code
                      where Student_Level_Code='UN' and p.Pid in (
                      
                      ",pidchar,")" , sep=""))
    
    if (termid !='All'){
      SISPMJR <- SISPMJR %>% filter(Term_Seq_Id %in% termid)
    }
    
    dat1 <- SISPMJR %>% left_join(PAPL[,c('Pid','Major_Code','CollEfctvTermId','AplcnEndTermId','Coll_Aplcn_Dcsn_Code')], by=c('Pid','Major_Code'))%>%
      mutate(AdmitColl= ifelse(is.na(Coll_Aplcn_Dcsn_Code), 0,
                               ifelse(Term_Seq_Id>= CollEfctvTermId & Term_Seq_Id<=AplcnEndTermId,1,0 )))%>%
      group_by(Pid, Term_Seq_Id, Major_Code, Aplcn_Ref_Num)%>% mutate(maxad = max(AdmitColl))%>% ungroup()%>%  filter(maxad== AdmitColl)%>%
      select(-c(CollEfctvTermId,AplcnEndTermId,Time_Stamp))%>% unique()
    
    dat <- rbind(dat, dat1)
  }
  dat
    
  
  
}


