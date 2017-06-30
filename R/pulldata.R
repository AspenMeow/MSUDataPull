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