% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pulldata.R
\name{rsadress_pull}
\alias{rsadress_pull}
\title{Residence Adress Country State County}
\usage{
rsadress_pull(ds = "SISFrzn", ex = "QRTRTERM", pidlist)
}
\arguments{
\item{ds}{A character string to indicate the SIS source  SISFrzn or SISFull or SISInfo}

\item{pidlist}{A character vector of Pids}
}
\value{
A data frame with Pids, student_level_code and entry Term residence address country county state
}
\description{
rsadress_pull is a function to pull the residence address from prsn default ds shows address in entry term
}
