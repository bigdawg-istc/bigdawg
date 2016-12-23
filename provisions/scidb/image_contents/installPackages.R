###########################################################
# INSTALL PACKAGES
#Rscript installPackages.R packages=RCurl,snow,ptw,bitops,mapdata,XML,rgeos,rgdal,MODIS,scidb verbose=0 quiet=0
###########################################################
repositories <- c("http://cran.us.r-project.org", 
           "http://cran.r-mirror.de/", 
           "http://www.laqee.unal.edu.co/CRAN/", 
           "http://ftp.iitm.ac.in/cran/",
           "http://cran.mirror.ac.za/",
           "http://cran.ms.unimelb.edu.au/", 
           "http://R-Forge.R-project.org")

#Get arguments
argsep <- "="
keys <- vector(mode = "character", length = 0)
values <- vector(mode = "character", length = 0)

for (arg in commandArgs()){
  if(agrep(argsep, arg) == TRUE){
    pair <- unlist(strsplit(arg, argsep))
    keys <- append(keys, pair[1], after = length(pair))
    values <- append(values, pair[2], after = length(pair))
  }
}		  

packages <- unlist(strsplit(values[which(keys == "packages")],  ","))
#repositories <- unlist(strsplit(values[which(keys == "repositories")], ","))
verbose <- as.numeric(values[which(keys == "verbose")])
quiet <- as.numeric(values[which(keys == "quiet")])

install.packages(pkgs = packages, repos = repositories, verbose = verbose, quiet = quiet)
