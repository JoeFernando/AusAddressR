#' buildGNAF
#'
#' Builds the SQLite database that will house the GNAF data used to geocode
#'
#' @param path The path to GNAF data directory
#'
#' @return boolean
#' @export
#'
#' @examples
#' buildGNAF('~/GNAF')
buildGNAF <- function(gnafPath, writePath=NULL){
  require(RSQLite)
  tryCatch(
    {
      m = simpleMessage('Unzipped')
      if(grep('\\.zip$',gnafPath)){
        unzip(gnafPath,exdir=writePath)
        writePathDir = paste0(writePath,'/GNAF')
        if(!dir.exists(writePathDir)){
          dir.create(writePathDir)
        }
        unzip(gnafPath,exdir=writePathDir)
      }
    }
  ,error = simpleError('Failed to unzip')
  ,finally = function(m) m
  )
  db = dbConnect(SQLite(),'GNAF.db')



  return(TRUE)
}
