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
buildGNAF <- function(gnafPath, writePath=NULL, Overwrite = F){
  require(MonetDBLite)
  require(DBI)
  tryCatch(
    {
      writePathDir = paste0(writePath,'/GNAF')
      if(grep('\\.zip$',gnafPath)){
        if(!dir.exists(writePathDir)){
          dir.create(writePathDir)
        }
        print('Unzipping: May take a while...')
        unzip(gnafPath,exdir=writePathDir)
      }
      db = dbConnect(MonetDBLite(),paste0(writePathDir,'/GNAF.db'))
    }
  ,finally = {
    print('Hooray: Directory created...')
    dbDisconnect(db)
  }
  )

  return(TRUE)
}



#' createTables
#'
#' Creates GNAF table structure
#'
#' @param GNAFDirectory
#'
#' @return T
#' @export
#'
#' @examples
createTables <- function(GNAFDirectory, Overwrite = F){
  require(MonetDBLite)
  require(DBI)
  tryCatch({
  if(!dir.exists(paste0(GNAFDirectory,"/GNAF.db"))){
    stop('Database needs to be created')
  }

  createTablesScriptLocation = list.files(GNAFDirectory,pattern='create_tables_ansi.sql',recursive = T,ignore.case = T)
  createKeysScriptLocation = list.files(GNAFDirectory,pattern='add_fk_constraints.sql',recursive = T,ignore.case = T)
  print(createTablesScriptLocation)
  print(createKeysScriptLocation)
  if(length(createTablesScriptLocation)!=1){
    stop('More than one create tables script!')
  }
  if(length(createKeysScriptLocation)!=1){
    stop('More than one create keys script!')
  }
  print('We good. Doing stuff now...')
  files = c(createTablesScriptLocation,createKeysScriptLocation
            )
  query = readLines(paste0(GNAFDirectory,"/",createTablesScriptLocation))

  queryStub = ''
  for(file in files)
  for(line in query){
    queryStub = paste0(queryStub,line)
    if(grepl(";",queryStub)){
      if(grepl("DROP TABLE",queryStub) && Overwrite == F){
        queryStub = ''
      }else{
        tryCatch({
          db = dbConnect(MonetDBLite(),paste0(GNAFDirectory,"/GNAF.db"))
          dbSendQuery(db, queryStub)
          dbDisconnect(db)
        }, error = function(err){
          print(err)
          print('Pushing through...')
        }, finally = {
          queryStub = ''
        })
      }
    }
  }


  db = dbConnect(MonetDBLite(),paste0(GNAFDirectory,"/GNAF.db"))
  print(DBI::dbListTables(db))
  dbDisconnect(db)
  }
  , warning = function(war){
    print(war)
    print('Exit warning!')
  }
  , error = function(err){
    print(err)
    print('Exit error!')
  }
  )
  return(TRUE)
}

#' Title
#'
#' @param GNAFDirectory
#'
#' @return
#' @export
#'
#' @examples
insertData <- function(GNAFDirectory){

}


