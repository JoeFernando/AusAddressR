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
#' buildGNAF('~/Downloads/MAY16_GNAF+EULA_PipeSeparatedValue_20160523140820.zip'
#'            ,writePath = '~/Downloads'
#'            ,Overwrite = T)
buildGNAF <- function(gnafPath, writePath=NULL, Overwrite = F){
  require(MonetDBLite)
  require(DBI)
  tryCatch(
    {
      writePathDir = paste0(writePath,'/GNAF')
      if(grepl('\\.zip$',gnafPath)){
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
#' #' createTables('~/Downloads/GNAF', overwrite = T, tablesFlag = T, keysFlag = T)
createTables <- function(GNAFDirectory, overwrite = F, tablesFlag = T, PKFlag = T, FKFlag = F){
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
  files = c(createTablesScriptLocation
            ,createKeysScriptLocation
            )


  queryStub = ""
  for(file in files){
    query = readLines(paste0(GNAFDirectory,"/",file))
    for(line in query){
      if(grepl("--",line)){
        line = ""
      }

      queryStub = paste0(queryStub,line)

      if(grepl(";",queryStub)){
        if(grepl("DROP TABLE",queryStub) & !overwrite){
          queryStub = ''
        }else if(grepl("PRIMARY KEY",queryStub)&!PKFlag){
          queryStub = ''
        }else if(grepl("FOREIGN KEY", queryStub)&!FKFlag){
          queryStub = ''
        }
        else{
          tryCatch({
            print(queryStub)
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

#' insert Data
#'
#' @param GNAFDirectory
#'
#' @return T
#' @export
#'
#' @examples
#' #'insertData('~/Downloads/GNAF')
insertData <- function(GNAFDirectory, dataTables = NULL){
  require(MonetDBLite)
  require(DBI)
  dataExt = list.files(GNAFDirectory,pattern='.psv',recursive = T,ignore.case = T)

  dataExtSearch = substr(dataExt,regexpr("/[^/]*$",dataExt),nchar(dataExt))
  dataExtSearch2 = substr(dataExtSearch,regexpr("_",dataExtSearch),nchar(dataExtSearch))

  if(is.null(dataTables)){
    db = dbConnect(MonetDBLite(),paste0(GNAFDirectory,"/GNAF.db"))
    dataTables = dbListTables(db)
    dbDisconnect(db)
  }

  for(table in dataTables){
    print(table)
    print(paste0("_",table,"_psv.psv"))

    if(grepl("_AUT_psv.psv",table)){
      dataExtTable = dataExt[grepl(paste0("^_Code_",table,"_psv.psv"),dataExtSearch2,ignore.case=T)]
    }else{
      dataExtTable = dataExt[grepl(paste0("^_",table,"_psv.psv"),dataExtSearch2,ignore.case=T)]
    }

    db = dbConnect(MonetDBLite(),paste0(GNAFDirectory,"/GNAF.db"))
    DBI::dbSendQuery(db,paste0('delete from ',table))
    dbDisconnect(db)
    for(file in dataExtTable){
      print(file)
      print('Size')
      print(file.size(paste0(GNAFDirectory,"/",file)))
      ptm <- proc.time()
      db = dbConnect(MonetDBLite(),paste0(GNAFDirectory,"/GNAF.db"))
      monetdb.read.csv(conn = db
                     ,files = paste0(GNAFDirectory,"/",file)
                     ,tablename = table
                     ,header = T
                     ,delim = "|"
                     ,quote = ""
                     ,create = F

      )
      dbDisconnect(db)
      print('Time')
      print(proc.time()-ptm)
    }

  }
}


