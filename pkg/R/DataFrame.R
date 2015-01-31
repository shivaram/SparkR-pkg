# DataFrame.R - DataFrame class and methods implemented in S4 OO classes

setOldClass("jobj")

#' @title S4 class that represents a DataFrame
#' @description DataFrames can be created using functions like
#'              \code{jsonFile}, \code{table} etc.
#' @rdname DataFrame
#' @seealso jsonFile, table
#'
#' @param env An R environment that stores bookkeeping states of the DataFrame
#' @param sdf A Java object reference to the backing Scala SchemaRDD
#' @export

setClass("DataFrame",
         slots = list(env = "environment",
                      sdf = "jobj"))

setMethod("initialize", "DataFrame", function(.Object, DataFrame, isCached, isCheckpointed) {
  .Object@env <- new.env()
  .Object@env$isCached <- isCached
  .Object@env$isCheckpointed <- isCheckpointed
  
  .Object@sdf <- DataFrame
  .Object
})

#' @rdname DataFrame
#' @export

dataFrame <- function(DataFrame, isCached = FALSE, isCheckpointed = FALSE) {
  new("DataFrame", DataFrame, isCached, isCheckpointed)
}

# The DataFrame accessor function

setGeneric("getsdf", function(df, ...) {standardGeneric("getsdf") })
setMethod("getsdf", signature(df = "DataFrame" ), function(df) df@sdf )

############################ DataFrame Methods ##############################################

#' Print Schema of a DataFrame
#' 
#' Prints out the schema in tree format
#' 
#' @param df A SparkSQL DataFrame
#' 
#' @rdname printSchema
#' @export

setGeneric("printSchema", function(df) { standardGeneric("printSchema") })

setMethod("printSchema",
          signature(df = "DataFrame"),
          function(df) {
            sdf <- getsdf(df)
            schemaString <- callJMethod(sdf, "printSchema")
            cat(schemaString)
          })

#' Register Temporary Table
#' 
#' Registers a DataFrame as a Temporary Table in the SQLContext
#' 
#' @param df A SparkSQL DataFrame
#' @param tableName A character vector containing the name of the table
#' 
#' @rdname registerTempTable
#' @export

setGeneric("registerTempTable", function(df, tableName) { standardGeneric("registerTempTable") })

setMethod("registerTempTable",
          signature(df = "DataFrame", tableName = "character"),
          function(df, tableName) {
            if (class(df) == "DataFrame") {
              sdf <- getsdf(df)
              callJMethod(sdf, "registerTempTable", tableName)    
            } else {
              stop("You must specify a DataFrame.")
            }
          })

#' Count
#' 
#' Returns the number of rows in a DataFrame
#' 
#' @param df A SparkSQL DataFrame
#' 
#' @rdname count
#' @export

setGeneric("count", function(df) { standardGeneric("count") })

setMethod("count",
          signature(df = "DataFrame"),
          function(df) {
            sdf <- getsdf(df)
            callJMethod(sdf, "count")
            }
          )

#' Collect elements of a DataFrame
#' 
#' Returns a list of Row objects from a DataFrame
#' 
#' @param df A SparkSQL DataFrame
#' 
#' @rdname collect-methods
#' @export
setGeneric("collect", function(df) { standardGeneric("collect") })

setMethod("collect",
          signature(df = "DataFrame"),
          function(df){
            sdf <- getsdf(df)
            list_obj <- callJMethod(sdf, "collect")
          })

# TODO: Add collect partition