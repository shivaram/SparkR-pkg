
# Collect all the elements of a DataFrame on the JVM, deserialize in R, and return an R data.frame
collect_df <- function(df) {
  listCols <- SparkR:::callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "dfToRDD", df@sdf)
  cols <- lapply(seq_along(listCols),
                 function(colIdx) {
                   rddCol <- listCols[[colIdx]]
                   rddRaw <- SparkR:::callJMethod(rddCol, "collect") # Returns a list of byte arrays per partition
                   colPartitions <- lapply(seq_along(rddRaw),
                                           function(partIdx) {
                                             objRaw <- rawConnection(rddRaw[[partIdx]])
                                             numRows <- SparkR:::readInt(objRaw)
                                             col <- readCol(objRaw, numRows) # List of deserialized values per partition
                                             #close(objRaw)
                                             # TODO: How can I close the connection once the loop is done (but not before)?
                                           })
                   colOut <- unlist(colPartitions, recursive = FALSE) # Flatten column list into a vector
                 })
  colNames <- SparkR:::callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "getColNames", df@sdf)
  names(cols) <- colNames
  dfOut <- do.call(cbind.data.frame, cols)
}

# Take a single column as Array[Byte] and deserialize it into an atomic vector
readCol <- function(rawCon, numRows) {
  sapply(1:numRows,
         function(x) {
           value <- SparkR:::readObject(rawCon)
           # Replace NULL with NA so we can coerce to vectors
           if (is.null(value)) {
             value <- NA
           }
           value
         })
}

# Return a list of column names from the DataFrame schema
getColNames <- function(df) {
  colNames <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "getColNames", df@sdf)
}
