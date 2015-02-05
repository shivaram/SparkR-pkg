
# Collect all the elements of a DataFrame on the JVM, deserialize in R, and return an R data.frame
collect_df <- function(df) { #TODO: Change the name once we add it as an S4 method
  listCols <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "dfToRDD", df@sdf)
  cols <- lapply(seq_along(listCols),
                 function(colIdx) {
                   objRaw <- rawConnection(listCols[[colIdx]])
                   numRows <- (readInt(objRaw))
                   col <- readCol(objRaw, numRows)
                   # TODO: How can I close the connection once the loop is done (but not before)?
                 })
  colNames <- getColNames(df)
  names(cols) <- colNames
  dfOut <- do.call(cbind.data.frame, cols)
}

# Take a single column as Array[Byte] and deserializes it into an atomic vector
readCol <- function(rawCon, numRows) {
  sapply(1:numRows,
         function(x) {
           value <- readObject(rawCon)
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
