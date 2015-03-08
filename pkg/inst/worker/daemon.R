# Worker daemon

rLibDir <- Sys.getenv("SPARKR_RLIBDIR")
script <- paste(rLibDir, "SparkR/worker/worker.R", sep="/")

# preload SparkR package, speedup worker
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))

inputCon <- socketConnection(host = "localhost",
                             port = port,
                             blocking = TRUE,
                             open = "rb",
                             timeout = 6000,
                             server = FALSE)

# Read from connection, retrying to read exactly 'size' bytes
readWithRetry <- function(con, size) {
  data <- readBin(con, raw(), size, endian = "big")
  # Since this function is called after
  # calling `select`, 0 bytes read implies socket is closed
  if (length(data) == 0) {
    return(data)
  }
  while (length(data) < size) {
    bytesToRead = size - length(data)
    extra <- readBin(con, raw(), bytesToRead, endian = "big")
    data <- c(data, extra)
  }
  stopifnot(length(data) == size)
  data
}

# Utility function to read the port with retry
# Returns -1 if the socket was closed
readIntPort <- function(con) {
  data <- readWithRetry(con, 4L)
  if (length(data) != 4) {
    -1
  } else {
    rc <- rawConnection(data)
    ret <- SparkR:::readInt(rc)
    close(rc)
    ret
  }
}

while (TRUE) {
  ready <- socketSelect(list(inputCon), timeout = 5)
  if (ready) {
    inport <- readIntPort(inputCon)
    if (inport < 0) {
      cat("quitting daemon\n")
      q(save="no")
    } else {
      #cat("got port ", inport, "\n")
      p <- parallel:::mcfork()
      if (inherits(p, "masterProcess")) {
        close(inputCon)
        # Set SIGUSR1 so that child can exit
        tools::pskill(Sys.getpid(), tools::SIGUSR1)
        Sys.setenv(SPARKR_WORKER_PORT = inport)
        source(script)
        parallel:::mcexit(0L)
      }
    }
  }
}
