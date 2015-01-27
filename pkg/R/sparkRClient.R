# Client code to connect to SparkRBackend

# Creates a SparkR client connection object
# if one doesn't already exist
connectBackend <- function(hostname, port, timeout = 60) {
  if (exists(".sparkRcon", envir = .sparkREnv)) {
    cat("SparkRBackend client connection already exists\n")
    return(get(".sparkRcon", envir = .sparkREnv))
  }

  con <- socketConnection(host = hostname, port = port, server = FALSE,
                          blocking = TRUE, open = "wb", timeout = timeout)

  # Set TCP NODELAY for sockets on Unix-like machines.
  if (.Platform$OS.type == "unix") {
    # Get the FD for this socket connection
    sockfd <- getFD(hostname, port)
    if (sockfd > 0) {
      ret <- .Call("setTcpNoDelay", sockfd)
      if (ret == 1) {
        cat("Successfully set TCP NODELAY on ", sockfd, "\n")
      }
    }
  }

  assign(".sparkRCon", con, envir = .sparkREnv)
  con
}

getFD <- function(hostname, port) {
  lsofArg <- paste("-i@", hostname, ":", port, sep="")
  tryCatch({
     lsofOut <- system2("lsof", lsofArg, stdout = T)
     # Get the row corresponding to R's pid
     # and then get the 4th column which is the FD
     fdStr <- strsplit(lsofOut[grep(Sys.getpid(), lsofOut)], "[ ]+")[[1]][4]
     return(as.integer(gsub("[a-zA-Z]+", "", fdStr)))
  }, warning = function(warn) {
     cat("Could not find socket descriptor\n")
     return(-1)
  }, error = function(err) {
     cat("Could not find socket descriptor\n")
     return(-1)
  })
}


# Launch the SparkR backend using a call to 'system2'.
launchBackend <- function(
    classPath, 
    mainClass, 
    args, 
    javaOpts = "-Xms2g -Xmx2g",
    javaHome = Sys.getenv("JAVA_HOME")) {
  if (.Platform$OS.type == "unix") {
    java_bin_name = "java"
  } else {
    java_bin_name = "java.exe"
  }

  if (javaHome != "") {
    java_bin <- file.path(javaHome, "bin", java_bin_name)
  } else {
    java_bin <- java_bin_name
  }
  combinedArgs <- paste(javaOpts, "-cp", classPath, mainClass, args, sep = " ")
  cat("Launching java with command ", java_bin, " ", combinedArgs, "\n")
  invisible(system2(java_bin, combinedArgs, wait = F))
}
