library(testthat)
library(SparkR)

Sys.setenv(SPARKR_BACKEND_REUSE_CONN = "FALSE")
test_package("SparkR")
