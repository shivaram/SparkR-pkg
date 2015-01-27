/**
 * A C function for SparkR that sets socket options 
 */

#include <R.h>
#include <Rinternals.h>

#ifndef WIN32
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h> // TCP_NODELAY
#endif

/* for compatibility with R before 3.1 */
#ifndef IS_SCALAR
#define IS_SCALAR(x, type) (TYPEOF(x) == (type) && XLENGTH(x) == 1)
#endif

SEXP setTcpNoDelay(SEXP fd) {
  int val = -1;
  int nodelay = 1;

  if (!IS_SCALAR(fd, INTSXP)) {
    error("invalid input");
  }

#ifndef WIN32
  int s = INTEGER(fd)[0];
  socklen_t len = sizeof(int);

  val = (int)(setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const void *)&nodelay, len) == 0);
  if (val == 0) {
    error("Error setting TCP_NODELAY");
  }
#endif

  return ScalarInteger(val);
}
