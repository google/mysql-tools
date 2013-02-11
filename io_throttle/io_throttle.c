#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <dlfcn.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/uio.h>

#define NS_IN_SEC 1000000000

// % of a second we can spend in IO
static double quota_fraction;

// Estimate how long we need to nanosleep() for us to maintain our given DTF
static inline uint64_t estimate_wait_for_resources(
    const uint64_t resources_needed) {
  return resources_needed / quota_fraction - resources_needed;
}


// Sleep for the specified period of time
static inline void throttle_sleep(uint64_t nsecs) {
  struct timespec sleep_time;
  sleep_time.tv_sec = nsecs / NS_IN_SEC;
  sleep_time.tv_nsec = nsecs % NS_IN_SEC;
  while (clock_nanosleep(CLOCK_MONOTONIC, 0, &sleep_time, &sleep_time));
}


// Calculate the elapsed ns between the two timespecs
static inline uint64_t elapsed_ns(
    const struct timespec * const ts_start,
    const struct timespec * const ts_finish) {
  return (ts_finish->tv_sec - ts_start->tv_sec) * NS_IN_SEC
      + (ts_finish->tv_nsec - ts_start->tv_nsec);
}


// Operations to perform after all intercepted IO goes here.
static inline void post_io_teardown(
    const struct timespec * const ts_start) {
  struct timespec ts_finish;
  clock_gettime(CLOCK_MONOTONIC, &ts_finish);

  static __thread int64_t bank = 0;

  uint64_t sleep_time = estimate_wait_for_resources(
      elapsed_ns(ts_start, &ts_finish));

  bank -= sleep_time;
  if (bank < 0) {
    struct timespec sleep_start, sleep_finish;
    clock_gettime(CLOCK_MONOTONIC, &sleep_start);
    throttle_sleep(-bank);
    clock_gettime(CLOCK_MONOTONIC, &sleep_finish);
    bank += elapsed_ns(&sleep_start, &sleep_finish);
  }
}


// ================================
// BEGINNING OF FUNCTION INTERCEPTS
// ================================

static ssize_t (*libc_write)(int fd, const void *buf, size_t len);

ssize_t write(int fd, const void *buf, size_t len) {
  ssize_t ret;
  struct timespec ts_start;

  clock_gettime(CLOCK_MONOTONIC, &ts_start);
  ret = (*libc_write)(fd, buf, len);
  post_io_teardown(&ts_start);
  return ret;
}


static ssize_t (*libc_pwrite)(int fd, const void *buf, size_t nbytes,
                              off_t offset);

ssize_t pwrite(int fd, const void *buf, size_t nbytes, off_t offset) {
  ssize_t ret;
  struct timespec ts_start;

  clock_gettime(CLOCK_MONOTONIC, &ts_start);
  ret = (*libc_pwrite)(fd, buf, nbytes, offset);
  post_io_teardown(&ts_start);
  return ret;
}


static ssize_t (*libc_writev)(int fd, const struct iovec *iov, int iovcnt);

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
  ssize_t ret;
  struct timespec ts_start;

  clock_gettime(CLOCK_MONOTONIC, &ts_start);
  ret = (*libc_writev)(fd, iov, iovcnt);
  post_io_teardown(&ts_start);
  return ret;
}


static ssize_t (*libc_read)(int fd, void *buf, size_t nbytes);

ssize_t read(int fd, void *buf, size_t nbytes) {
  ssize_t ret;
  struct timespec ts_start;

  clock_gettime(CLOCK_MONOTONIC, &ts_start);
  ret = (*libc_read)(fd, buf, nbytes);
  post_io_teardown(&ts_start);
  return ret;
}


static ssize_t (*libc_pread)(int fd, void *buf, size_t nbytes,
                             off_t offset);

ssize_t pread(int fd, void *buf, size_t nbytes, off_t offset) {
  ssize_t ret;
  struct timespec ts_start;

  clock_gettime(CLOCK_MONOTONIC, &ts_start);
  ret = (*libc_pread)(fd, buf, nbytes, offset);
  post_io_teardown(&ts_start);
  return ret;
}


static ssize_t (*libc_readv)(int fd, const struct iovec *iov, int iovcnt);

ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
  ssize_t ret;
  struct timespec ts_start;

  clock_gettime(CLOCK_MONOTONIC, &ts_start);
  ret = (*libc_readv)(fd, iov, iovcnt);
  post_io_teardown(&ts_start);
  return ret;
}

// ==========================
// END OF FUNCTION INTERCEPTS
// ==========================


// Initialize all necessary variables.
void _init() {
  libc_write = dlsym(RTLD_NEXT, "write");
  libc_pwrite = dlsym(RTLD_NEXT, "pwrite");
  libc_writev = dlsym(RTLD_NEXT, "writev");
  libc_read = dlsym(RTLD_NEXT, "read");
  libc_pread = dlsym(RTLD_NEXT, "pread");
  libc_readv = dlsym(RTLD_NEXT, "readv");

  char * fraction = getenv("QUOTA_FRACTION");

  if (fraction == NULL) {
    fprintf(stderr,
            "You must provide a environment variable QUOTA_FRACTION\n");
    exit(1);
  }

  quota_fraction = atof(fraction);

  if (quota_fraction <= 0.0 || quota_fraction > 1.0) {
    fprintf(stderr, "Invalid QUOTA_FRACTION\n");
    exit(1);
  }
}
