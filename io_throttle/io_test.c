#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#define BLOCK_SIZE 4096

int main(int argc, char *argv[]) {
  if (argc != 4) {
    fprintf(stderr, "Usage: %s <filename> <blocks> <cycles>\n", argv[0]);
    return 1;
  }

  int fd = open(argv[1], O_RDWR | O_CREAT, 0600);
  if (fd == -1) {
    perror("Unable to open file");
    return 1;
  }
  int blocks = atoi(argv[2]);
  int cycles = atoi(argv[3]);

  char buf[BLOCK_SIZE] = {0};

  int cycle;
  for (cycle = 0; cycle < cycles; ++cycle) {
    lseek(fd, 0, SEEK_SET);

    int block;
    for (block = 0; block <= blocks; ++block) {
      if (write(fd, buf, BLOCK_SIZE) != BLOCK_SIZE) {
        fprintf(stderr, "Short write\n");
        return 1;
      }
    }
    lseek(fd, 0, SEEK_SET);
    for (block = 0; block <= blocks; ++block) {
      if (read(fd, buf, BLOCK_SIZE) != BLOCK_SIZE) {
        fprintf(stderr, "Short read\n");
        return 1;
      }
    }
  }

  close(fd);

  return 0;
}
