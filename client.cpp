#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
void die(const char* msg) {
    perror(msg);
    exit(1);
}

int main() {
    // Obtain socket handle
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket() failed");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK); // 127.0.0.1
    int rv = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (rv) {
        die("connect() failed");
    }

    char msg[] = "hello";
    write(fd, msg, sizeof(msg));

    char rbuf[64] = {};
    ssize_t n = read(fd, rbuf, sizeof(rbuf)-1);
    if (n < 0) {
        perror("read() failed");
    }
    printf("Received: %s\n", rbuf);
    close(fd);
}