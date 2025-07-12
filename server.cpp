#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

void die(const char* msg) {
    perror(msg);
    exit(1);
}


// Simple echo handler for demonstration
void do_read_write(int connfd) {
    char rbuf[64] = {};
    ssize_t n = read(connfd, rbuf, sizeof(rbuf));
    if (n < 0) {
        perror("read() failed");
        return;
    }
    printf("Received: %s\n", rbuf);

    char wbuf[] = "world";
    write(connfd, wbuf, sizeof(wbuf));
}

/*
This is necessary for event looping because we aren't blocking
for read/write operations. We need to keep track of the state
of a connection if it lasts multiple loop iterations.
This is a simple structure to hold the connection state.
*/
struct Conn {
    // socket file descriptor
    int fd;

    // application's intention
    bool want_read = false; // represents the fd list for the readiness API
    bool want_write = false; // represents the fd list for the readiness API
    bool want_close = false; // tells the event loop to close the connection

    // buffered input and output
    std::vector<uint8_t> incoming; // buffers data for the socket for the parser
    std::vector<uint8_t> outgoing; // buffers generated data that are written to the socket
};


int main() {
	// Obtain socket handle
	int fd = socket(AF_INET, SOCK_STREAM, 0);

	// Set socket options
	int val = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    // SO_REUSEADDR should always be used on listening sockets
    // to allow the socket to be bound to an address even after restarting

    struct sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(1234); // port number
    server_addr.sin_addr.s_addr = htonl(0); // IP 0.0.0.0
    int rv = bind(fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (rv) { die("bind()"); }

    // Listen for incoming connections
    rv = listen(fd, SOMAXCONN);
    if (rv) { die("listen()"); }

    // Accept a connection
    while (true) {
        struct sockaddr_in client_addr = {};
        socklen_t addr_len = sizeof(client_addr);
        int connfd = accept(fd, (struct sockaddr*)&client_addr, &addr_len);
        if (connfd < 0) {
            continue; // Continue to accept next connection
        }

        // Handle the client connection here
        // For example, read/write data using client_fd
        do_read_write(connfd);
        // Close the client socket after handling
        close(connfd);
    }
	// Close socket before exiting
	close(fd);

	return 0;
}


