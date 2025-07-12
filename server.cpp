// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
// system
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
// C++
#include <vector>

static void msg(const char* msg) {
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char* msg) {
    fprintf(stderr, "[errno: %d] %s\n", errno, msg);
}

static void die(const char* msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

// append to the back
static void
buf_append(std::vector<uint8_t> &buf, const uint8_t *data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

// remove from the front
static void buf_consume(std::vector<uint8_t> &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + n);
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

const size_t k_max_msg = 32 << 20;  // likely larger than the kernel buffer

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

/*
make accept non-blocking
*/
Conn* handle_accept(int fd, std::vector<Conn*>& fd2conn) {
    // accept a connection
    struct sockaddr_in client_addr = {};
    socklen_t addr_len = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr*)&client_addr, &addr_len);
    if (connfd < 0) {
        return NULL;
    }
    // set the connection to non-blocking
    fd_set_nb(connfd);
    // create a new connection state
    Conn* conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true; // we want to read from the socket
    return conn;
}


Conn* handle_read(Conn* conn) {
    // 1. do non-blocking read
    uint8_t buf[64 * 1024] = {}; // 64k buffer
    ssize_t rv = read(conn->fd, buf, sizeof(buf)); // note: this only reads avail.. data
    // recall that sockets fill up the buffer as data arrives
    if (rv <= 0) {
        // handle IO error (r < 0) or EOF (r == 0)
        conn->want_close = true; // close the connection
    }

    // 2. add new data to the Conn:incoming buffer
    buf_append(conn->incoming, buf, rv);
    // 3. Try to parse the accummulating buffer
    // 4. process the parsed data
    // remove the message from the buffer
    return;
}

Conn* handle_write(Conn* conn) {
    // TODO: implement
    return;
}

static void fd_set_nb(int fd) {
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
}

/*
handle a request. if there is enough in the buffer, it will
do something. Otherwise, it will wait for future iteration
*/
static bool try_one_request(Conn* conn) {
    // 3. try to parse the acumulating buffer
    // if the header isn't complete, return false
    if (conn->incoming.size() < 4) {
        return false; // not enough data to parse
    }

    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), sizeof(len));
    if (len > k_max_msg) { // protocol error
        conn->want_close = true; // close the connection on error
        return false;
    }

    const uint8_t *request = &conn->incoming[4];

    // 4. process the parsed msg
    // ...
    // echo the response back
    buf_append(conn->outgoing, (const uint8_t*)&len, 4);
    buf_append(conn->outgoing, request, len);

    // 5. remove the message from conn:incoming
    buf_consume(conn->incoming, 4 + len);
    return true; // successfully processed a request
}

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


    // create fd -> Conn mapping
    // On Unix, fd is mapped to the lowest non-negative integer
    // so this hashmap is just a vector
    std::vector<Conn*> fd2conn;

    std::vector<struct pollfd> poll_args;
    // Accept a connection
    while (true) {
        // Prepare poll arguments
        poll_args.clear();

        // put the listening socket in the poll arguments
        /*
        struct pollfd {
            int   fd;
            short events;   // request: want to read, write, or both?
            short revents;  // returned: can read? can write?
        };
        */
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);

        // the rest are connections
        for (Conn* conn : fd2conn) {
            if (!conn) {
                continue; // skip null connections
            }

            // create pollfd for each connection
            struct pollfd pfd = {conn->fd, POLLERR, 0};
            // parse conn intent -> poll flags
            if (conn->want_read) {
                pfd.events |= POLLIN;
            }
            if (conn->want_write) {
                pfd.events |= POLLOUT;
            }
            poll_args.push_back(pfd);
        }

        // wait for readiness
        // poll takes in a list of fds the program wants to do IO on
        // it returns when one of the fds is ready for IO
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), -1);
        if (rv < 0 && errno != EINTR) {
            continue;
        }
        if (rv < 0) {
            die("poll() failed");
        }

        // handle the listening (accepting) socket
        if (poll_args[0].revents) {
            if (Conn *conn = handle_accept(fd, fd2conn)) {
                
                // put it into the fd2conn mapping
                if (fd2conn.size() <= (size_t)conn->fd) {
                    fd2conn.resize(conn->fd + 1);
                }
                fd2conn[conn->fd] = conn;
            }
        }

        // the rest of the fds are connections
        // go through the fds our poll API returned
        for (size_t i = 1; i < poll_args.size(); ++i) {
            uint32_t ready = poll_args[i].revents;

            // map returned fd to Conn state holder
            Conn* conn = fd2conn[poll_args[i].fd];

            // apply correct application logic
            if (ready & POLLIN) {
                handle_read(conn);
            }
            if (ready & POLLOUT) {
                handle_write(conn);
            }

            // close the socket from socket error or application intent
            if ((ready & POLLERR) || (conn->want_close)) {
                (void)close(conn->fd); // close the socket API
                fd2conn[conn->fd] = nullptr; // remove from fd2conn mapping
                delete conn; // free the connection state
            }
        }


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


