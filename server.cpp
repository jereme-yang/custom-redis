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
#include <string>
#include <vector>
#include <map>

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
// TODO: O(n) implementation
static void buf_consume(std::vector<uint8_t> &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + n);
}

static void fd_set_nb(int fd) {
    errno = 0; // clear errno
    int flags = fcntl(fd, F_GETFL, 0); // grab the current file status for the given fd
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK; // make future io operations non-blocking
    // set the new flags
    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
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
Conn* handle_accept(int fd) {
    // accept a connection
    struct sockaddr_in client_addr = {};
    socklen_t addr_len = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr*)&client_addr, &addr_len);
    if (connfd < 0) {
        return NULL;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
    );

    // set the connection to non-blocking
    fd_set_nb(connfd);
    // create a new connection state
    Conn* conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true; // we want to read from the socket
    return conn;
}

const size_t k_max_args = 200 * 1000;

/*
read a uint32_t from the buffer.
return true if successful, false if not enough data.
*/
static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur + 4 > end) {
        return false;
    }
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

/*
read the str from the length prefix
returns false if the length prefix goes out of bounds
*/
static bool
read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out) {
    if (cur + n > end) {
        return false; // data bigger than end
    }
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

/* 
parse length prefixed commands

returns false if:
can't read nstr
can't read len prefixed strings

+------+-----+------+-----+------+-----+-----+------+
| nstr | len | str1 | len | str2 | ... | len | strn |
+------+-----+------+-----+------+-----+-----+------+
*/
static int32_t
parse_req(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_u32(data, end, nstr)) {
        return -1;
    }
    if (nstr > k_max_args) {
        return -1;  // safety limit
    }

    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(data, end, len)) {
            return -1;
        }
        out.push_back(std::string());
        if (!read_str(data, end, len, out.back())) {
            return -1;
        }
    }
    if (data != end) {
        return -1;  // trailing garbage
    }
    return 0;
}

// Response::status
enum {
    RES_OK = 0,
    RES_ERR = 1,    // error
    RES_NX = 2,     // key not found
};

struct Response {
    uint32_t status; // TODO: make this type-enforced to be the enum above
    std::vector<uint8_t> data;
};

// placeholder; implemented later
static std::map<std::string, std::string> g_data;


// TODO: optimize so that response data goes directly to Conn::outgoing
static void do_request(std::vector<std::string> &cmd,Response &out) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end()) {
            out.status = RES_NX;    // not found
            return;
        }
        const std::string &val = it->second;
        out.data.assign(val.begin(), val.end());
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        g_data[cmd[1]].swap(cmd[2]);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        g_data.erase(cmd[1]);
    } else {
        out.status = RES_ERR;       // unrecognized command
    }
}

static void make_response(const Response &resp, std::vector<uint8_t> &out) {
    uint32_t resp_len = 4 + (uint32_t)resp.data.size();
    buf_append(out, (const uint8_t *)&resp_len, 4);
    buf_append(out, (const uint8_t *)&resp.status, 4);
    buf_append(out, resp.data.data(), resp.data.size());
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

    // message header
    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), sizeof(len));
    if (len > k_max_msg) { // protocol error
        msg("protocol error: message too large");
        conn->want_close = true; // close the connection on error
        return false;
    }

    // message body
    if (4 + len > conn->incoming.size()) {
        return false;   // want read more bytes
    }

    const uint8_t *request = &conn->incoming[4];

    // 4. process the parsed msg
    // got one request, do some application logic
    std::vector<std::string> cmd;
    if (parse_req(request, len, cmd) < 0) {
        conn->want_close = true;
        return false;   // error
    }
    Response resp;
    do_request(cmd, resp);
    make_response(resp, conn->outgoing);

    // 5. remove the message from conn:incoming
    buf_consume(conn->incoming, 4 + len);
    return true; // successfully processed a request
}

static void handle_write(Conn* conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) {
        return; // actually not ready
    }
    if (rv < 0) {
        conn->want_close = true; // close the connection on error
        return;
    } 

    // remove written data from the outgoing buffer
    buf_consume(conn->outgoing, (size_t)rv);
    
    if (conn->outgoing.size() == 0) {
        conn->want_write = false; // no more data to write
        conn->want_read = true; // we can read more data
    } // else: keep writing
}

static void handle_read(Conn* conn) {
    // 1. do non-blocking read
    uint8_t buf[64 * 1024] = {}; // 64k buffer
    ssize_t rv = read(conn->fd, buf, sizeof(buf)); // note: this only reads avail.. data
    if (rv < 0 && errno == EAGAIN) {
        return; // actually not ready
    }
    // handle IO error
    if (rv < 0) {
        msg_errno("read() error");
        conn->want_close = true;
        return; // want close
    }
    // handle EOF
    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            msg("client closed");
        } else {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return; // want close
    }

    // 2. add new data to the Conn:incoming buffer
    buf_append(conn->incoming, buf, rv);
    // 3. Try to parse the accummulating buffer
    // 4. process the parsed data
    // remove the message from the buffer
    while(try_one_request(conn)) {}
    if (conn->outgoing.size() > 0) {
        conn->want_read = false; // we have data to write
        conn->want_write = true; // we have data to write
        return handle_write(conn); // write the data
    } // else: keep reading
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

    // set the listen fd to non-blocking mode
    fd_set_nb(fd);

    // Listen for incoming connections
    rv = listen(fd, SOMAXCONN);
    if (rv) { die("listen()"); }


    // create fd -> Conn mapping
    // On Unix, fd is mapped to the lowest non-negative integer
    // so this hashmap is just a vector
    std::vector<Conn*> fd2conn;

    std::vector<struct pollfd> poll_args;
    // the event loop
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
            if (Conn *conn = handle_accept(fd)) {
                
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
    }

	return 0;
}


