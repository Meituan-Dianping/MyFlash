#include <poll.h>
#include <errno.h>
#define CLIENT_NET_READ_TIMEOUT         365*24*3600     /* Timeout on read */
#define CLIENT_NET_WRITE_TIMEOUT        365*24*3600     /* Timeout on write */

/* Data may be read. */
#define POLL_SET_IN      (POLLIN | POLLPRI)
/* Data may be written. */
#define POLL_SET_OUT     (POLLOUT)
/* An error or hangup. */
#define POLL_SET_ERR     (POLLERR | POLLHUP | POLLNVAL)

#define socket_errno    errno
#define closesocket(A)  close(A)
#define SOCKET_EINTR    EINTR
#define SOCKET_EAGAIN   EAGAIN
#define SOCKET_EWOULDBLOCK EWOULDBLOCK
#define SOCKET_EADDRINUSE EADDRINUSE
#define SOCKET_ETIMEDOUT ETIMEDOUT
#define SOCKET_ECONNRESET ECONNRESET
#define SOCKET_ENFILE   ENFILE
#define SOCKET_EMFILE   EMFILE


#define SOCKET_ERROR  ((size_t) -1)

enum enum_socket_io_event
{
  SOCKET_IO_EVENT_READ,
  SOCKET_IO_EVENT_WRITE,
  SOCKET_IO_EVENT_CONNECT
};

int io_wait(int sockFd, enum enum_socket_io_event event, int timeout)
{
  int ret;

  struct pollfd pfd;
  //my_socket sd= mysql_socket_getfd(vio->mysql_socket);



  memset(&pfd, 0, sizeof(pfd));

  pfd.fd= sockFd;

  /*
    Set the poll bitmask describing the type of events.
    The error flags are only valid in the revents bitmask.
  */
  switch (event)
  {
  case SOCKET_IO_EVENT_READ:
    pfd.events= POLL_SET_IN;

    break;
  case SOCKET_IO_EVENT_WRITE:
  case SOCKET_IO_EVENT_CONNECT:
    pfd.events= POLL_SET_OUT;
    break;
  }


  /*
    Wait for the I/O event and return early in case of
    error or timeout.
  */
  switch ((ret= poll(&pfd, 1, timeout)))
  {
  case -1:   /* On error, -1 is returned. */
    break;
  case 0:
    /*
      Set errno to indicate a timeout error.
      (This is not compiled in on WIN32.)
    */
    errno= SOCKET_ETIMEDOUT;
    break;
  default:
    /* Ensure that the requested I/O event has completed. */
    assert(pfd.revents & revents);
    break;
  }

  return ret;
}


int socket_io_wait(int socketFd, enum enum_socket_io_event event)
{
  int timeout, ret;

  assert(event == SOCKET_IO_EVENT_READ || event == SOCKET_IO_EVENT_WRITE);

  /* Choose an appropriate timeout. */
  if (event == VIO_IO_EVENT_READ)
    timeout= CLIENT_NET_READ_TIMEOUT;
  else
    timeout= CLIENT_NET_WRITE_TIMEOUT;

  /* Wait for input data to become available. */
  switch (io_wait(socketFd, event, timeout))
  {
  case -1:
    /* Upon failure, vio_read/write() shall return -1. */
    ret= -1;
    break;
  case  0:
    /* The wait timed out. */
    ret= -1;
    break;
  default:
    /* A positive value indicates an I/O event. */
    ret= 0;
    break;
  }

  return ret;
}



inline gsize inline_socket_sendto
(int socketFd, const void *buf, gsize n, int flags, const struct sockaddr *addr, int addr_len)
{
  gsize result;
  result= sendto(socketFd, buf,  n, flags, addr, addr_len);

  return result;
}


size_t socket_write(int socketFd, const uchar* buf, size_t size)
{
  ssize_t ret;
  int flags= 0;


  /* If timeout is enabled, do not block. */
  flags= MSG_DONTWAIT;

  while ((ret= inline_socket_sendto(socketFd, (void *)buf, size, flags)) == -1)
  {
    int error= socket_errno;

    /* The operation would block? */
    if (error != SOCKET_EAGAIN && error != SOCKET_EWOULDBLOCK)
      break;

    /* Wait for the output buffer to become writable.*/
    if ((ret= socket_io_wait(socketFd, VIO_IO_EVENT_WRITE)))
      break;
  }

  return ret;
}

my_bool
socket_should_retry(int socketFd,uint *retry_count MY_ATTRIBUTE((unused)))
{
  return (errno == SOCKET_EINTR);
}

my_bool
socket_was_timeout(int socketFd)
{
  return (errno == SOCKET_ETIMEDOUT);
}

static my_bool
net_write_raw_loop(int socketFd, const uchar *buf, size_t count)
{
  unsigned int retry_count= 0;

  while (count)
  {
    size_t sentcnt= socket_write(socketFd, buf, count);

    /* VIO_SOCKET_ERROR (-1) indicates an error. */
    if (sentcnt == SOCKET_ERROR)
    {
      /* A recoverable I/O error occurred? */
      if (net_should_retry(net, &retry_count))
        continue;
      else
        break;
    }

    count-= sentcnt;
    buf+= sentcnt;
  }

  /* On failure, propagate the error code. */
  if (count)
  {
    /* Socket should be closed. */
    net->error= 2;

    /* Interrupted by a timeout? */
    /*
    if (socket_was_timeout(net->vio))
      net->last_errno= ER_NET_WRITE_INTERRUPTED;
    else
      net->last_errno= ER_NET_ERROR_ON_WRITE;
    */
  }

  return count;
}

my_bool
net_write_packet(int sockFd, const uchar *packet, size_t length)
{
  my_bool res;


  /* Socket can't be used */
  if (net->error == 2)
    return TRUE;

  //net->reading_or_writing= 2;


  res= net_write_raw_loop(sockFd, packet, length);



  //net->reading_or_writing= 0;

  return res;
}


my_bool net_init(NET *net)
{
  //net->vio = vio;
  //my_net_local_init(net);                       /* Set some limits */
  net->max_packet=4*1024*1024;
  if (!(net->buff=(uchar*) malloc(key_memory_NET_buff,
                                     (size_t) net->max_packet+NET_HEADER_SIZE + COMP_HEADER_SIZE
             ))
    return 1;
  net->buff_end=net->buff+net->max_packet;
  net->error=0; net->return_status=0;
  net->pkt_nr=net->compress_pkt_nr=0;
  net->write_pos=net->read_pos = net->buff;
  net->last_error[0]=0;
  net->compress=0; net->reading_or_writing=0;
  net->where_b = net->remain_in_buf=0;
  net->last_errno=0;
  net->unused= 0;


  return 0;
}


static my_bool
net_write_buff(NET *net, const uchar *packet, size_t len)
{
  ulong left_length;

  left_length= (ulong) (net->buff_end - net->write_pos);


  if (len > left_length)
  {
    if (net->write_pos != net->buff)
    {
      /* Fill up already used packet and write it */
      memcpy(net->write_pos, packet, left_length);
      if (net_write_packet(net, net->buff,
                           (size_t) (net->write_pos - net->buff) + left_length))
        return 1;
      net->write_pos= net->buff;
      packet+= left_length;
      len-= left_length;
    }

    if (len > net->max_packet)
      return net_write_packet(net, packet, len);
    /* Send out rest of the blocks as full sized blocks */
  }
  memcpy(net->write_pos, packet, len);
  net->write_pos+= len;
  return 0;
}
