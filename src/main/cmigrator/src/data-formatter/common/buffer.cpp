#include "buffer.h"

#include <string.h>
#include <assert.h>
#include <stdlib.h>

static bool CopyLoadRawBuf(Buffer* buffer);
static size_t CopyGetData(Buffer* buffer, void* databuf, size_t minread, size_t maxread);

void BufferNew(Buffer * buffer, FILE * file, size_t size)
{
  buffer->raw_buf= (char*) malloc(size+1);
  buffer->size=size;
  buffer->raw_buf_index=0;
  buffer->raw_buf_len=0;
  buffer->file=file;
}

void BufferDispose(Buffer * buffer)
{
  free(buffer->raw_buf);
}

size_t BufferRead(void* address, size_t size, size_t count, Buffer * buffer)
{
  assert(count==1);
  size_t bytesNumber = size*count;
  assert(bytesNumber < buffer->size);
  // check if there is enough bytes to be read (if not then fetch new bytes)
  if (buffer->raw_buf_len-buffer->raw_buf_index < bytesNumber)
    {
      // copy new bytes to the buffer
      CopyLoadRawBuf(buffer); // this change raw_buf_index and raw_buf_len
      // if there is still less bytes than required - just return what is there
      if (buffer->raw_buf_len-buffer->raw_buf_index < bytesNumber)
	{
	  // copy remaining bytes
	  memcpy(address,buffer->raw_buf+buffer->raw_buf_index,buffer->raw_buf_len-buffer->raw_buf_index);
	  // increase the buffer index
	  buffer->raw_buf_index+=buffer->raw_buf_len-buffer->raw_buf_index;
	  // return number of bytes copied from the buffer (it can be 0)
	  return buffer->raw_buf_len-buffer->raw_buf_index;
	}
    }
  // copy the bytes from the buffer
  memcpy(address,buffer->raw_buf+buffer->raw_buf_index,bytesNumber);
  // increase the buffer index
  buffer->raw_buf_index+=bytesNumber;
  return bytesNumber;
}

/*
 * this imitates CopyLoadRawBuf from PostgreSQL copy.c file
 *
 * CopyLoadRawBuf loads some more data into raw_buf
 *
 * Returns TRUE if able to obtain at least one more byte, else FALSE.
 *
 * If raw_buf_index < raw_buf_len, the unprocessed bytes are transferred
 * down to the start of the buffer and then we load more data after that.
 * This case is used only when a frontend multibyte character crosses a
 * bufferload boundary.
 */
static bool CopyLoadRawBuf(Buffer* buffer)
{
  size_t nbytes; /* number of bytes to be still processed in the buffer */
  size_t inbytes; /* number of bytes read from the file to the buffer */

  if (buffer->raw_buf_index < buffer->raw_buf_len)
    {
      /* Copy down the unprocessed data */
      nbytes = buffer->raw_buf_len - buffer->raw_buf_index;
      memmove(buffer->raw_buf,buffer->raw_buf + buffer->raw_buf_index,nbytes);
    }
  else
    {
      nbytes = 0; /* no data need to be saved / still processed from the buffer */
    }
  inbytes = CopyGetData(buffer, buffer->raw_buf+nbytes,1,buffer->size-nbytes);
  nbytes+=inbytes;
  buffer->raw_buf[nbytes]='\0';
  buffer->raw_buf_index=0;
  buffer->raw_buf_len=nbytes;
  /* printf("%s\n","show buffer"); */
  /* for (size_t i=0;i<buffer->raw_buf_len;++i)  */
  /*   { */
  /*     printf("%o ",buffer->raw_buf[i]); */
  /*   } */
  /* 	printf("%s \n","the end of buffer show"); */
  return (inbytes > 0);
}

/*
 * this imitates CopyGetData from PostgreSQL copy.c file
 *
 * CopyGetData reads data from the source (file or frontend)
 *
 * We attempt to read at least minread, and at most maxread, bytes from
 * the source.  The actual number of bytes read is returned; if this is
 * less than minread, EOF was detected.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 */
static size_t CopyGetData(Buffer* buffer, void* databuf, size_t minread, size_t maxread)
{
  assert(minread==1);
  size_t bytesRead = 0;
  bytesRead = fread(databuf,1,maxread,buffer->file);
  if (ferror(buffer->file))
    {
      fprintf(stderr,"%s\n","Not able to read data from the file in buffer");
      exit(1);
    }
  return bytesRead;
}
