#ifndef BUFFER_H
#define BUFFER_H

#include <stdio.h>
// imitate the buffer for copy.c from PostgreSQL

// #define RAW_BUF_SIZE 65536 // this is equal to the default size of the pipe

typedef struct Buffer {
    char * raw_buf; /* this is the buffer */
    size_t size; /* size of the buffer */
    size_t raw_buf_index; /* next byte to process */
    size_t raw_buf_len; /* total # of bytes stored */
    FILE * file; /* file to be read from */

} Buffer;

void BufferNew(Buffer* buffer, FILE * file, size_t size);
void BufferDispose(Buffer* buffer);

/* imitate the fread: buffer read
   write the bytes to the address
   read (size*1) bytes
   count has to be 1
   from the buffer
*/
size_t BufferRead(void* address, size_t size, size_t count, Buffer* buffer);

#endif // BUFFER_H
