#include <stdint.h>
#include <iostream>
#include <endian.h>
#include <inttypes.h>
#include "postgres.h"

#define __STDC_FORMAT_MACROS

static const char BinarySignature[11] = {'P','G','C','O','P','Y','\n','\377','\r','\n','\0'};

uint16_t Postgres::readColNumber(FILE *fp) {
    // read 2 bytes representing number of columns stored in each line
    // we have to read the colNumber and check if it is the end of the file
    // -1 represents the end of the binary data
    uint16_t colNumber;
    fread(&colNumber,2,1,fp);
    colNumber = be16toh(colNumber);
    return colNumber;
}

uint16_t Postgres::readColNumberBuffer(Buffer * buffer) {
    // read 2 bytes representing number of columns stored in each line
    // fseek(this->fp,2,SEEK_CUR);
    // we have to read the colNumber and check if it is the end of the file
    // -1 represents the end of the binary data
    uint16_t colNumber;
    BufferRead(&colNumber,2,1,buffer);
    colNumber = be16toh(colNumber);
    return colNumber;
}

/* read first 19 bytes */
char* Postgres::readHeader(FILE *fp) {
    unsigned int headerSize = 19;
    char *buffer = new char[headerSize];
    fread(buffer,headerSize,1,fp);
    return buffer;
}

void Postgres::writeHeader(FILE *fp) {
    // std::cout << "Write PostgreSQL header\n";
    // write 19 bytes

    // 11 bytes for the signature
    fwrite(&BinarySignature,11,1,fp);

    // 4 bytes for flags
    uint32_t flagsField = 0;
    uint32_t flagsFieldFormatted = htobe32(flagsField);
    fwrite(&flagsFieldFormatted,4,1,fp);

    // 4 bytes for the length of the header extension area
    int32_t headerExtensionLen = 0;
    int32_t headerExtensionLenFormatted = htobe32(headerExtensionLen);
    fwrite(&headerExtensionLenFormatted,4,1,fp);
}

/** skip first 19 bytes */
void Postgres::skipHeader(FILE *fp) {
    fseek(fp,19,SEEK_CUR);
}

/* Each tuple begins with a 16-bit integer count of the number of fields in the tuple. */
void Postgres::writeColNumber(FILE *fp, uint16_t colNumber) {
    uint16_t colNumberPostgres = htobe16(colNumber);
    fwrite(&colNumberPostgres,2,1,fp);
}

/* The file trailer consists of a 16-bit integer word containing -1.
       This is easily distinguished from a tuple's field-count word. */
void Postgres::writeFileTrailer(FILE *fp) {
    uint16_t trailer = -1;
    uint16_t trailerPostgres = htobe16(trailer);
    fwrite(&trailerPostgres,2,1,fp);
}
