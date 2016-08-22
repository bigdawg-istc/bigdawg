#ifndef UTILS_H
#define UTILS_H

#include <cstdio>
#include <vector>
#include <string>
#include <inttypes.h>

void getCurrentPath(char * currentPath, size_t currentPathSize);

std::vector<std::string> splitString(const std::string &text, char sep);

void sortPrintVector(const char * message, std::vector<std::string> & vector);

/**
 * Trimming - get rid of leading and trailing whitespaces.
 */
std::string trim(const std::string & text, const std::string & whitespaces =
		"\t ");

/** Get current position of the cursor in the file referenced by fp. */
long int getCurrentFilePosition(FILE * fp);

/** Move the cursor of the file referenced by fp by number of bytes equal
 * to bytesMoved (it can be forward, positive bytesMoved,
 * or backward, negative bytesMoved). */
void moveFilePosition(FILE * fp, int bytesMoved);

/** Set the file position to the previousPosition from the beginning of the
 * file. */
void moveToPreviousPosition(FILE * fp, long int previousPosition);

/** Reverse the order of bits in a byte. */
int8_t reverseBitsInByte(int8_t byte);

#endif // UTILS_H
