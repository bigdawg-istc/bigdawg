#include "utils.h"

#ifdef WINDOWS
#include <direct.h>
#define GetCurrentDir _getcwd
#else
#include <unistd.h>
#define GetCurrentDir getcwd
#endif
#include <iostream>
#include <algorithm>
#include "formatterExceptions.h"

void getCurrentPath(char * cCurrentPath, size_t sizeCurrentPath) {
	if (!GetCurrentDir(cCurrentPath, sizeCurrentPath)) {
		fprintf(stderr, "%s", "Could not get current path\n");
	}
}

std::vector<std::string> splitString(const std::string &text, char sep) {
	std::vector < std::string > tokens;
	std::size_t start = 0, end = 0;
	std::string temp; /* The last string has to be added separately. */

	/* std::string::npos - end of string indicator */
	while ((end = text.find(sep, start)) != std::string::npos) {
		std::string temp = text.substr(start, end - start);
		if (temp != "")
			tokens.push_back(temp);
		start = end + 1;
	}
	temp = text.substr(start);
	if (temp != "")
		tokens.push_back(temp);
	return tokens;
}

void sortPrintVector(const char* message, std::vector<std::string> & vector) {
	std::cerr << message;
	std::sort(vector.begin(), vector.end());
	for (std::vector<std::string>::const_iterator it = vector.begin();
			it != vector.end();) {
		std::cerr << *it;
		++it;
		if (it != vector.end()) {
			std::cerr << ", ";
		}
	}
	std::cerr << std::endl;
}

std::string trim(const std::string & text, const std::string & whitespaces) {
	const size_t strBegin = text.find_first_not_of(whitespaces);
	/* string::npos - used to indicate no matches */
	if (strBegin == std::string::npos) {
		return ""; // no content
	}
	const size_t strEnd = text.find_last_not_of(whitespaces);
	const size_t strRange = strEnd - strBegin + 1;
	return text.substr(strBegin, strRange);
}

long int getCurrentFilePosition(FILE * fp) {
	long int pos = ftell(fp); /* Position indicator at start of the file. */
	if (pos == -1L) {
		perror("ftell()");
		fprintf(stderr, "ftell() failed in file %s at line # %d\n", __FILE__,
		__LINE__ - 4);
		std::string message = "ftell() failed in file " + std::string(__FILE__);
		message += " at line # " + __LINE__ - 4;
		throw DataMigratorException(message);
	}
	return pos;
}

void moveFilePosition(FILE * fp, int bytesMoved) {
	if (fseek(fp, bytesMoved, SEEK_CUR) != 0) {
		if (ferror(fp)) {
			perror("fseek()");
			fprintf(stderr,
					"moveFilePosition fseek() failed in file %s at line # %d\n",
					__FILE__, __LINE__ - 5);
			std::string message = "moveFilePosition fseek() failed in file "
					+ std::string(__FILE__);
			message += " at line # " + __LINE__ - 4;
			throw DataMigratorException(message);
		}
	}
}

void moveToPreviousPosition(FILE * fp, long int previousPosition) {
	if (fseek(fp, previousPosition, SEEK_SET) != 0) {
		if (ferror(fp)) {
			perror("fseek()");
			fprintf(stderr,
					"moveToPreviousPosition fseek() failed in file %s at line # %d\n",
					__FILE__, __LINE__ - 5);
			std::string message =
					"moveToPreviousPosition fseek() failed in file "
							+ std::string(__FILE__);
			message += " at line # " + __LINE__ - 4;
			throw DataMigratorException(message);
		}
	}
}

/**
 * First the left four bits are swapped with the right four bits.
 * Then all adjacent pairs are swapped and then all adjacent single bits.
 * This results in a reversed order.
 */
int8_t reverseBitsInByte(int8_t b) {
	b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
	b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
	b = (b & 0xAA) >> 1 | (b & 0x55) << 1;
	return b;
}
