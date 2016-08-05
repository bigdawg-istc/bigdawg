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

void getCurrentPath(char * cCurrentPath, size_t sizeCurrentPath) {
	if (!GetCurrentDir(cCurrentPath, sizeCurrentPath)) {
		fprintf(stderr, "%s", "Could not get current path\n");
	}
}

std::vector<std::string> splitString(const std::string &text, char sep) {
	std::vector < std::string > tokens;
	std::size_t start = 0, end = 0;
	while ((end = text.find(sep, start)) != std::string::npos) {
		std::string temp = text.substr(start, end - start);
		if (temp != "")
			tokens.push_back(temp);
		start = end + 1;
	}
	std::string temp = text.substr(start);
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
