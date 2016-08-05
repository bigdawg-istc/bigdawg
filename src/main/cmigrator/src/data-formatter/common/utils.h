#ifndef UTILS_H
#define UTILS_H

#include <cstdio>
#include <vector>
#include <string>

void getCurrentPath(char * currentPath, size_t currentPathSize);
std::vector<std::string> splitString(const std::string &text, char sep);
void sortPrintVector(const char * message, std::vector<std::string> & vector);

#endif // UTILS_H
