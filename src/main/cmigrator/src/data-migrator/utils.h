#ifndef UTILS_H
#define UTILS_H

#include <cstdio>
#include <vector>
#include <string>

void getCurrentPath(char * currentPath, size_t currentPathSize);
std::vector<std::string> splitString(const std::string &text, char sep);

#endif // UTILS_H
