#include "utils.h"

#ifdef WINDOWS
#include <direct.h>
#define GetCurrentDir _getcwd
#else
#include <unistd.h>
#define GetCurrentDir getcwd
#endif

void getCurrentPath(char * cCurrentPath,size_t sizeCurrentPath) {
    if (!GetCurrentDir(cCurrentPath,sizeCurrentPath)) {
        fprintf(stderr,"%s","Could not get current path\n");
    }
}

std::vector<std::string> split(const std::string &text, char sep) {
    std::vector<std::string> tokens;
    std::size_t start = 0, end = 0;
    while ((end = text.find(sep, start)) != std::string::npos) {
        std::string temp = text.substr(start, end - start);
        if (temp != "") tokens.push_back(temp);
        start = end + 1;
    }
    std::string temp = text.substr(start);
    if (temp != "") tokens.push_back(temp);
    return tokens;
}
