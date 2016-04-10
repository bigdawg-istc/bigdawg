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
