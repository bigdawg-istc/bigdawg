#include <cstdio>

int main(int argc, char *argv[]) 
{
  //printf("from stdin to stdout");
  char a;
  size_t nrBytesRead=1;
  while(fread(&a,nrBytesRead,1,stdin)==nrBytesRead) 
  {
    fwrite(&a,1,1,stdout);
  }
}
