#include <cstdio> /* also defines FILENAME_MAX */
#include <string>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>
#include <vector>
#include <iostream>

#include "attribute/attribute.h"
#include "postgres2scidb.h"
#include "postgres2csv.h"
#include "utils.h"
#include "typeAttributeMap.h"
#include "dataMigratorExceptions.h"

void printUsage() {
    fprintf(stderr,"%s\n","usage: ./postgres2scidb -i<input> -o<output> -f<format>");
    fprintf(stderr,"%s\n","format: (<type>:[null]), for example: -fint32_t,int32_t null,double,double null,string,string null");
    fprintf(stderr,"%s\n","full examples:\n./postgres2scdb -istdin -ostdout -fint32_t,int32_t:null,double,double:null,string,string:null");
    fprintf(stderr,"%s\n","./postgres2scidb -i data/fromPostgresIntDoubleString.bin -o data/toSciDBIntDoubleString.bin");
}

void closeFiles(FILE * logFile, bool isInFile, FILE * inFile, bool isOutFile, FILE * outFile) {
    fclose(logFile);
    if(isInFile && inFile != NULL) fclose(inFile);
    if(isOutFile && outFile != NULL) fclose(outFile);
}

int main(int argc, char *argv[], char* env[]) {
    printf("%s\n","go with buffer");
    FILE * logFile = NULL;
    FILE * inFile = stdin;
    FILE * outFile = stdout;
    // is the input from a file or it is from stdin (standard input)
    bool isInFile=false; // by default we read from stdin not from a file
    // is the output to a file or it is to a stdout (standard output)
    bool isOutFile=false; // by default we wrtie to the stdout not from a file

    char * cCurrentPath = new char[FILENAME_MAX];
    getCurrentPath(cCurrentPath,FILENAME_MAX);
    if(cCurrentPath==NULL) return errno;
    std::string cwd(cCurrentPath);
    fprintf(stderr,"The current working directory is %s\n",cwd.c_str());

    const char* defaultOut="stdout";
    const char* defaultIn="stdin";
    const char* defaultTypes="int32_t,int32_t null,double,double null,string,string null";

    char* out = (char*)defaultOut;
    char* in = (char*)defaultIn;
    char* types = (char*)defaultTypes;
    int c;

    std::string logFileName="postgres2scidb.log";
    std::string message = "Please see the log file: "+logFileName+" \n";
    fprintf(stderr,"%s",message.c_str());

    if((logFile=fopen(logFileName.c_str(),"a"))==NULL) {
        fprintf(stderr,"file %s could not be opened for logging\n",logFileName.c_str());
        return 1;
    }

    fprintf(logFile,"%s","from postgres to scidb\n");

    opterr=0;
    while((c = getopt(argc,argv,"f:i:o:h")) != -1) {
        switch (c) {
        case 'i':
            in=optarg;
            if((inFile=fopen(in,"r"))==NULL) {
                fprintf(stderr,"file %s could not be opened for reading\n",in);
                return 1;
            }
            break;
        case 'o':
            out=optarg;
            if((outFile=fopen(out,"w"))==NULL) {
                fprintf(stderr,"file %s could not be opened for writing\n",out);
                return 1;
            }
            break;
        case 'f':
            types=optarg;
            break;
        case 'h':
            printUsage();
            exit(0);
        case '?':
            if (optopt == 'f')
                fprintf (stderr, "Option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf (stderr, "Unknown option `-%c'.\n", optopt);
            else
                fprintf (stderr,
                         "Unknown option character `\\x%x'.\n",
                         optopt);
            printUsage();
            return 1;
        }
    }

    // // set the buffers for fread and fwrite
    // fprintf(stderr,"%s\n","Set buffers for fread/fwrite");
    // if( setvbuf(inFile,NULL,_IOFBF,65536) != 0)
    //   {
    //     fprintf(stderr,"%s\n","ERROR: The buffer for the input file was not set!");
    //   }
    // if( setvbuf(outFile,NULL,_IOFBF,65536) !=0 )
    //   {
    //     fprintf(stderr,"%s\n","ERROR: The buffer for the output file was not set!");
    //   }

    fprintf(stderr,"in=%s, out=%s, types=%s\n",in,out,types);
    std::vector<std::shared_ptr<Attribute> > attributes = std::vector<std::shared_ptr<Attribute> >();
    TypeAttributeMap mapTypes = TypeAttributeMap();
    try {
        mapTypes.getAttributesFromTypes(attributes,types);
        //Postgres2Scidb::postgres2scidb(inFile,attributes,outFile);
	std::string csvOut(out);
	Postgres2Csv::postgres2csv(inFile,attributes,csvOut);
    } catch(const TypeAttributeMapException & ex) {
        std::cerr << ex.what();
        std::vector<std::string> supportedTypes;
        mapTypes.getSupportedTypes(supportedTypes);
        std::sort(supportedTypes.begin(),supportedTypes.end());
        std::cerr << "Supported types (ordered by name): ";
        for (std::vector<std::string>::const_iterator it = supportedTypes.begin(); it != supportedTypes.end(); ++it) {
            std::cerr << *it << " ";
        }
        std::cerr << std::endl;
        closeFiles(logFile,isInFile,inFile,isOutFile,outFile);
        return 1; // something went wrong
    }
    closeFiles(logFile,isInFile,inFile,isOutFile,outFile);
    delete [] cCurrentPath;
    return 0;
}
