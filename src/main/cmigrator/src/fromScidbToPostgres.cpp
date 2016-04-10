#define BOOST_LOG_DYN_LINK 1

#include <cstdio> /* also defines FILENAME_MAX */
#include <string>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>
#include <vector>
#include <iostream>

#include "attribute/attribute.h"
#include "scidb2postgres.h"
#include "utils.h"
#include "typeAttributeMap.h"
#include "dataMigratorExceptions.h"

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

void printUsage() {
    fprintf(stderr,"%s\n","usage: ./scidb2postgres -i<input> -o<output> -f<format>");
    fprintf(stderr,"%s\n","format: (<type>:[null]), for example: -fint32_t,int32_t null,double,double null,string,string null");
    fprintf(stderr,"%s\n","full examples:\n./scidb2postgres -istdin -ostdout -fint32_t,int32_t:null,double,double:null,string,string:null");
    fprintf(stderr,"%s\n","./scidb2postgres -i data/fromSciDBIntDoubleString.bin -o data/toPostgresIntDoubleString.bin");
}

void closeFiles(FILE * logFile, bool isInFile, FILE * inFile, bool isOutFile, FILE * outFile) {
    fclose(logFile);
    if(isInFile && inFile != NULL) fclose(inFile);
    if(isOutFile && outFile != NULL) fclose(outFile);
}

void initLog() {
    logging::add_file_log
    (
        keywords::file_name = "scidb2postgresBoost_%N.log",
        keywords::rotation_size = 10 * 1024 * 1024,
        keywords::time_based_rotation = sinks::file::rotation_at_time_point(0, 0, 0),
        keywords::format = "[%TimeStamp%]: %Message%"
    );

    logging::core::get()->set_filter
    (
        logging::trivial::severity >= logging::trivial::debug
    );
}

int main(int argc, char *argv[], char* env[]) {
    initLog();
    logging::add_common_attributes();
    //BOOST_LOG_TRIVIAL(debug) << "From SciDB to Postgres";
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

    std::string logFileName="scidb2postgres.log";
    std::string message = "Please see the log file: "+logFileName+" \n";
    fprintf(stderr,"%s",message.c_str());

    if((logFile=fopen(logFileName.c_str(),"a"))==NULL) {
        fprintf(stderr,"file %s could not be opened for logging\n",logFileName.c_str());
        return 1;
    }

    fprintf(logFile,"%s","from scidb to postgres\n");

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
        Scidb2Postgres::scidb2postgres(inFile,attributes,outFile);
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
