#include "scidb2postgres.h"
#include "postgres.h"
#include "buffer.h"
#include <boost/numeric/conversion/cast.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <string>
#include <vector>
#include <memory>

namespace logging = boost::log;

#define BUFFER_SIZE 65536
//#define BUFFER_SIZE 104857600

int Scidb2Postgres::scidb2postgres(FILE* inFile, std::vector<std::shared_ptr<Attribute> > &attributes, FILE* outFile) {
    //std::cout << "scidb2postgres processing" << std::endl;
    //BOOST_LOG_TRIVIAL(debug) << "scidb2postgres processing";
    Postgres::writeHeader(outFile);
    Buffer buffer;
    BufferNew(&buffer,inFile,BUFFER_SIZE);
    // in each step of the loop we process one line
    int scidbReadResult = 0;
    do {
        // process each column in a line
        for (std::vector<std::shared_ptr<Attribute> >::iterator it=attributes.begin(); it != attributes.end(); ++it) {
            scidbReadResult = (*it)->scidbReadBinary(inFile);
            //BOOST_LOG_TRIVIAL(debug) << "scidbReadResult: " << scidbReadResult;
            //BOOST_LOG_TRIVIAL(debug) << "is end of file:" << feof(inFile);
            if (scidbReadResult != 0) {
                //BOOST_LOG_TRIVIAL(debug) << "is it the first attribute:" << (it == attributes.begin());
                if (it == attributes.begin()) {
                    break;
                } else {
                    std::string msg ("The data for a tuple finished but the processing of all attributes/columns was not finished!");
                    std::cerr << msg << std::endl;
                    BOOST_LOG_TRIVIAL(error) << msg;
                    exit(1);
                }
            }
            if (it == attributes.begin()) {
                // write number of attributes in this tupple for postgres bin if it is the first attribute
                Postgres::writeColNumber(outFile,boost::numeric_cast<int16_t>(attributes.size()));
            }
            //BOOST_LOG_TRIVIAL(debug) << "do postgres write";
            (*it)->postgresWriteBinary(outFile);
        }
    } while(!feof(inFile));
    Postgres::writeFileTrailer(outFile);
    BufferDispose(&buffer);
    fclose(inFile);
    fclose(outFile);
    return 0;
}
