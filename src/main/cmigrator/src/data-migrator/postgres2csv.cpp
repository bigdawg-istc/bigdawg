#include <iostream>
#include <fstream>
#include <string>

#include "postgres2csv.h"
#include "endianness.h"
#include "postgres.h"

//#include <boost/log/core.hpp>
//#include <boost/log/trivial.hpp>
//#include <boost/log/expressions.hpp>
#include "boost/smart_ptr/shared_ptr.hpp"

//namespace logging = boost::log;

/**

 */
int Postgres2Csv::postgres2csv(const std::string & inputCsv, std::vector<boost::shared_ptr<Attribute> > & attributes, const std::string & outputCsv) {
    //BOOST_LOG_TRIVIAL(debug) << "postgres2csv processing (string, attributes, string)";
    FILE* inFile;
    if ((inFile=fopen(inputCsv.c_str(),"r"))==NULL) {
	fprintf(stderr,"file %s could not be opened for reading\n",inFile);
	return -1;
    }
    return Postgres2Csv::postgres2csv(inFile,attributes,outputCsv);
}

/**
   Change postgres binary to csv format.
   
   @in FILE* to an input file
   @attributes attributes in a row in the input file
   @csvFileName the name of the output file
   @return 0 if everything went okay, -1 otherwise
 */
int Postgres2Csv::postgres2csv(FILE* in, std::vector<boost::shared_ptr<Attribute> > & attributes, const std::string & csvFileName) {
//BOOST_LOG_TRIVIAL(debug) << "postgres2csv processing (FILE*, attributes, string)";
    if (!in) {
	fprintf(stderr,"The input file could not be opened!");
        return -1;
    }

    //this->csvOutFp=fopen(csvFileName.c_str(),"w");
    std::ofstream csvOut (csvFileName.c_str());

    Postgres::skipHeader(in);
    while (Postgres::readColNumber(in) != -1) {
        for (std::vector<boost::shared_ptr<Attribute> >::iterator it=attributes.begin(); it != attributes.end();) {
            (*it)->postgresReadBinary(in);
            //it->writeCsv(this->csvOutFp);
            (*it)->writeCsv(csvOut);
            ++it;
            if (it != attributes.end()) {
                csvOut << ",";
            } else {
                csvOut << std::endl;
            }
        }
    }

    //std::cout << "My endianness is: " << endianness::host_endian << std::endl;
    fclose(in);
    csvOut.close();
    return 0;
}

