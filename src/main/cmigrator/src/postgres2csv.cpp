#include <iostream>
#include <fstream>
#include <cstring>

#include "postgres2csv.h"
#include "endianness.h"
#include "postgres.h"

int Postgres2Csv::postgres2csv(const std::string & binFileName, std::vector<std::shared_ptr<Attribute> > & attributes, const std::string & csvFileName)
{
  printf("postgres2csv\n");
  this->binInFp=fopen(binFileName.c_str(),"rb");
  if (!this->binInFp)
  {
    std::cout << "The file with name " << binFileName <<" does not exist" << std::endl;
    return -1;
  }

  //this->csvOutFp=fopen(csvFileName.c_str(),"w");
  std::ofstream csvOut (csvFileName.c_str());
  
  Postgres::skipHeader(this->binInFp);
  while (Postgres::readColNumber(this->binInFp) != -1)
    {
      for (std::vector<std::shared_ptr<Attribute> >::iterator it=attributes.begin(); it != attributes.end();) {
	(*it)->postgresReadBinary(this->binInFp);
	//it->writeCsv(this->csvOutFp);
	(*it)->writeCsv(csvOut);
	++it;
	if (it != attributes.end()) 
	  {
	    csvOut << ",";
	  }
	else 
	  {
	    csvOut << std::endl;
	  }
      }
    }

  std::cout << "My endianness is: " << endianness::host_endian << std::endl;
  fclose(this->binInFp);
  //fclose(this->csvOutFp);
  csvOut.close();
  return 0;
}

