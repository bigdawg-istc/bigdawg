#include "hellofunc.h"
#include "csv2scidb.h"
#include "attribute/attribute.h"
#include "csv2postgres.h"
#include "postgres2csv.h"
#include <boost/ptr_container/ptr_vector.hpp>
#include <string>
#include "postgres2scidb.h"
#include "utils.h"
#include <errno.h>

int main(int argc, char *argv[])
{
  char * cCurrentPath = new char[FILENAME_MAX];
  getCurrentPath(cCurrentPath,FILENAME_MAX);
  if(cCurrentPath==NULL) return errno;
  std::string cwd(cCurrentPath);
  
  printf("The current working directory is %s\n",cCurrentPath);

  // call a function in another file
  //myPrintHelloMake();
  // Csv2SciDB *c2s = new Csv2SciDB();
  // c2s->csv2scidb(argc,argv);
  // delete c2s;

  // Csv2Postgres c2p = Csv2Postgres();
  // c2p.csv2postgres(argc,argv);

  std::cout << "attributes:\n";

  Postgres2Csv p2c = Postgres2Csv();
  
  std::vector<std::shared_ptr<Attribute> > attributes = std::vector<std::shared_ptr<Attribute> >();
  
  attributes.push_back(std::make_shared<GenericAttribute<int64_t> >(false));
  p2c.postgres2csv(cwd+"/data/postgres_int.bin",attributes,cwd+"data/postgres_int.csv");

  std::cout << "Test table with int64 and Double\n";
  attributes.pop_back();
  attributes.push_back(std::make_shared<GenericAttribute<int> >(false));
  attributes.push_back(std::make_shared<GenericAttribute<double> >(false));
  p2c.postgres2csv(cwd+"/data/postgres_int_double.bin",attributes,cwd+"/data/postgres_int_double.csv");

  std::cout << std::endl << std::endl << std::endl;
  std::cout << "Test for 6 attributes: " << std::endl;
  std::cout << "size of double: " << sizeof(double) << std::endl;
  std::vector<std::shared_ptr<Attribute> > attributes6 = std::vector<std::shared_ptr<Attribute> >();
  std::shared_ptr<Attribute> attrInt32=std::make_shared< GenericAttribute<int32_t> >(false);
  attributes6.push_back(attrInt32);
  attributes6.push_back(std::make_shared< GenericAttribute<int32_t> >(true));
  attributes6.push_back(std::make_shared< GenericAttribute<double> >(false));
  attributes6.push_back(std::make_shared< GenericAttribute<double> >(true));
  attributes6.push_back(std::make_shared< GenericAttribute<char*> >(false));
  attributes6.push_back(std::make_shared< GenericAttribute<char*> >(true));
  p2c.postgres2csv(strcat(cCurrentPath,"/data/fromPostgrestoScidb.bin"),attributes6,cwd+"/data/postgres_6_attributes.csv");

  std::string inFileString = cwd+"/data/fromPostgrestoScidb.bin";
  std::string outFileString = cwd+"/data/scidbFromPostgrestoScidb.bin";
  FILE * inFile = fopen(inFileString.c_str(),"r");
  FILE * outFile = fopen(outFileString.c_str(),"w");
  Postgres2Scidb::postgres2scidb(inFile,attributes6,outFile);

  std::cout << "checking int double string" << std::endl;
  // data in PostgreSQL
    // test=# select * from test_int_double_string;
    //  i1 | i2 | d1 | d2 |  v1  |    v2    
    // ----+----+----+----+------+----------
    //   1 |  2 |  1 |  2 | adam | dziedzic
    //   1 |    |  1 |    | adam | 

  std::string inFileStringIntDoubleString = cwd+"/data/fromPostgresIntDoubleString.bin";
  std::string outFileStringIntDoubleString = cwd+"/data/toSciDBIntDoubleString.bin";
  FILE * inFileIntDoubleString = fopen(inFileStringIntDoubleString.c_str(),"r");
  if (!inFileIntDoubleString)
    {
      fprintf(stderr,"%s%s\n", "File does not exist: ",inFileStringIntDoubleString.c_str());
      return -1;
    }
  FILE * outFileIntDoubleString = fopen(outFileStringIntDoubleString.c_str(),"w");
  Postgres2Scidb::postgres2scidb(inFileIntDoubleString,attributes6,outFileIntDoubleString);
  
  fclose(inFile);
  fclose(outFile);
  fclose(inFileIntDoubleString);
  fclose(outFileIntDoubleString);
  delete [] cCurrentPath;

  // test shared_ptr-s
  std::shared_ptr<Attribute> attrCharShared = std::make_shared<GenericAttribute<char*> >(true);
  std::shared_ptr<Attribute> attrCharShare2 = attrCharShared;
  
  std::shared_ptr<Attribute> attrInt32Shared = std::make_shared<GenericAttribute<int32_t> >(true);

  std::vector<Attribute*> vectorAttributeStar = std::vector<Attribute*>();
  vectorAttributeStar.push_back(new GenericAttribute<int32_t>(true));
  for (std::vector<Attribute*>::iterator it=vectorAttributeStar.begin();it!=vectorAttributeStar.end();++it) {
    delete (*it);
  }
  return 0;
}
