#ifndef DATA_MIGRATOR_EXCEPTIONS
#define DATA_MIGRATOR_EXCEPTIONS

#include <string>
#include <exception>

class DataMigratorException : public std::exception
{
 public: 
 DataMigratorException(const char* m) : msg(m) {}
 DataMigratorException(std::string & m) : msg(m) {}
  ~DataMigratorException() throw() {}
  const char* what() const throw() {return msg.c_str();}
  
 protected:
  std::string msg;
};

class TypeAttributeMapException : public DataMigratorException
{
 public:
 TypeAttributeMapException(const char* m) : DataMigratorException(m) {}
 TypeAttributeMapException(std::string & m) : DataMigratorException(m) {}
  ~TypeAttributeMapException() throw() {}
  const char* what() const throw() {return msg.c_str();}
};

#endif // DATA_MIGRATOR_EXCEPTIONS
