#ifndef DATA_MIGRATOR_EXCEPTIONS
#define DATA_MIGRATOR_EXCEPTIONS

#include <string>
#include <exception>
#include <stdexcept>

/**
 * Throw when something went wrong with the migration/export/loading process.
 */
class DataMigratorException : public std::exception {
  public:
    DataMigratorException(const char* m) : msg(m) {}
    DataMigratorException(std::string & m) : msg(m) {}
    ~DataMigratorException() throw() {}
    const char* what() const throw() {
        return msg.c_str();
    }

  protected:
    /** the message about what went wrong */
    std::string msg;
};

/**
 * The data migrator in binary format is highly dependent on what data types are supported.
 * If a given data type is not supported by the data migrator, then throw the exception.
 */
class TypeAttributeMapException : public DataMigratorException {
  public:
    TypeAttributeMapException(const char* m) : DataMigratorException(m) {}
    TypeAttributeMapException(std::string & m) : DataMigratorException(m) {}
    ~TypeAttributeMapException() throw() {}
    const char* what() const throw() {
        return msg.c_str();
    }
};

/**
 * Throw when whenever a data migration function is not implemented but only we have a scaffolding.
 */
class DataMigratorNotImplementedException : public DataMigratorException
{
  public:
    DataMigratorNotImplementedException() : DataMigratorException("Function not implemented yet!") {}
    DataMigratorNotImplementedException(const char* m) : DataMigratorException(m) {}
    DataMigratorNotImplementedException(std::string & m) : DataMigratorException(m) {}
    ~DataMigratorNotImplementedException() throw() {}
    const char* what() const throw() {
        return msg.c_str();
    }
};

#endif // DATA_MIGRATOR_EXCEPTIONS

