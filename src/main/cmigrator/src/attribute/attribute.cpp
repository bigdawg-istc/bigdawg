/*

  This file contains specialization for strings and boolean values.

  The generic handle for numbers can be found in: attribute.h
*/

#include "attribute/attribute.h"

#include <boost/log/trivial.hpp>

namespace logging = boost::log;

// this is the definition of null(s) for values that are empty (for example in SciDB binary format)
const char Attribute::nullValues[Attribute::nullValuesSize] = {'\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'};

Attribute* new_clone(Attribute const& other) {
    return other.clone();
}

Attribute::~Attribute() {}

//######################################################################################################
// Specialization for strings

GenericAttribute<char*>::GenericAttribute(bool isNullable) {
    this->value=NULL;
    this->isNullable=isNullable;
    this->bytesNumber=-1; // this is specific for every string
}

GenericAttribute<char*>::~GenericAttribute() {

}

void GenericAttribute<char*>::freeValue() {
    if (this->value != NULL) {
        delete [] this->value;
    }
    this->value = NULL;
}

int GenericAttribute<char*>::postgresReadBinary(FILE *fp) {
// string is handled differently than other types
    fread(&this->bytesNumber,4,1,fp);
//std::cout << "value bytes number before endianness: " << valueBytesNumber << std::endl;
    this->bytesNumber = endianness::from_postgres<int32_t>(this->bytesNumber);
//std::cout << "value bytes number after endianness: " << valueBytesNumber << std::endl;
    if (this->bytesNumber == -1) {
// this is null and there are no bytes for the value
        this->isNull=true;
        return 0;
    }
    this->isNull=false;
    this->value = new char[this->bytesNumber+1]; // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
    this->value[this->bytesNumber]='\0';
//std::cout << "value bytes number: " << valueBytesNumber << std::endl;
    fread(this->value,this->bytesNumber,1,fp);
// std::cout << "value read: " << value << std::endl;
    return 0; // success
}

int GenericAttribute<char*>::writeCsv(std::ofstream & ofile) {
// write nothing if the value is NULL
    if (this->isNull) return 0;
// write the value only if it's not NULL
// fprintf(fp,"%" PRId64,this->value);
//printf("%.*s",this->bytesNumber,value);
    ofile.width(this->bytesNumber);
    ofile << this->value;
    freeValue();
    return 0;
}

int GenericAttribute<char*>::scidbWriteBinary(FILE *fp) {
    if(this->isNullable) {
// we have to add one byte with info it it is null or not
        if(this->isNull) {
// we don't know the reason why it is null so we'll write byte 0
            char nullReason=0;
            fwrite(&nullReason,1,1,fp);
// this is indeed null, so only write -1 as the number of bytes
// and don't write the NULL value
            int32_t scidbBytesNumber = 0;
            fwrite(&scidbBytesNumber,4,1,fp);
            return 0;
        } else {
// this value is not null so the byte of nullable should have value: -1
            char notNull=-1;
            fwrite(&notNull,1,1,fp);
        }
    }
//std::cout << "Bytes number written for SciDB: " << this->bytesNumber << std::endl;
//fflush(stdout);
    int32_t scidbBytesNumber=this->bytesNumber+1;
    fwrite(&(scidbBytesNumber),4,1,fp);
//printf("Value written for SciDB: %.13s\n",this->value);
//fflush(stdout);
    fwrite(this->value,this->bytesNumber+1,1,fp); // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
// free the string buffer
    freeValue();
    return 0;
}

int GenericAttribute<char*>::postgresReadBinaryBuffer(Buffer * buffer) {
// string is handled differently than other types
    BufferRead(&this->bytesNumber,4,1,buffer);
//std::cout << "value bytes number before endianness: " << valueBytesNumber << std::endl;
    this->bytesNumber = endianness::from_postgres<int32_t>(this->bytesNumber);
//std::cout << "value bytes number after endianness: " << this->bytesNumber << std::endl;
    if (this->bytesNumber == -1) {
// this is null and there are no bytes for the value
        this->isNull=true;
        return 0;
    }
    this->isNull=false;
    this->value = new char[this->bytesNumber+1]; // +1 is for NULL \0 value at the end of the string: SciDB adds \0 at the end of each string
    this->value[this->bytesNumber]='\0';
//std::cout << "value bytes number: " << valueBytesNumber << std::endl;
    BufferRead(this->value,this->bytesNumber,1,buffer);
//std::cout << "value read: " << value << std::endl;
    return 0; // success
}

int GenericAttribute<char*>::scidbReadBinary(FILE *fp) {
    /* A string data type that allows nulls is always preceded by five bytes:
       a null byte indicating whether a value is present
       and four bytes indicating the string length. */
    if (this->isNullable) {
        /* read 1 byte that tells us if the attribute is null or not:
           - If a nullable attribute contains a non-null value,
           the preceding null byte is -1.
           - If a nullable attribute contains a null value,
           the preceding null byte will contain the missing reason code,
           which must be between 0 and 127
        */
        int8_t nullValue;
        size_t bytes_read;
        bytes_read = fread(&nullValue,1,1,fp);
        if(bytes_read < 1) return 1; // no more data in the input file
        if(nullValue>=0 && nullValue <= 127) {
            this->isNull = true;
// we don't need the reason why it is null so we'll write byte 0
            /* A fixed-length data type that allows null values
               will always consume one more byte than the datatype requires,
               regardless of whether the actual value is null or non-null.
               For example, an int8 will require 2 bytes and an int64
               will require 9 bytes. (In the figure, see bytes 2-4 or 17-19.)
            */
        } else {
            /* if nullValue != -1: it means that there was another unexpected value
               different from [-1,127] */
            assert(nullValue==-1);
        }
    }
// read the length of the string
    size_t elements_read = fread(&(this->bytesNumber),4,1,fp);
    if(elements_read != 1) return 1; // no more data in the input file
    if (this->isNull) {
        if (this->bytesNumber != 0) {
            std::string msg ("The null byte indicated null value but the string size is different than zero!");
            std::cerr << msg << std::endl;
            //BOOST_LOG_TRIVIAL(error) << msg;
            exit(1);
        }
        return 0;
    }
// prepare string buffer for the new value
    this->value = new char[this->bytesNumber];
    elements_read = fread(this->value,this->bytesNumber,1,fp);
    if(elements_read != 1) return 1; // no more data in the input file
    return 0;
}

int GenericAttribute<char*>::postgresWriteBinary(FILE *fp) {
    // we don't write the \0 null at the end of a string in postgres
    this->bytesNumber-=1;
    if(this->isNullable) {
        if(this->isNull) {
            /* -1 indicates a NULL field value */
            int32_t nullValue = -1;
            int32_t nullValuePostgres = endianness::to_postgres<int32_t>(nullValue);
            fwrite(&nullValuePostgres,4,1,fp);
            /* No value bytes follow in the NULL case. */
            return 0;
        }
    }
    int32_t attrLengthPostgres = endianness::to_postgres<int32_t>(this->bytesNumber);
    fwrite(&attrLengthPostgres,4,1,fp);
    fwrite(this->value,this->bytesNumber,1,fp);
    // free the string buffer
    freeValue();
    return 0;
}


//######################################################################################################
// specialization for: BOOL

GenericAttribute<bool>::GenericAttribute(bool isNullable) {
    this->isNullable=isNullable;
    this->bytesNumber=sizeof(bool);
}

GenericAttribute<bool>::~GenericAttribute() {

}

int GenericAttribute<bool>::postgresReadBinary(FILE *fp) {
    int32_t valueBytesNumber;
    fread(&valueBytesNumber,4,1,fp);
    valueBytesNumber = endianness::from_postgres<int32_t>(valueBytesNumber);
    if (valueBytesNumber == -1) {
        assert(isNullable==true);
        // if (isNullable==false) {
        //     std::string msg ("Binary input shows a null value, whereas the attribute does not allow null values!");
        //     std::cerr << msg << std::endl;
        //     //BOOST_LOG_TRIVIAL(error) << msg;
        //     throw msg;
        // }
        // this is null and there are no bytes for the value
        this->isNull=true;
        return 0;
    }
    this->isNull=false;
    assert(valueBytesNumber==this->bytesNumber);
    // bool value in the binary format
    char boolBin;
    fread(&boolBin,this->bytesNumber,1,fp);
    if (boolBin == '\0') {
        value = false;
    } else if (boolBin == 1) {
        value = true;
    } else {
        std::string msg ("Unrecognized value for bool attribute in the binary format: "+boolBin);
        std::cerr << msg << std::endl;
        //BOOST_LOG_TRIVIAL(error) << msg;
        exit(1);
    }
    return 0; // success
}

int GenericAttribute<bool>::postgresReadBinaryBuffer(Buffer * buffer) {
    std::string msg ("The function: GenericAttribute<bool>::postgresReadBinaryBuffer is not implemented!");
    std::cerr << msg << std::endl;
    //BOOST_LOG_TRIVIAL(error) << msg;
    exit(1);
}

int GenericAttribute<bool>::writeCsv(std::ofstream & ofile) {
    // write nothing if the value is NULL
    if (this->isNull) return 0;
    // fprintf(fp,"%" PRId64,this->value);
    if (value == true) {
        ofile << 't';
    } else {
        ofile << 'f';
    }
    return 0;
}

int GenericAttribute<bool>::scidbWriteBinary(FILE *fp) {
    if(this->isNullable) {
        if(this->isNull) {
            // we don't know the reason why it is null so we'll write byte 0
            char nullReason=0;
            fwrite(&nullReason,1,1,fp);
            assert(nullValuesSize>=this->bytesNumber);
            fwrite(nullValues,this->bytesNumber,1,fp);
            return 0;
        } else {
            char notNull=-1;
            fwrite(&notNull,1,1,fp);
        }
    }
    char writeValue;
    if (this->value == true) {
        writeValue = 1;
    } else {
        writeValue='\0';
    }
    fwrite(&writeValue,this->bytesNumber,1,fp);
    return 0;
}

int GenericAttribute<bool>::scidbReadBinary(FILE *fp) {
    //std::cout << "this is type: " << boost::typeindex::type_id().pretty_name() << std::endl;
    if(this->isNullable) {
        /* read 1 byte that tells us if the attribute is null or not:
        - If a nullable attribute contains a non-null value,
         the preceding null byte is -1.
         - If a nullable attribute contains a null value,
         the preceding null byte will contain the missing reason code,
         which must be between 0 and 127
            */
        int8_t nullValue;
        size_t bytes_read;
        bytes_read = fread(&nullValue,1,1,fp);
        //BOOST_LOG_TRIVIAL(debug) << "bytes read: " << bytes_read;
        if(bytes_read < 1) return 1; // no more data in the input file
        if(nullValue>=0 && nullValue <= 127) {
            this->isNull = true;
            // we don't need the reason why it is null so we'll write byte 0
            /* A fixed-length data type that allows null values
               will always consume one more byte than the datatype requires,
               regardless of whether the actual value is null or non-null.
               For example, an int8 will require 2 bytes and an int64
               will require 9 bytes. (In the figure, see bytes 2-4 or 17-19.)
            */
        } else {
            /* if nullValue != -1: it means that there was another unexpected value
               different from [-1,127] */
            assert(nullValue==-1);
        }
    }
    char boolValue;
    size_t elements_read = fread(&boolValue,this->bytesNumber,1,fp);
    //BOOST_LOG_TRIVIAL(debug) << "elements_read: " << elements_read;
    //BOOST_LOG_TRIVIAL(debug) << "bytes number in the attribute: " << this->bytesNumber;
    //BOOST_LOG_TRIVIAL(debug) << "value in the binary file: " << boolValue;
    if (boolValue == 1) {
        this->value=true;
    } else if(boolValue=='\0') {
        this->value=false;
    } else {
        std::string msg ("Unrecognized value for bool attribute in the binary format: "+boolValue);
        std::cerr << msg << std::endl;
        //BOOST_LOG_TRIVIAL(debug) << msg;
        exit(1);
    }
    if(elements_read != 1) return 1; // no more data in the input file
    return 0; // everything is correct: success
}

int GenericAttribute<bool>::postgresWriteBinary(FILE *fp) {
    //BOOST_LOG_TRIVIAL(debug) << "postgresWriteBinary is nullable: " << this->isNullable;
    if(this->isNullable) {
        if(this->isNull) {
            /* -1 indicates a NULL field value */
            int32_t nullValue = -1;
            int32_t nullValuePostgres = endianness::to_postgres<int32_t>(nullValue);
            fwrite(&nullValuePostgres,4,1,fp);
            /* No value bytes follow in the NULL case. */
            return 0;
        }
    }
    int32_t attrLengthPostgres = endianness::to_postgres<int32_t>(this->bytesNumber);
    fwrite(&attrLengthPostgres,4,1,fp);
    char boolBin;
    if (this->value == false) {
        boolBin='\0';
    } else {
        boolBin=1;
    }
    //BOOST_LOG_TRIVIAL(debug) << "postgresWriteBinary bytes number: " << this->bytesNumber;
    fwrite(&boolBin,this->bytesNumber,1,fp);
    return 0;
}
