#include <utility>
#include <cstdio>

#include "format.h"
#include "formatterExceptions.h"
#include "utils.h"

Format::Format(const std::string & fileName, const char * fileMode) {
	this->fp = NULL;
	setFile(fileName, fileMode);
}

Format::Format() {
	this->fp = NULL;
}

Format::~Format() {
    //printf("Freeing memory Format\n");
	//fflush (stdout);
	if (fp != NULL) {
		fclose(fp);
	}
	cleanTypeAttributeMap();
}

void Format::cleanTypeAttributeMap() {
	for (std::map<std::string, Attribute *>::iterator it =
			typeAttributeMap.begin(); it != typeAttributeMap.end(); ++it) {
		delete it->second;
	}
	typeAttributeMap.clear();
}

Attribute * Format::getAttribute(const std::string& typeRaw) {
	/* Trimming - get rid of whit spaces (leading and trailing) */
	std::string type = trim(typeRaw);
	try {
		return typeAttributeMap.at(type);
	} catch (const std::out_of_range & oor) {
		std::string msg = "Unrecognized type: " + type + "\n"
				+ "Message from the original exception: Out of range error "
				+ oor.what() + "\n";
		std::vector < std::string > supportedTypes;
		getSupportedTypes (supportedTypes);
		sortPrintVector("Supported types: ", supportedTypes);
		throw TypeAttributeMapException(msg);
	}
}

void Format::getSupportedTypes(std::vector<std::string> & supportedTypes) {
	for (std::map<std::string, Attribute *>::iterator it =
			typeAttributeMap.begin(); it != typeAttributeMap.end(); ++it) {
		supportedTypes.push_back(it->first);
	}
}

void Format::setFormat(const std::string & fileName, const char * fileMode) {
	setFile(fileName, fileMode);
	/* We have to update the fp (file pointer) in the attribute map. */
    //printf("Set attribute map.");
	setTypeAttributeMap();
}

void Format::setFile(const std::string & fileName, const char * fileMode) {
	/* initialize the input file;
	 * we will have to establish if it is for reads or for writes */
    //printf("file name %s and file mode %s\n", fileName.c_str(), fileMode);
    //printf("setFile for format in format.cpp\n");
	if (fileName == "stdout") {
		this->fp = stdout;
	} else if (fileName == "stdin") {
		this->fp = stdin;
	} else if ((this->fp = fopen(fileName.c_str(), fileMode)) == NULL) {
		std::string message = "File " + fileName + " could not be opened!";
		throw DataMigratorException(message);
	}
}
