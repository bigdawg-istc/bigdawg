#include <cstdio>
#include <iterator>
#include <string>
#include <vector>
#include <iostream>

#include "csv/csv.h"
#include "attribute/attributeWrapper.h"
#include "format/format.h"
#include "formatter/formatter.h"
#include "postgres/postgres.h"
#include "scidb/scidb.h"
#include "vertica/vertica.h"
#include "common/utils.h"
#include "fcntl.h"

/**
 * example: ./data-migrator-exe -tpostgres2csv -i /home/${USER}/data/int_not_null_test.bin -o /home/${USER}/data/int_not_null_test.bin.v2.csv  -f int32_t
 *
 * scidb2postgres: make; ./data-migrator-exe -tscidb2postgres -i /home/${USER}/data/int_scidb.bin -o /home/${USER}/data/int_postgres2.bin  -f int32_t
 *
 * postgres2scidb:

 *
 * scidb to postgres for string: ./data-migrator-exe -t scidb2postgres -i /home/${USER}/data/names_scidb.bin -o /home/${USER}/data/names_postgres.bin -f string
 */
void printUsage() {
	fprintf(stderr, "%s\n",
			"usage: -t<migrationType> -i<input> -o<output> -f<format>");
	fprintf(stderr, "%s\n",
			"format: (<type> [null]), for example: -fint32_t,int32_t null,double,double null,string,string null");
	fprintf(stderr, "%s\n",
			"full examples:\n./postgres2scdb -istdin -ostdout -f'int32_t,int32_t null,double,double null,string,string null'");
	fprintf(stderr, "%s\n",
			"./postgres2scidb -i /home/${USER}/data/fromPostgresIntDoubleString.bin -o /home/${USER}/data/toSciDBIntDoubleString.bin -f'int32_t,int32_t null,double,double null,string,string null'");
}

/**
 * Print the message and the ordered keys from the given map.
 */
void printMapKeys(const char * message, std::map<std::string, Format *> map) {
	std::vector < std::string > keys;
	for (std::map<std::string, Format *>::iterator it = map.begin();
			it != map.end(); ++it) {
		keys.push_back(it->first);
	}
	sortPrintVector(message, keys);
}

int main(int argc, char *argv[]) {

	/* direct to stdout to a file */
	int out_file = open("cout.log", O_RDWR | O_CREAT | O_APPEND, 0600);
	if (-1 == out_file) {
		perror("opening cout.log");
		return 255;
	}
	int save_out = dup(fileno(stdout));
	if (-1 == dup2(out_file, fileno(stdout))) {
		perror("cannot redirect stdout");
		return 254;
	}
	/* the end of redirecting stdout */

	printf("%s\n", "test formatter now");
	printf("Number of arguments: %d\n", argc);
	for (int i = 0; i < argc; ++i) {
		printf("Value of argument %d is: %s\n", i, argv[i]);
	}

	/* map names of formatters (scidb, postgres, csv) to their proper objects. */
	std::map<std::string, Format *> nameFormatMap;

	nameFormatMap.insert(std::make_pair("postgres", new Postgres()));
	nameFormatMap.insert(std::make_pair("csv", new Csv()));
	nameFormatMap.insert(std::make_pair("scidb", new SciDB()));
	nameFormatMap.insert(std::make_pair("vertica", new Vertica()));

	std::vector < std::string > types;
	std::vector<AttributeWrapper *> attributes;

	/* The getopt function returns the option character for the next
	 * command line option. When no more option arguments are available,
	 * it returns -1. */
	int c;
	opterr = 0;
	char* migrationType = NULL;
	char* in = NULL;
	char* out = NULL;
	char* typesRaw = NULL;
	while ((c = getopt(argc, argv, "t:f:i:o:h")) != -1) {
		switch (c) {
		case 't':
			migrationType = optarg;
			break;
		case 'i':
			in = optarg;
			break;
		case 'o':
			out = optarg;
			break;
		case 'f':
			typesRaw = optarg;
			break;
		case 'h':
			printUsage();
			exit(0);
		case '?':
			if (optopt == 'f')
				fprintf(stderr, "Option -%c requires an argument.\n", optopt);
			else if (isprint (optopt))
				fprintf(stderr, "Unknown option `-%c'.\n", optopt);
			else
				fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
			printUsage();
			return 1;
		}
	}
	fprintf(stderr, "migrationType=%s, in=%s, out=%s, types=%s\n",
			migrationType, in, out, typesRaw);

	/* set the migrators */
	std::vector < std::string > migrators = splitString(migrationType, '2');

	if (migrators.size() != 2) {
		fprintf(stderr, "%s\n",
				"Migration should be from one format to another.");
		printUsage();
		return 1; // something went wrong
	}
	/* set source and destination formatters */
	const char * knownFormatters = "Known formats: ";
	std::map<std::string, Format *>::iterator sourceIterator =
			nameFormatMap.find(migrators[0]);
	if (sourceIterator == nameFormatMap.end()) {
		fprintf(stderr, "%s %s\n", "Unknown source format: ",
				migrators[0].c_str());
		printMapKeys(knownFormatters, nameFormatMap);
		printUsage();
		return 1; //something went wrong
	}
	Format & source = (*sourceIterator->second);
	source.setFormat(in, "r");

	std::map<std::string, Format *>::iterator destIterator = nameFormatMap.find(
			migrators[1]);
	if (destIterator == nameFormatMap.end()) {
		fprintf(stderr, "%s %s\n", "Unknown destination format: ",
				migrators[1].c_str());
		printMapKeys(knownFormatters, nameFormatMap);
		printUsage();
		return 1; // something went wrong
	}
	Format & dest = (*destIterator->second);
	dest.setFormat(out, "w");

	//Postgres postgres = Postgres("/home/adam/data/int_not_null_test.bin", "r");
	//Csv csv = Csv("/home/adam/data/int_not_null_test.bin.v2.csv", "w");

	types = splitString(typesRaw, ','); // types.push_back(std::string("int32_t"));

	/* Iterate through the attributes. */
	int32_t attributePosition = 0;
	for (std::vector<std::string>::iterator it = types.begin();
			it != types.end();) {
		//printf("Type name: %s\n", (*it).c_str());
		Attribute * sourceAttr = source.getAttribute(*it)->clone();
		Attribute * destAttr = dest.getAttribute(*it)->clone();
		++it;
		if (it == types.end()) {
			sourceAttr->setIsLast(true);
			destAttr->setIsLast(true);
		}
		/* Set the position of the attributes. */
		sourceAttr->setPosition(attributePosition);
		destAttr->setPosition(attributePosition);
		++attributePosition;

		/* Create the attribute wrapper which combines (connects) internally
		 * the source and destination attributes.
		 * The source attribute reads from the source binary file and the
		 * destination attribute writes to the destination binary file. */
		AttributeWrapper * wrapper = new AttributeWrapper(sourceAttr, destAttr);
		attributes.push_back(wrapper);
		//printf("wrapper added\n");
	}
	//printf("format\n");
	//printf("Attributes size: %lu\n", attributes.size());

	source.setAttributes(attributes);
	dest.setAttributes(attributes);
	Formatter::format(source, attributes, dest);

	//printf("delete the source and destination attributes!\n");
	for (std::vector<AttributeWrapper*>::iterator it = attributes.begin();
			it != attributes.end(); ++it) {
		delete (*it);
	}

	/* Delete the formats: cpp, postgres, scidb, vertica, etc. */
	for (std::map<std::string, Format *>::iterator it = nameFormatMap.begin();
			it != nameFormatMap.end(); ++it) {
		delete it->second;
	}

	/* Go back to normal stdout. */
	fflush (stdout);
	close(out_file);
	dup2(save_out, fileno(stdout));
	close(save_out);
	/* The end of going to standard stdout. */
}
