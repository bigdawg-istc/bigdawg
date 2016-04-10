# http://www.cs.colby.edu/maxwell/courses/tutorials/maketutor/

IDIR=include
SRC_DIR=src
CC=g++
LIBS=-lm -lboost_log -lboost_thread -lboost_system -lboost_log_setup -lpthread

#CFLAGS=-I$(IDIR) -Wall -pedantic -L/usr/include/boost -DBOOST_LOG_DYN_LINK -std=c++11 -ggdb # -g with debug
CFLAGS=-I$(IDIR) -Wall -pedantic -L/usr/local/include/boost -DBOOST_LOG_DYN_LINK -std=c++11 -O2 # -O2 for tests with optimization
#CFLAGS=-I$(IDIR) -Wall -pedantic -L/usr/include/boost -DBOOST_LOG_DYN_LINK -std=c++11 -O2 # -O2 for tests with optimization

FINAL_PROGRAM=data-migrator
POSTGRES_TO_SCIDB=postgres2scidb
SCIDB_TO_POSTGRES=scidb2postgres

ODIR=obj
LDIR=lib

# to add/remove program files edit onlny this block

_DEPS=csv2scidb.h hellofunc.h attribute/attribute.h csv2postgres.h endianness.h postgres2csv.h $(POSTGRES_TO_SCIDB).h postgres.h utils.h typeAttributeMap.h dataMigratorExceptions.h buffer.h $(SCIDB_TO_POSTGRES).h
DEPS=$(patsubst %,$(IDIR)/%,$(_DEPS))

_OBJ=data-migrator.o csv2scidb.o hellofunc.o attribute/attribute.o csv2postgres.o postgres2csv.o $(POSTGRES_TO_SCIDB).o postgres.o utils.o typeAttributeMap.o buffer.o $(SCIDB_TO_POSTGRES).o
OBJ=$(patsubst %,$(ODIR)/%,$(_OBJ))

# this is the end of the block to add/remove program files

# debug
# obj/data-migrator.o: src/data-migrator.cpp $(DEPS)
# 	$(CC) -c -o $@ $< $(CFLAGS) $(LIBS)

$(ODIR)/%.o: $(SRC_DIR)/%.cpp $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS) $(LIBS)

$(ODIR)/%.o: $(SRC_DIR)/%.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS) $(LIBS)

$(ODIR)/attribute/%.o: $(SRC_DIR)/attribute/%.cpp $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS) $(LIBS)

$(FINAL_PROGRAM): $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

# phone says that clean is not a final file (a program to be exeucted) but a virtual goal (the file does not exist)
.PHONY: clean all

clean:
	rm -f $(ODIR)/*.o *~ core $(INCDIR)/*~
	rm -f $(ODIR)/attribute/*.o 
	rm $(FINAL_PROGRAM)
	rm $(POSTGRES_TO_SCIDB)
	rm $(SCIDB_TO_POSTGRES)

ex1: ex1.c
	$(CC) -o ex1 ex1.c $(CFLAGS)

check_stdin_stdout: src/check_stdin_stdout.cpp
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

# postgres to scidb
# src/fromPostgresToSdidb.cpp is compiled to obj/fromPostgresToScidb.o in the general rule for $(ODIR)/%.o 
$(POSTGRES_TO_SCIDB): obj/fromPostgresToScidb.o obj/$(POSTGRES_TO_SCIDB).o obj/postgres.o obj/attribute/attribute.o obj/utils.o obj/typeAttributeMap.o obj/buffer.o obj/postgres2csv.o $(DEPS)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

$(SCIDB_TO_POSTGRES): obj/fromScidbToPostgres.o obj/$(SCIDB_TO_POSTGRES).o obj/postgres.o obj/attribute/attribute.o obj/utils.o obj/typeAttributeMap.o obj/buffer.o $(DEPS)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)
