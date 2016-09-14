# CMIGRATOR docs #

## BOOST

version >= 1.60.0

You can check the boost version on Ubuntu in the following way:

$ dpkg -s libboost-dev | grep 'Version'

Version: 1.54.0.1ubuntu1

if there is no boost you can try to install it from the ubuntu repository:

sudo apt-get install libboost-all-dev

Check the version!

The version 1.54.0 is too old. We have to install the latest one.

wget https://sourceforge.net/projects/boost/files/boost/1.60.0/boost_1_60_0.tar.gz/download

tar xzvf boost_1_60_0.tar.gz

cd boost_1_60_0/

sudo apt-get update

sudo apt-get install build-essential g++ python-dev autotools-dev libicu-dev build-essential libbz2-dev libboost-all-dev

./bootstrap.sh --prefix=/usr/local

./b2

sudo ./b2 install

you can add the following entries to: .bashrc

export LIBS="-L/usr/local/lib":$LIBS

export CPPFLAGS="-I/usr/local/include/boost"

export BOOST_INCLUDEDIR=/usr/local/include/boost/

export BOOST_LIBRARYDIR=/usr/local/lib/

export BOOST_ROOT=/home/adam/Downloads/boost_1_60_0

export INCLUDE="/usr/local/include/boost/:$INCLUDE"

export LIBRARY_PATH="/usr/local/lib/:$LIBRARY_PATH"

Or in your makefiles you can add: CPPFLAGS += -L/usr/local/include/boost

## MAKEFILE
to run the old makefile: make -f Makefile

## CMAKE
Cmake is a preferred way to compile cmigrator. Create a folder build, run cmake .. and then make. It generates the shared library: data-migrator.so and the executable: data-migrator-exe.

## EXAMPLES
adam@gaia:~/cmigrator$ ./data-migrator -t postgres2scidb -i /home/adam/data/scidb/bool_table_test.bin -o /home/adam/data/scidb/bool_table_test_to_scidb2.bin -f'bool null'

AFL% load(bool_array4,'/home/adam/data/scidb/bool_table_test_to_scidb2.bin',-2,'(bool null)');

### Full example for data migration between PostgreSQL and SciDB for basic types: bool, int, double and string

```sql
// create table in PostgreSQL

create table test_all (a bool not null, b bool, c int not null, d int, e double precision not null, f double precision, g varchar not null, h varchar);
insert into test_all values(true, null, 1, null, 1.0, null, 'adam', 'dziedzic');
insert into test_all values(true, false, 1, 2, 1.2, 1.3, 'aaron', 'elmore');
insert into test_all values(false, null, 4, 5, 2.1, 2.3, 'barack', 'obama');

// copy table to external file
copy test_all to '/home/adam/data/test_all_postgres.bin' with (format binary);

// transform from binary postgres to binary scidb
~/bigdawgmiddle/src/main/cmigrator$ 
mkdir build
cd build
cmake ..
make
./data-migrator-exe -t postgres2scidb -i /home/${USER}/data/test_all_postgres.bin -o /home/${USER}/data/test_all_scidb.bin -f"bool,bool null,int, int null, double, double null, string, string null"

cd /home/adam/data
sudo cp test_all_scidb.bin /home/scidb/data
sudo chown scidb:scidb /home/scidb/data/test_all_scidb.bin

sudo su scidb
cd
cd data

AFL% load(test_all,'/home/scidb/data/test_all_scidb.bin',-2,'(bool, bool null, int32, int32 null, double, double null, string, string null)');
{i} a1,a2,b1,b2,c1,c2,d1,d2
{0} true,null,1,null,1,null,'adam','dziedzic'
{1} true,false,1,2,1.2,1.3,'aaron','elmore'
{2} false,null,4,5,2.1,2.3,'barack','obama'

AFL% save(test_all,'/home/scidb/data/test_all_scidb2.bin',-2,'(bool, bool null, int32, int32 null, double, double null, string, string null)');
{i} a1,a2,b1,b2,c1,c2,d1,d2
{0} true,null,1,null,1,null,'adam','dziedzic'
{1} true,false,1,2,1.2,1.3,'aaron','elmore'
{2} false,null,4,5,2.1,2.3,'barack','obama'

adam@gaia:~/data$ sudo cp /home/scidb/data/test_all_scidb2.bin .
adam@gaia:~/data$ sudo chown adam:adam /home/scidb/data/test_all_scidb2.bin .

adam@gaia:~/bigdawgmiddle/src/main/cmigrator/build$ ./data-migrator-exe -t scidb2postgres -i /home/${USER}/data/test_all_scidb2.bin -o /home/${USER}/data/test_all_postgres2.bin -f"bool,bool null,int, int null, double, double null, string, string null"

psql: test=# create table test_all2 (a bool not null, b bool, c int not null, d int, e double precision not null, f double precision, g varchar not null, h varchar);

test=# create table test_all2 (a bool not null, b bool, c int not null, d int, e double precision not null, f double precision, g varchar not null, h varchar);
CREATE TABLE
test=# copy test_all2 from '/home/adam/data/test_all_postgres2.bin' with (format binary);
COPY 3
test=# select * from test_all2;
 a | b | c | d |  e  |  f  |   g    |    h     
---+---+---+---+-----+-----+--------+----------
 t |   | 1 |   |   1 |     | adam   | dziedzic
 t | f | 1 | 2 | 1.2 | 1.3 | aaron  | elmore
 f |   | 4 | 5 | 2.1 | 2.3 | barack | obama
(3 rows)

```

## TESTS
We use google tests: https://github.com/google/googletest/blob/master/googletest/docs/Primer.md

the test are in cmigrator/test/main_tests

Tests were integrated with cmake.
Go to bigdawgmiddle/src/main/cmigrator/build and execute:
```
cmake ..
make
cmake --build .
ctest -VV
```

The test files are specified in bigdawgmiddle/src/main/cmigrator/CMakeLists.txt file in the line: file(GLOB TEST_SRC_FILES ${PROJECT_SOURCE_DIR}/test/main_tests/*.cc). The main test files are currently in bigdawgmiddle/src/main/cmigrator/test/main_tests/*.cc 

### obsolete version
run make to build the tests

all the tests can be run with: ./runTests

each test can be run individually, e.g.: ./attribute_unittest

## GDB
change the CMakeLists.txt (add -ggdb here: set(CMAKE_CXX_FLAGS " -ggdb -Wall ... )

go to build:

gdb data-migrator-exe
$ file data-migrator-exe
$ list
$ b 12 // set breakpoint at line 12
$ info sources

to run the tests go to bigdawgmiddle/src/main/cmigrator/build: 
gdb --args 

