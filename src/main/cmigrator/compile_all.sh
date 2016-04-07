# clean everything
rm check_stdin_stdout
rm postgres2scidb
rm scidb2postgres
make clean

mkdir -p obj/attribute
# tests
cwd=`pwd`
cd ${cwd}/test

make clean
make
./attribute_unittest

cd ${cwd}

# main program
make
make check_stdin_stdout
make postgres2scidb
make scidb2postgres

