# clean everything
rm check_stdin_stdout
rm postgres2scidb
make clean

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
