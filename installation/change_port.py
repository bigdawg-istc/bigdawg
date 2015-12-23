#!/usr/bin/python
# Adam Dziedzic

import sys
import getopt
import fileinput

def main(argv):
    file=''
    port=''

    help="change_port.py -f <file> -p <port>"
    try:
        opts,args = getopt.getopt(argv,"hf:p:",["file=","port="])
    except:
        print help
        sys.exit(2)
    for opt,arg in opts:
        if opt=="-h":
            print help
        elif opt in ("-f","--file"):
            file=arg
        elif opt in ("-p","--port"):
            port=arg

    print "file: ",file
    print "port: ",port

    searched="#port = 5432"

    # fileinput is a special module to process lines of a file in place
    for line in fileinput.input(file,inplace=True):
        print line, # trailing comman to avoid the newline being printed
        if searched in line:
            print line.replace(searched,"port="+port),
    print "Port changed"

if __name__=="__main__":
    main(sys.argv[1:])
