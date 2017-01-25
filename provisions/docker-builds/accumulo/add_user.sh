#!/bin/bash

export USERNAME=${1:-bob}
export PASSWORD=${2:-robert}

su - accumulo << ENDE
date
echo -e "${PASSWORD}\n${PASSWORD}\n" | /usr/lib/accumulo/bin/accumulo shell -u root -p DOCKERDEFAULT -e "createuser ${USERNAME}" 2>/dev/null
echo -e "${PASSWORD}\n" | /usr/lib/accumulo/bin/accumulo shell -u root -p DOCKERDEFAULT -e "authenticate ${USERNAME}" 2>/dev/null
echo "Allow ${USERNAME} to create tables"
/usr/lib/accumulo/bin/accumulo shell -u root -p DOCKERDEFAULT -e "grant System.CREATE_TABLE -s -u ${USERNAME}" 2>/dev/null
ENDE
