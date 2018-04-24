#!/bin/bash
echo "Setting up mimic2 data..."
/opt/vertica/bin/vsql -U dbadmin -c 'ALTER DATABASE docker SET StandardConformingStrings = 0;'
/opt/vertica/bin/vsql -U dbadmin -f '/home/dbadmin/mimic_dump_vertica.sql'