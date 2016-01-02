import os

with open("inserts.sql", "w") as sql_file:
        for file in os.listdir("."):
                if ".csv" in file:
                        print file
                        with open(file, 'r') as csv_file:
                                for line in csv_file:
                                        if "##" in line:
                                                sql_file.write("--"+line[2:])
                                        elif "#" in line:
                                                table=line[1:]
                                                table=table.rstrip() # remove new line at the end of the table name
                                        else:
                                                line=line.rstrip() # remove new line at the end of the data line
                                                sql_file.write("insert into "+table+" values("+line+");\n")
