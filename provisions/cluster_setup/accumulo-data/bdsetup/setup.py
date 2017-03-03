"""
Reads lines from a data file and inserts a number of records
"""

from pyaccumulo import Accumulo, Mutation, Range
import settings
import sys
sys.path
sys.path.append('/bdsetup')

table = "mimic_logs"

conn = Accumulo(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD)

if conn.table_exists(table):
    conn.delete_table(table)

conn.create_table(table)
wr = conn.create_batch_writer(table)

print "Ingesting some data ..."
f = open("/bdsetup/s00318.txt", "rb")
for i in range(250):
    line = f.readline().rstrip()
    label = '%04d'%i
    mut = Mutation('r_%s'%label)
    mut.put(cf='cf_%s'%label, cq='cq1', val=line)
    mut.put(cf='cf_%s'%label, cq='cq2', val=line)
    wr.add_mutation(mut)
    i += 1
wr.close()


# print "Rows 001 through 003 ..."
# for entry in conn.scan(table, scanrange=Range(srow='r_0001', erow='r_0003'), cols=[]):
#     print entry

# print "Rows 001 and 011 ..."
# for entry in conn.batch_scan(table, scanranges=[Range(srow='r_001', erow='r_001'), Range(srow='r_011', erow='r_011')]):
#     print entry

conn.close()

