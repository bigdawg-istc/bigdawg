from wfdbtools import rdsamp
record  = 'a40001_000001'
data, info = rdsamp(record, 0, 15)
with open(record+".csv", "wb") as f:
    for val in iter(data[:,2]):
        f.write(str(val) + "\n")

