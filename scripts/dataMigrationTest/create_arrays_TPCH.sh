
iquery -aq "DROP ARRAY region"



iquery -aq "CREATE ARRAY region < r_regionkey:int64, r_name:string, r_comment:string > [k=0:*,1048576,0]"
