#! /usr/local/bin/octave --traditional

cd /bdsetup
addpath('./d4m_api/matlab_src/');


%setup bindings to DB
hostname='classdb55.cloud.llgrid.txe1.mit.edu';
port=':2181';
dbname='classdb55';
user='AccumuloUser';
password='T9RUdInhqSuRSfrUQHzO24Y~d';

DB=DBserver([hostname port], 'Accumulo', dbname, user, password);

if(ls(DB))
    disp('Binding done correctly');
else
    disp('Error creating binding');
    exit;
end

myName = 'note_events_';

Tedge = DB([myName 'Tedge'],[myName 'TedgeT']);         % Create database table pair for holding incidense matrix.

TedgeDeg = DB([myName 'TedgeDeg']);                     % Create database table for counting degree.

TedgeTxt = DB([myName 'TedgeTxt']);


deleteForce(Tedge);
deleteForce(TedgeDeg);
deleteForce(TedgeTxt);

exit;



