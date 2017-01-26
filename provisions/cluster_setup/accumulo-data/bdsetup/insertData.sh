#! /usr/bin/octave --traditional

javaaddpath("/bdsetup/d4m_api/lib/graphulo-2.6.3-alldeps.jar")
addpath('/bdsetup/d4m_api/matlab_src/');
DBinit;
Assoc('','','');

%setup bindings to DB
hostname='zookeeper.docker.local';
port=':2181';
dbname='accumulo';
user='bigdawg';
password='bigdawg';

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

inDir='/bdsetup/data';
D=dir([inDir '/*.mat']);

for i=1:numel(D);

    %read in edge
    %read in text

    fileToLoad=[inDir '/' D(i).name];

    if(strfind(D(i).name, 'edge'))
        disp(["loading " fileToLoad])
        load(fileToLoad);

        put(Tedge, num2str(Aout));
	put(TedgeDeg,putCol(num2str(sum(Aout,1).'),['Degree' char(10)]));

    elseif(strfind(D(i).name, 'txt'))
        disp(["loading " fileToLoad])
        load(fileToLoad);
	put(TedgeTxt,Atxtout);	
    end

end

exit
