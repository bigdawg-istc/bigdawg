#! /usr/bin/octave --traditional

cd /image_contents
javaaddpath("./d4m_api/lib/graphulo-2.6.3-alldeps.jar")
addpath('./d4m_api/matlab_src/');

%setup bindings to DB
hostname='zookeeper.docker.local';
port=':2181';
dbname='accumulo';
user='root';
password='DOCKERDEFAULT';

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

inDir='./data';
D=dir([inDir '/*.mat']);

for i=1:numel(D);

    %read in edge
    %read in text

    fileToLoad=[inDir '/' D(i).name];

    if(strfind(D(i).name, 'edge'))
        disp(["loading " fileToLoad])
        load(fileToLoad);

        [r,c,v] = find(Aout.A);
        r = [regexprep(int2str(r), '\s*', ','),","];  % take a vector of integers [1,2,3] and convert to a comma-separated string: "1,2,3,"
        c = [regexprep(int2str(c), '\s*', ','),","];
        v = [regexprep(int2str(v), '\s*', ','),","];
        put(Tedge, r, c, v);

        [r,c,v] = find(sum(Aout.A,1));
        r = [regexprep(int2str(r), '\s*', ','),","];
        c = [regexprep(int2str(c), '\s*', ','),","];
        v = [regexprep(int2str(v), '\s*', ','),","];
        putTriple(TedgeDeg, r, c, v);

    elseif(strfind(D(i).name, 'txt'))
        disp(["loading " fileToLoad])
        load(fileToLoad);
        [r,c,v] = find(Atxtout.A);
        r = [regexprep(int2str(r'), '\s*', ','),","];
        c = [regexprep(int2str(c'), '\s*', ','),","];
        v = [regexprep(int2str(v'), '\s*', ','),","];
        putTriple(TedgeTxt, r, c, v);

    end

end

exit