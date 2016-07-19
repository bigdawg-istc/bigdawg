for machine in madison francisco; do
    rsync -avxP ../src/ adam@${machine}:/home/adam/bigdawgmiddle/src/
    rsync -avxP ../pom.xml adam@${machine}:/home/adam/bigdawgmiddle/pom.xml
    
done
