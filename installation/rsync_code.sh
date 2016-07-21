cwd=$(pwd)
cd ../
for machine in madison francisco; do
    for module in "src/" "pom.xml" "profiles/"; do
	rsync -avxP ${module} adam@${machine}:/home/adam/bigdawgmiddle/${module}
    done
done
