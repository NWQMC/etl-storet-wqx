# cache the current dir and check it
cwd=$(pwd)
echo "working dir is ${cwd}"

# need the spring boot batch app from story fork to build the jar
rm -rf ETL_Spike
git clone git@github.com:duselmann/ETL-Spike.git ETL_Spike
cd ETL_Spike
git checkout origin/job-id-date
/opt/tomcat/.jenkins/tools/hudson.tasks.Maven_MavenInstallation/Maven_3.5.4/bin/mvn package

# ant is looking for app.jar
mv target/wqp-etl-base-0.0.3-SNAPSHOT.jar ${cwd}/app.jar

# back to the workspacea dir
cd $cwd

