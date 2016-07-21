#! /bin/sh

#cp ./Step1/*.java ./exe
#cp ./Step2/*.java ./exe
#cp ./*.java ./exe

#cd ./exe && javac -classpath $LAB_CLASSPATH *.java

#jar cf Pred.jar *.class

#rm *.class && rm *.java

#cp *.jar ../

#cd ..

#rm -rf exe

cd ./Step1
javac -classpath $LAB_CLASSPATH *.java
jar cf Step1.jar *.class
rm *.class
scp Step1.jar  2016st21@114.212.190.91:./Pred
rm Step1.jar

cd ../Step2
javac -classpath $LAB_CLASSPATH *.java
jar cf Step2.jar *.class
rm *.class
scp Step2.jar  2016st21@114.212.190.91:./Pred
rm Step2.jar

cd ..

