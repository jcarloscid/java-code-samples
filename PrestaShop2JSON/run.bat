@ECHO OFF
SET CLASSPATH=.\bin;.\lib\mariadb-java-client-2.2.1.jar 
java -DMessageLogger.mode=VERBOSE com.indigoid.PrestaShop2JSON All localhost 3306 prestashop
IF ERRORLEVEL 0 GOTO Import
GOTO End
:Import
mongoimport -d sample -c customers \tmp\customers.json
mongoimport -d sample -c products \tmp\products.json
mongoimport -d sample -c orders \tmp\orders.json
:End
EXIT /B