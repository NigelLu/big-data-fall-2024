INPUT_PATH="/users/xl3139/xl3139-hw1/data"
OUTPUT_PATH="/users/xl3139/xl3139-hw1/topten"
EXTRA_PATH="/users/xl3139/xl3139-hw1/extra"

# compile and run
mvn clean package
hadoop jar ./target/hw1-1.0-SNAPSHOT.jar --input $INPUT_PATH --output $OUTPUT_PATH --extra $EXTRA_PATH

# output result to txt files
rm ./topten.txt
rm ./extraCredit.txt
hadoop fs -cat $OUTPUT_PATH/part-r-00000 > topten.txt
hadoop fs -cat $EXTRA_PATH/part-r-00000 > extraCredit.txt