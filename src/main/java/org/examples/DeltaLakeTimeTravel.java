package org.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DeltaLakeTimeTravel is a class to illustrate TIME TRAVEL USING DELTA LAKE .
 */
public final class DeltaLakeTimeTravel {

    private static final Logger LOGGER = Logger.getLogger(DeltaLakeTimeTravel.class);
    private final static String SPARK_APPLICATION_NAME = "DeltaLakeTimeTravel";
    private final static String SPARK_APPLICATION_RUNNING_MODE = "local";
    private final static String FILE_PATH = "sparkdata/deltalakedata";
    private final static String FIRST_COMMIT_FILE_PATH = "sparkdata/deltalakedata/_delta_log/00000000000000000000.json";

    public static void main(String[] args) {

        // Turn off spark's default logger
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder().appName(SPARK_APPLICATION_NAME)
                .master(SPARK_APPLICATION_RUNNING_MODE)
                .getOrCreate();

        //Instantiate required classes
        CreateJobs createJobs = new CreateJobs();
        TimeTravelApproaches timeTravelApproaches = new TimeTravelApproaches();

        //Job-1 writing data into delta lake
        createJobs.createJob1(sparkSession, FILE_PATH);
        LOGGER.info("records created after job-1: " + readDeltaTable(sparkSession).count());
        Dataset<Row> job1DeltaTable = readDeltaTable(sparkSession);
        job1DeltaTable.show();

        //Job-2 updating record created by Job-1
        createJobs.createJob2(sparkSession, FILE_PATH);
        LOGGER.info("records created after job-2: " + readDeltaTable(sparkSession).count());
        Dataset<Row> job2DeltaTable = readDeltaTable(sparkSession);
        job2DeltaTable.show();

        //Read first version of table using version number.
        Dataset<Row> version_0_DeltaLakeTable = timeTravelApproaches
                .timeTravelUsingVersionNumber(sparkSession, FILE_PATH);
        version_0_DeltaLakeTable.show();

        //Read first version of table using its time Stamp.
        Dataset<Row> firstVersionDeltaLakeTable = timeTravelApproaches
                .timeTravelUsingTimeStamp(sparkSession, FILE_PATH, FIRST_COMMIT_FILE_PATH);
        firstVersionDeltaLakeTable.show();

        //get the latest History of of delta table
        Dataset<Row> latestHistory = timeTravelApproaches.getLatestHistory(sparkSession, FILE_PATH);
        latestHistory.select("version", "timestamp", "operation", "operationParameters").show(false);

        //close Spark Session
        sparkSession.close();
    }

    /**
     * Read delta lake table.
     *
     * @param sparkSession Spark Session.
     * @return Dataset<Row> Data set.
     */
    private static Dataset<Row> readDeltaTable(SparkSession sparkSession) {
        return sparkSession.read()
                .format("delta").load(DeltaLakeTimeTravel.FILE_PATH);
    }
}
