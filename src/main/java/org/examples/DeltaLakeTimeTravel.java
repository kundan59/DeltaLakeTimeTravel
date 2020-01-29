package org.examples;

import io.delta.tables.DeltaTable;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;

/**
 * DeltaLakeTimeTravel is a class to illustrate TIME TRAVEL USING DELTA LAKE .
 */
final public class DeltaLakeTimeTravel {

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

        //Job-1 writing data into delta lake
        Dataset<Long> data = sparkSession.range(100, 200);
        data.write().mode("overwrite").format("delta").save(FILE_PATH);
        LOGGER.info("records created after job-1: " + readDeltaTable(sparkSession).count());
        Dataset<Row> job1DeltaTable = readDeltaTable(sparkSession);
        job1DeltaTable.show();

        //Job-2 updating record created by Job-1
        sparkSession.range(100).map((MapFunction<Long, Integer>)
                Math::toIntExact, Encoders.INT())
                .write().mode("overwrite").format("delta").option("overwriteSchema", "true").save(FILE_PATH);
        LOGGER.info("records created after job-2: " + readDeltaTable(sparkSession).count());
        Dataset<Row> job2DeltaTable = readDeltaTable(sparkSession);
        job2DeltaTable.show();

        //Read first version of table using version number.
        Dataset<Row> version_0_DeltaLakeTable = timeTravelUsingVersion(sparkSession);
        version_0_DeltaLakeTable.show();

        //find timestamp for first version of delta table
        Dataset<Row> load = sparkSession.read().format("json").option("multiline", true).load(FIRST_COMMIT_FILE_PATH);
        Dataset<Row> timestampDataSet = load.select("commitInfo.timestamp");
        String timestamp = new Timestamp(timestampDataSet.first().getLong(0)).toString();

        //Read first version of table using its time Stamp.
        Dataset<Row> firstVersionDeltaLakeTable = timeTravelUsingTimeStamp(sparkSession, timestamp);
        firstVersionDeltaLakeTable.show();

        //get the latest History of of delta table
        Dataset<Row> latestHistory = getLatestHistory(sparkSession);
        latestHistory.select("version","timestamp","operation","operationParameters").show(false);

        //close Spark Session
        sparkSession.close();
    }

    /**
     * Read delta lake table.
     *
     * @param sparkSession sSpark Session.
     * @return  Dataset<Row> Data set.
     */
    private static Dataset<Row> readDeltaTable(SparkSession sparkSession) {
        return sparkSession.read()
                .format("delta").load(DeltaLakeTimeTravel.FILE_PATH);
    }

    /**
     * Time Travel Using Version Number.
     *
     * @param sparkSession sSpark Session.
     * @return  Dataset<Row> Data set.
     */
    private static Dataset<Row> timeTravelUsingVersion(SparkSession sparkSession) {
        return sparkSession.read().option("versionAsOf", NumberUtils.INTEGER_ZERO)
                .format("delta")
                .load(DeltaLakeTimeTravel.FILE_PATH);
    }

    /**
     * Time Travel Using Time Stamp.
     *
     * @param sparkSession sSpark Session.
     * @param timeStamp time stamp of  first version of Table.
     * @return  Dataset<Row> Data set.
     */
    private static Dataset<Row> timeTravelUsingTimeStamp(SparkSession sparkSession, String timeStamp) {
        return sparkSession.read().option("timestampAsOf", timeStamp)
                .format("delta")
                .load(DeltaLakeTimeTravel.FILE_PATH);
    }
    /**
     * Latest history of delta table.
     *
     * @param sparkSession sSpark Session.
     * @return  Dataset<Row> Data set.
     */
    private static Dataset<Row> getLatestHistory(SparkSession sparkSession) {
        DeltaTable deltaTable = DeltaTable.forPath(sparkSession, DeltaLakeTimeTravel.FILE_PATH);
        return deltaTable.history();
    }
}
