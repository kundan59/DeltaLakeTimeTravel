package org.examples;

import io.delta.tables.DeltaTable;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;

/**
 * TimeTravelApproaches is a class to illustrate Time travel approaches in Delta lake.
 */
public class TimeTravelApproaches {

    private static final Logger LOGGER = Logger.getLogger(DeltaLakeTimeTravel.class);

    /**
     * Time Travel Using Time Stamp.
     *
     * @param sparkSession    sSpark Session.
     * @param deltaTable      file consist records.
     * @param firstCommitFile commit file created by job 1.
     * @return Dataset<Row> Data set.
     */
    public Dataset<Row> timeTravelUsingTimeStamp(final SparkSession sparkSession,
                                                 final String deltaTable,
                                                 final String firstCommitFile) {

        LOGGER.info("read version_0 records in a delta lake using Time Stamp");
        Dataset<Row> load = sparkSession.read().format("json").option("multiline", true).load(firstCommitFile);
        Dataset<Row> timestampDataSet = load.select("commitInfo.timestamp");
        String timestamp = new Timestamp(timestampDataSet.first().getLong(0)).toString();

        return sparkSession.read().option("timestampAsOf", timestamp)
                .format("delta")
                .load(deltaTable);
    }

    /**
     * Time Travel Using Version number zero.
     *
     * @param sparkSession   Spark Session.
     * @param deltaTableFile file consist records.
     * @return Dataset<Row> Data set.
     */
    public Dataset<Row> timeTravelUsingVersionNumber(final SparkSession sparkSession,
                                                     final String deltaTableFile) {

        LOGGER.info("read version_0 records in a delta lake version number");
        return sparkSession.read().option("versionAsOf", NumberUtils.INTEGER_ZERO)
                .format("delta")
                .load(deltaTableFile);
    }

    /**
     * Latest history of delta table.
     *
     * @param sparkSession Spark Session.
     * @return Dataset<Row> Data set.
     */
    public Dataset<Row> getLatestHistory(final SparkSession sparkSession, final String deltaFile) {
        LOGGER.info("Audit changes in delta table");
        DeltaTable deltaTable = DeltaTable.forPath(sparkSession, deltaFile);
        return deltaTable.history();
    }


}
