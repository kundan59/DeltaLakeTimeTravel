package org.examples;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

/**
 * CreateJobs is a class to create jobs.
 * you can create your own jobs
 */
public final class CreateJobs {

    private static final Logger LOGGER = Logger.getLogger(DeltaLakeTimeTravel.class);

    /**
     * Job 1 to write 100 integers from 100 to 200 into delta lake.
     *
     * @param sparkSession Spark Session.
     * @param file         delta Lake File
     */
    public void createJob1(final SparkSession sparkSession, final String file) {
        LOGGER.info("records created for job 1");
        sparkSession.range(100, 200).write().mode("overwrite").format("delta").save(file);
    }

    /**
     * Job 2 to update record created by job 1 from 100 to 200
     * into delta lake.
     *
     * @param sparkSession Spark Session.
     * @param file         delta Lake File
     */
    public void createJob2(final SparkSession sparkSession, final String file) {
        LOGGER.info("job 2 : update records created by job 1");
        sparkSession.range(100).write().mode("overwrite")
                .format("delta").option("overwriteSchema", "true").save(file);
    }
}
