package p;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.metrics.source.Source;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageStatus;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

/**
 * Example of using Spark's status APIs from Java.
 */

public final class JavaStatusTrackerDemo {

    public static final String APP_NAME = "JavaStatusAPIDemo";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
/*   conf.set("spark.executor.extraJavaOptions", "-Dhdp.version=current");
        conf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=current");
        conf.set("spark.driver.extraJavaOptions", "-Dhdp.version=current");

        conf.set("spark.yarn.dist.jars", "/Users/jiatao.tao/Documents/project/test_spark/target/aron-1.0-SNAPSHOT.jar");
        conf.set("spark.driver.memory", "512m");
        conf.set("spark.executor.memory", "512m");
        conf.set("spark.task.maxFailures", "1");
        conf.set("spark.ui.port", "4041");
        conf.set("spark.sql.dialect", "hiveql");
        conf.set("spark.yarn.am.memory", "512m");
        conf.set("spark.executor.cores", "1");
        conf.set("spark.executor.instances", "3");
        conf.set("spark.hadoop.yarn.timeline-service.enabled", "false");
        conf.set("spark.yarn.archive", "hdfs:///spark-jars/sparkjars.zip");
        conf.set("spark.driver.host", "10.1.0.24");
        //        conf.set("spark.yarn.jars", "hdfs:///spark-jars/jars/*.jar");
        //        conf.set("spark.driver.host", "10.2.0.11");
        //        conf.set("spark.hadoop.yarn.resourcemanager.hostname", "10.1.1.77");
        //        conf.set("spark.hadoop.yarn.resourcemanager.address", "10.1.1.77:8050");
 */
//        SparkSession spark = SparkSession.builder().appName(APP_NAME).master("yarn").config(conf).getOrCreate();
        SparkSession spark = SparkSession.builder().appName(APP_NAME).master("local").config(conf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//        jsc.setLogLevel("WARN");
        // Example of implementing a progress reporter for a simple job.
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                Thread.sleep(2 * 1000); // 2 seconds
                return v1 * 10;
            }
        });

        JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
        while (!jobFuture.isDone()) {
            Thread.sleep(1000); // 1 second
            List<Integer> jobIds = jobFuture.jobIds();
            if (jobIds.isEmpty()) {
                continue;
            }
            int currentJobId = jobIds.get(jobIds.size() - 1);
            SparkJobInfo jobInfo = jsc.statusTracker().getJobInfo(currentJobId);
            SparkStageInfo stageInfo = jsc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
            System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() + " active, "
                    + stageInfo.numCompletedTasks() + " complete");
        }

        StorageStatus[] storageStatus = SparkEnv.get().blockManager().master().getStorageStatus();
        Seq<Source> executorSource = SparkEnv.get().metricsSystem().getSourcesByName("executor");
        Seq<Source> executorSource2 = SparkEnv.get().metricsSystem().getSourcesByName("executorSource");
        int totalNumBlocks = 0;
        int totalMemUsed = 0;
        int totalDiskUsed = 0;
        for (StorageStatus status : storageStatus) {
            totalNumBlocks += status.numBlocks();
            totalMemUsed += status.memUsed();
            totalDiskUsed += status.diskUsed();
        }


        System.out.println("isTraceEnabled:::" + SparkEnv.get().metricsSystem().isTraceEnabled());
        System.out.println("totalNumBlocks:::::" + totalNumBlocks);
        System.out.println("totalMemUsed:::::" + totalMemUsed);
        System.out.println("totalDiskUsed:::::" + totalDiskUsed);
        System.out.println("Job results are: " + jobFuture.get());
        Thread.sleep(20000 * 1000); // 2 seconds
        spark.stop();
    }


}
