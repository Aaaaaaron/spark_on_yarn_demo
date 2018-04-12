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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes an approximation to pi
 * Usage: JavaSparkPiLocal [partitions] */
public final class JavaSparkFileDemo {
    private static final Logger logger = LoggerFactory.getLogger(JavaSparkPiLocal.class);

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf();
        conf.set("spark.executor.extraJavaOptions", "-Dhdp.version=current");
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
        conf.set("spark.yarn.dist.archives", "hdfs:///aron/test_file.zip");
        //        conf.set("spark.yarn.jars", "hdfs:///spark-jars/jars/*.jar");
        //        conf.set("spark.driver.host", "10.2.0.11");
        //        conf.set("spark.hadoop.yarn.resourcemanager.hostname", "10.1.1.77");
        //        conf.set("spark.hadoop.yarn.resourcemanager.address", "10.1.1.77:8050");
        conf.set("spark.driver.host", "10.1.0.24");

        SparkSession spark = SparkSession.builder().appName("JavaSparkPiLocal").master("yarn").config(conf)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        //        jsc.addFile("hdfs:///aron/test_file.csv");
        //        System.out.println("aaaaa:" + SparkFiles.get("test_file.csv"));
        //        Source.fromFile()
        final int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {

                RandomAccessFile in = new RandomAccessFile("./test_file1.csv", "r");
                RandomAccessFile in2 = new RandomAccessFile("./test_file2.csv", "r");
                System.out.println("innnnnnnnnnn:" + in.readChar());
                System.out.println("innnnnnnnnnn2:" + in2.readChar());

                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;

                return (x * x + y * y <= 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println("Pi is roughly " + 4.0 * count / n);
        Thread.sleep(20000 * 1000); // 2 seconds

        spark.stop();
    }
}
