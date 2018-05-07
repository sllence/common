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
package spark;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: JavaStructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.JavaStructuredNetworkWordCount
 *    localhost 9999`
 */
public final class SqlTest {

  public static void main(String[] args) throws Exception {


    SparkSession spark = SparkSession
      .builder()
            .master("local[3]")
            .config("spark.defalut.parallelism",3)
            .config("spark.sql.shuffle.partitions",3)
      .appName("JavaStructuredNetworkWordCount")
      .getOrCreate();


    Dataset<Row> joinTable = spark.read().format("jdbc")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("url", "jdbc:mysql://172.16.3.147:6606")
            .option("dbtable", "merlin.rpt_real_kpi_mm")
            .option("user", "appRW")
            .option("password", "123456")
            .option("fetchsize", "3")
            .load();
    joinTable.createOrReplaceTempView("join_table");
    Dataset<Row> wordCounts = spark.sql("select * from join_table where id in (1,2,3,4,5,6)");
    //Dataset<Row> wordCounts = words.groupBy("value").count();

    // Start running the query that prints the running counts to the console
    wordCounts.foreach((ForeachFunction<Row>) SqlTest::print);
  }

  public static void print(Object object) {
    System.out.println(object);
  }
}
