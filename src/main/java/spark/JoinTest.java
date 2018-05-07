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

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 * <p>
 * Usage: JavaStructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.JavaStructuredNetworkWordCount
 * localhost 9999`
 */
public final class JoinTest {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaStructuredNetworkWordCount <hostname> <port>");
			System.exit(1);
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);

		SparkSession spark = SparkSession
				.builder()
				.master("local[3]")
				.config("spark.defalut.parallelism", 3)
				.config("spark.sql.shuffle.partitions", 3)
				.appName("JavaStructuredNetworkWordCount")
				.getOrCreate();

		// Create DataFrame representing the stream of input lines from connection to host:port
		Dataset<Row> lines = spark
				.readStream()
				.format("socket")
				.option("host", host)
				.option("port", port)
				.load();

		// Split the lines into words
		Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
				(FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
				Encoders.STRING());
		// Generate running word count
		words.createOrReplaceTempView("table");

		spark.read().format("jdbc")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("url", "jdbc:mysql://10.33.108.69:6606")
				.option("dbtable", "merlin.rpt_real_kpi_mm")
				.option("user", "appRW")
				.option("password", "123456")
				.option("fetchsize", "3")
				.load().createOrReplaceTempView("test_table");
		spark.sql("select * from test_table where id in (1,2,3,4,5,6)").createOrReplaceTempView("join_table");
		Dataset<Row> table2 = spark.sql("select * from table left join join_table on join_table.id=table.value");
		Dataset<Row> table3 = spark.sql("select * from table");
		/*StreamingQuery query = table2.writeStream()
				.outputMode("append")
				.format("console")
				.start();*/
		StreamingQuery query2 = table3.writeStream()
				.outputMode("append")
				.format("console")
				.start();
		spark.streams().awaitAnyTermination();
		// query.awaitTermination();
		//query2.awaitTermination();
	}
}
