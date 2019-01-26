/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.util.matching.Regex


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {

    var num_cores = 4
    var source_path = "./access_log_Jul95"
    args.sliding(2, 2).toList.collect {
      case Array("--path", argPath: String) => source_path = argPath
      case Array("--cores", argCores: String) => num_cores = argCores.toInt
    }

    println("source_path: " + source_path)
    println("num_cores: " + num_cores)


    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(num_cores)

    val stream = env.readTextFile(source_path)
    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    def split_line(line: String): Request = {
      val request_pattern: Regex = """(.*) - - \[(.*) -0400\] "GET (.*) HTTP/1.0" ([0-9]{3}) (.*)""".r

      line match {
        case request_pattern(host, timestamp, resource, reply_code, reply_bytes) => Request(host, timestamp, resource, reply_code, reply_bytes)
        case _ => Request("", "", "", "", "")
      }
    }

    // execute program

    val format = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:m:ss")

    val splitted = stream
      .map(s => split_line(s))
      .filter(s => s.host != "")
      .assignAscendingTimestamps(s => format.parse(s.timestamp).getTime)

    val resources = splitted
      .map(s => (s.resource, 1))
      .keyBy(s => s._1)
      .sum(1)
      .map(s => ("Most requested resource", s._1, s._2))
      .keyBy(s => s._1)
      .maxBy(2)
      .map(s => Result(s._1, s._2))

    val clients = splitted
      .map(s => (s.host, 1))
      .keyBy(s => s._1)
      .sum(1)
      .map(s => ("Most common client", s._1, s._2))
      .keyBy(s => s._1)
      .maxBy(2)
      .map(s => Result(s._1, s._2))

    val outputTag = OutputTag[Result]("side-output")

    val output = resources
      .union(clients)
      .keyBy(s => s.metric)
      .filter(new FilterNewFunction)
      .process(new ProcessFunction[Result, Result] {
        override def processElement(
                                     value: Result,
                                     ctx: ProcessFunction[Result, Result]#Context,
                                     out: Collector[Result]): Unit = {
          // emit data to regular output
          out.collect(value)

          // emit data to side output
          ctx.output(outputTag, value)
        }
      })

    val sideOutputStream: DataStream[Result] = output.getSideOutput(outputTag)
    sideOutputStream
        .map(s => s.metric + ": " + s.value)
        .print()

    env.execute("DDM Flink Homework")

    val final_output = DataStreamUtils.collect[Result](output.javaStream).asScala.toList

    val final_results: mutable.HashMap[String, String] = mutable.HashMap()

    final_output.foreach(s => final_results.update(s.metric, s.value))

    println("")
    println("--- Final results ---")
    final_results.foreach(s => println(s._1 + ": " + s._2))
    println("")
  }

  case class Request(host: String, timestamp: String, resource: String, reply_code: String, reply_bytes: String)
  case class Result(metric: String, value: String)

  class FilterNewFunction extends RichFilterFunction[Result] {
    // keyed, managed state
    lazy val current_value: ValueState[Result] = getRuntimeContext.getState(
      new ValueStateDescriptor[Result]("current value", classOf[Result]))

    override def filter(result: Result): Boolean = {
      val current = current_value.value

      if (current == null || current.value != result.value){
        current_value.update(result)
        true
      }
      else{
        false
      }
    }
  }
}
