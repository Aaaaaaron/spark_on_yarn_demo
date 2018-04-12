package p

import org.apache.spark.scheduler.SparkListenerInterface

import scala.collection.mutable

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

object Test {
  def main(args: Array[String]): Unit = {
    val nums: List[Int] = List(1, 2, 3, 4)
    val aWithB = new mutable.HashMap[String, Int]()

    nums.foreach {
      case 1 => aWithB.put("one", 1)
      case 2 => aWithB.put("two", 2)
      case 3 => aWithB.put("three", 3)
      case _ => Unit
    }

    println(aWithB)

  }
}
