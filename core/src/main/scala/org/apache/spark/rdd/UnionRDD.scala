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

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, RangeDependency, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Partition for UnionRDD.
 *
 * @param idx index of the partition
 * @param rdd the parent RDD this partition refers to
 * @param parentRddIndex index of the parent RDD this partition refers to
 * @param parentRddPartitionIndex index of the partition within the parent RDD
 *                                this partition refers to
 */
private[spark] class UnionPartition[T: ClassTag](
    idx: Int,
    @transient private val rdd: RDD[T],
    val parentRddIndex: Int,
    @transient private val parentRddPartitionIndex: Int)
  extends Partition {

  var parentPartition: Partition = rdd.partitions(parentRddPartitionIndex)

  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    parentPartition = rdd.partitions(parentRddPartitionIndex)
    oos.defaultWriteObject()
  }
}

object UnionRDD {
  private[spark] lazy val partitionEvalTaskSupport =
    new ForkJoinTaskSupport(ThreadUtils.newForkJoinPool("partition-eval-task-support", 8))
}

@DeveloperApi
class UnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  // visible for testing
  private[spark] val isPartitionListingParallel: Boolean =
    rdds.length > conf.get(RDD_PARALLEL_LISTING_THRESHOLD)

  override def getPartitions: Array[Partition] = {
    val parRDDs = if (isPartitionListingParallel) {
      val parArray = new ParVector(rdds.toVector)
      parArray.tasksupport = UnionRDD.partitionEvalTaskSupport
      parArray
    } else {
      rdds
    }
    // ???????????????rdd???????????????
    val array = new Array[Partition](parRDDs.map(_.partitions.length).sum)
    var pos = 0
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      // pos: ????????????, rdd: ???rdd, rddIndex: ???rdd??????, split.index: ???rdd??????????????????
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      /**
       * RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
       * rdd: ???rdd
       * inStart: ???range??????rdd???????????????, ????????? 0
       * outStart: ???range??????rdd???????????????(????????????rdd?????????rdd?????????????????????), ?????????: 0, rdd1.partitions.length, rdd1.partitions.length + rdd2.partitions.length, ...
       * length: ???range ?????????, ???rdd????????????
       * ???rdd???partitionId?????????rdd?????????: partitionId - outStart(inStart????????????)
       */
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps.toSeq
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    // parent[T](part.parentRddIndex): ???rdd
    // part.parentPartition: ?????????rdd?????????
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
