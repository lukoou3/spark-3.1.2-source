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

package org.apache.spark.sql.catalyst.util

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.sql.catalyst.util.QuantileSummaries.Stats

/**
 * 用于计算近似分位数摘要的帮助程序类。该实现基于Greenwald，Michael和Khanna，Sanjeev的论文“分位数摘要的空间有效在线计算”中提出的算法。(https://doi.org/10.1145/375663.375670)
 *
 * 为了优化速度，它保留了最后一次看到的样本的内部缓冲区，并且只在超过一定大小阈值后插入它们。与原始算法相比，这保证了近乎恒定的运行时复杂性。
 *
 * Helper class to compute approximate quantile summary.
 * This implementation is based on the algorithm proposed in the paper:
 * "Space-efficient Online Computation of Quantile Summaries" by Greenwald, Michael
 * and Khanna, Sanjeev. (https://doi.org/10.1145/375663.375670)
 *
 * In order to optimize for speed, it maintains an internal buffer of the last seen samples,
 * and only inserts them after crossing a certain size threshold. This guarantees a near-constant
 * runtime complexity compared to the original algorithm.
 *
 * @param compressThreshold the compression threshold.
 *   After the internal buffer of statistics crosses this size, it attempts to compress the
 *   statistics together.
 * @param relativeError the target relative error.
 *   It is uniform across the complete range of values.
 * @param sampled a buffer of quantile statistics.
 *   See the G-K article for more details.
 * @param count the count of all the elements *inserted in the sampled buffer*
 *              (excluding the head buffer)
 * @param compressed whether the statistics have been compressed
 */
class QuantileSummaries(
    val compressThreshold: Int,
    val relativeError: Double,
    val sampled: Array[Stats] = Array.empty, // Greenwald Khanna论文的统计数据
    val count: Long = 0L, // 插入的总数
    var compressed: Boolean = false // statistics是否压缩
) extends Serializable {

  // 迄今为止看到的最新样本缓冲区
  // a buffer of latest samples seen so far
  private val headSampled: ArrayBuffer[Double] = ArrayBuffer.empty

  import QuantileSummaries._

  /**
   * 返回一个摘要，其中插入了给定的观察结果。此方法可以就地修改当前摘要（并返回相同的就地修改的摘要），也可以根据需要从头开始创建新的摘要。
   * Returns a summary with the given observation inserted into the summary.
   * This method may either modify in place the current summary (and return the same summary,
   * modified in place), or it may create a new summary from scratch it necessary.
   * @param x the new observation to insert into the summary
   */
  def insert(x: Double): QuantileSummaries = {
    headSampled += x // 最新样本采样
    compressed = false // 插入就设置为没压缩
    if (headSampled.size >= defaultHeadSize) { //采样大于50000
      val result = this.withHeadBufferInserted
      // 抽样结果大于10000，压缩
      if (result.sampled.length >= compressThreshold) {
        result.compress()
      } else {
        result
      }
    } else {
      this
    }
  }

  /**
   * 在批次中插入一个数组（未排序的样本），首先对数组进行排序，以遍历单个批次中的汇总统计信息。
   * 此方法不修改当前对象，并在必要时返回一个新副本。
   *
   * Inserts an array of (unsorted samples) in a batch, sorting the array first to traverse
   * the summary statistics in a single batch.
   *
   * This method does not modify the current object and returns if necessary a new copy.
   *
   * @return a new quantile summary object.
   */
  private def withHeadBufferInserted: QuantileSummaries = {
    if (headSampled.isEmpty) {
      return this
    }
    var currentCount = count
    // 排序
    val sorted = headSampled.toArray.sorted
    val newSamples: ArrayBuffer[Stats] = new ArrayBuffer[Stats]()
    // 下一个插入元素的index
    // The index of the next element to insert
    var sampleIdx = 0
    // index遍历排序后的抽样数组
    // The index of the sample currently being inserted.
    var opsIdx: Int = 0
    while (opsIdx < sorted.length) {
      val currentSample = sorted(opsIdx)
      // Add all the samples before the next observation.
      while (sampleIdx < sampled.length && sampled(sampleIdx).value <= currentSample) {
        newSamples += sampled(sampleIdx)
        sampleIdx += 1
      }

      // If it is the first one to insert, of if it is the last one
      currentCount += 1
      val delta =
        if (newSamples.isEmpty || (sampleIdx == sampled.length && opsIdx == sorted.length - 1)) {
          0
        } else {
          math.floor(2 * relativeError * currentCount).toLong
        }

      val tuple = Stats(currentSample, 1, delta)
      newSamples += tuple
      opsIdx += 1
    }

    // Add all the remaining existing samples
    while (sampleIdx < sampled.length) {
      newSamples += sampled(sampleIdx)
      sampleIdx += 1
    }
    new QuantileSummaries(compressThreshold, relativeError, newSamples.toArray, currentCount)
  }

  /**
   * 返回一个新的摘要，用于压缩摘要统计信息和头缓冲区。
   * 这实现了GK算法的COMPRESS功能。它不会修改对象。
   * Returns a new summary that compresses the summary statistics and the head buffer.
   *
   * This implements the COMPRESS function of the GK algorithm. It does not modify the object.
   *
   * @return a new summary object with compressed statistics
   */
  def compress(): QuantileSummaries = {
    // Inserts all the elements first
    val inserted = this.withHeadBufferInserted
    assert(inserted.headSampled.isEmpty)
    assert(inserted.count == count + headSampled.size)
    // 压缩
    val compressed =
      compressImmut(inserted.sampled, mergeThreshold = 2 * relativeError * inserted.count)
    new QuantileSummaries(compressThreshold, relativeError, compressed, inserted.count, true)
  }

  private def shallowCopy: QuantileSummaries = {
    new QuantileSummaries(compressThreshold, relativeError, sampled, count, compressed)
  }

  /**
   * Merges two (compressed) summaries together.
   *
   * Returns a new summary.
   */
  def merge(other: QuantileSummaries): QuantileSummaries = {
    require(headSampled.isEmpty, "Current buffer needs to be compressed before merge")
    require(other.headSampled.isEmpty, "Other buffer needs to be compressed before merge")
    if (other.count == 0) {
      this.shallowCopy
    } else if (count == 0) {
      other.shallowCopy
    } else {
      // Merge the two buffers.
      // The GK algorithm is a bit unclear about it, but we need to adjust the statistics during the
      // merging. The main idea is that samples that come from one side will suffer from the lack of
      // precision of the other.
      // As a concrete example, take two QuantileSummaries whose samples (value, g, delta) are:
      // `a = [(0, 1, 0), (20, 99, 0)]` and `b = [(10, 1, 0), (30, 49, 0)]`
      // This means `a` has 100 values, whose minimum is 0 and maximum is 20,
      // while `b` has 50 values, between 10 and 30.
      // The resulting samples of the merge will be:
      // a+b = [(0, 1, 0), (10, 1, ??), (20, 99, ??), (30, 49, 0)]
      // The values of `g` do not change, as they represent the minimum number of values between two
      // consecutive samples. The values of `delta` should be adjusted, however.
      // Take the case of the sample `10` from `b`. In the original stream, it could have appeared
      // right after `0` (as expressed by `g=1`) or right before `20`, so `delta=99+0-1=98`.
      // In the GK algorithm's style of working in terms of maximum bounds, one can observe that the
      // maximum additional uncertainty over samples comming from `b` is `max(g_a + delta_a) =
      // floor(2 * eps_a * n_a)`. Likewise, additional uncertainty over samples from `a` is
      // `floor(2 * eps_b * n_b)`.
      // Only samples that interleave the other side are affected. That means that samples from
      // one side that are lesser (or greater) than all samples from the other side are just copied
      // unmodifed.
      // If the merging instances have different `relativeError`, the resulting instance will cary
      // the largest one: `eps_ab = max(eps_a, eps_b)`.
      // The main invariant of the GK algorithm is kept:
      // `max(g_ab + delta_ab) <= floor(2 * eps_ab * (n_a + n_b))` since
      // `max(g_ab + delta_ab) <= floor(2 * eps_a * n_a) + floor(2 * eps_b * n_b)`
      // Finally, one can see how the `insert(x)` operation can be expressed as `merge([(x, 1, 0])`

      val mergedSampled = new ArrayBuffer[Stats]()
      val mergedRelativeError = math.max(relativeError, other.relativeError)
      val mergedCount = count + other.count
      val additionalSelfDelta = math.floor(2 * other.relativeError * other.count).toLong
      val additionalOtherDelta = math.floor(2 * relativeError * count).toLong

      // Do a merge of two sorted lists until one of the lists is fully consumed
      var selfIdx = 0
      var otherIdx = 0
      while (selfIdx < sampled.length && otherIdx < other.sampled.length) {
        val selfSample = sampled(selfIdx)
        val otherSample = other.sampled(otherIdx)

        // Detect next sample
        val (nextSample, additionalDelta) = if (selfSample.value < otherSample.value) {
          selfIdx += 1
          (selfSample, if (otherIdx > 0) additionalSelfDelta else 0)
        } else {
          otherIdx += 1
          (otherSample, if (selfIdx > 0) additionalOtherDelta else 0)
        }

        // Insert it
        mergedSampled += nextSample.copy(delta = nextSample.delta + additionalDelta)
      }

      // Copy the remaining samples from the other list
      // (by construction, at most one `while` loop will run)
      while (selfIdx < sampled.length) {
        mergedSampled += sampled(selfIdx)
        selfIdx += 1
      }
      while (otherIdx < other.sampled.length) {
        mergedSampled += other.sampled(otherIdx)
        otherIdx += 1
      }

      val comp = compressImmut(mergedSampled.toIndexedSeq, 2 * mergedRelativeError * mergedCount)
      new QuantileSummaries(other.compressThreshold, mergedRelativeError, comp, mergedCount, true)
    }
  }

  /**
   * 查询分位数
   * Runs a query for a given quantile.
   * The result follows the approximation guarantees detailed above.
   * The query can only be run on a compressed summary: you need to call compress() before using
   * it.
   *
   * @param quantile the target quantile
   * @return
   */
  def query(quantile: Double): Option[Double] = {
    require(quantile >= 0 && quantile <= 1.0, "quantile should be in the range [0.0, 1.0]")
    require(headSampled.isEmpty,
      "Cannot operate on an uncompressed summary, call compress() first")

    if (sampled.isEmpty) return None

    if (quantile <= relativeError) {
      return Some(sampled.head.value)
    }

    if (quantile >= 1 - relativeError) {
      return Some(sampled.last.value)
    }

    // Target rank, 分位数数据对应排名
    val rank = math.ceil(quantile * count).toLong
    // 下面的应该都是套用公式
    val targetError = sampled.map(s => s.delta + s.g).max / 2
    // Minimum rank at current sample
    var minRank = 0L
    var i = 0
    while (i < sampled.length - 1) {
      val curSample = sampled(i)
      minRank += curSample.g
      val maxRank = minRank + curSample.delta
      if (maxRank - targetError <= rank && rank <= minRank + targetError) {
        return Some(curSample.value)
      }
      i += 1
    }
    Some(sampled.last.value)
  }
}

object QuantileSummaries {
  // TODO(tjhunter) more tuning could be done one the constants here, but for now
  // the main cost of the algorithm is accessing the data in SQL.
  /**
   * The default value for the compression threshold.
   */
  val defaultCompressThreshold: Int = 10000

  /**
   * The size of the head buffer.
   */
  val defaultHeadSize: Int = 50000

  /**
   * The default value for the relative error (1%).
   * With this value, the best extreme percentiles that can be approximated are 1% and 99%.
   */
  val defaultRelativeError: Double = 0.01

  /**
   * Greenwald Khanna论文的统计数据。
   * Statistics from the Greenwald-Khanna paper.
   * @param value the sampled value 采样值
   * @param g the minimum rank jump from the previous value's minimum rank 从上一个值的最小rank跳出来的最小rank
   * @param delta the maximum span of the rank.
   */
  case class Stats(value: Double, g: Long, delta: Long)

  // 这应该是套用公式
  private def compressImmut(
      currentSamples: IndexedSeq[Stats],
      mergeThreshold: Double): Array[Stats] = {
    if (currentSamples.isEmpty) {
      return Array.empty[Stats]
    }
    val res = ListBuffer.empty[Stats]
    // Start for the last element, which is always part of the set.
    // The head contains the current new head, that may be merged with the current element.
    var head = currentSamples.last
    var i = currentSamples.size - 2
    // Do not compress the last element
    while (i >= 1) {
      // The current sample:
      val sample1 = currentSamples(i)
      // Do we need to compress?
      if (sample1.g + head.g + head.delta < mergeThreshold) {
        // Do not insert yet, just merge the current element into the head.
        head = head.copy(g = head.g + sample1.g)
      } else {
        // Prepend the current head, and keep the current sample as target for merging.
        res.prepend(head)
        head = sample1
      }
      i -= 1
    }
    res.prepend(head)
    // If necessary, add the minimum element:
    val currHead = currentSamples.head
    // don't add the minimum element if `currentSamples` has only one element (both `currHead` and
    // `head` point to the same element)
    if (currHead.value <= head.value && currentSamples.length > 1) {
      res.prepend(currentSamples.head)
    }
    res.toArray
  }
}
