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

package org.apache.spark.streaming.scheduler

import java.util.concurrent.TimeUnit

import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Checkpoint, CheckpointWriter, StreamingConf, Time}
import org.apache.spark.streaming.api.python.PythonDStream
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, EventLoop, ManualClock, Utils}

/** Event classes for JobGenerator */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(
    time: Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
private[scheduler] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  private val ssc = jobScheduler.ssc
  private val conf = ssc.conf
  private val graph = ssc.graph

  val clock = {
    val clockClass = ssc.sc.conf.get(
      "spark.streaming.clock", "org.apache.spark.util.SystemClock")
    try {
      Utils.classForName[Clock](clockClass).getConstructor().newInstance()
    } catch {
      case e: ClassNotFoundException if clockClass.startsWith("org.apache.spark.streaming") =>
        val newClockClass = clockClass.replace("org.apache.spark.streaming", "org.apache.spark")
        Utils.classForName[Clock](newClockClass).getConstructor().newInstance()
    }
  }

  /**
   * timer负责定期的发送GenerateJobs的事件, start方法中会调用timer.start
   * timer中有个线程每隔batchDuration时间, 调用一下传入的callback, 也就是eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
   * 每隔batchDuration时间, 会调用本类的[[processEvent(event)]], 进而调用[[generateJobs(time)]], 进而调用[[graph.generateJobs(time)]]
   * 每个周期生成jobs后, 调用[[jobScheduler.submitJobSet]], 会回调我们传入的callback
   * generateJobs(time)会调用[[org.apache.spark.streaming.dstream.ForEachDStream.generateJob()]]生成job,
   * job实际调用的就是我们传入的foreachRDD的foreachFunc函数, 每个批次的rdd就是DirectKafkaInputDStream的compute方法生成的rdd
   * DirectKafkaInputDStream每个批次生成的rdd就是KafkaRDD, KafkaRDD的compute方法(每个分区)是读取上次的offset到最新时间的offset
   * 消费当然会考虑我们配置的参数(spark.streaming.kafka.maxRatePerPartition), 逻辑就在第一行: val untilOffsets = clamp(latestOffsets()) 中
   * DirectKafkaInputDStream生成的KafkaRDD每个分区是直接连接Kafka的分区按照offset读取的数据, 所以不必但系调用rdd.isempty等是否影响之后的调用会少数据
   *    直接连接Kafka的分区, 也就不像其他ReceiverInputDStream需要额外的不间断流不停的读数据到block, 需要单独的读取数据的核数
   *    spark streaming和flink的模式不一样, 基于微批实现的, 操作的rdd和离线没啥区别, 就是每个批次操作的rdd数据不一样而已
   */
  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

  // This is marked lazy so that this is initialized after checkpoint duration has been set
  // in the context and the generator has been started.
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null

  private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  // eventLoop is created when generator starts.
  // This not being null means the scheduler has been started and not stopped
  private var eventLoop: EventLoop[JobGeneratorEvent] = null

  // last batch whose completion,checkpointing and metadata cleanup has been completed
  @volatile private[streaming] var lastProcessedBatch: Time = null

  /** Start generation of jobs */
  def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started

    // Call checkpointWriter here to initialize it before eventLoop uses it to avoid a deadlock.
    // See SPARK-10125
    checkpointWriter

    // 时间循环, eventLoop.post() 把event添加到队列
    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()

    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }

  /**
   * Stop generation of jobs. processReceivedData = true makes this wait until jobs
   * of current ongoing time interval has been generated, processed and corresponding
   * checkpoints written.
   */
  def stop(processReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // generator has already been stopped

    if (processReceivedData) {
      logInfo("Stopping JobGenerator gracefully")
      val timeWhenStopStarted = System.nanoTime()
      val stopTimeoutMs = conf.getTimeAsMs(
        StreamingConf.GRACEFUL_STOP_TIMEOUT.key, s"${10 * ssc.graph.batchDuration.milliseconds}ms")
      val pollTime = 100

      // To prevent graceful stop to get stuck permanently
      def hasTimedOut: Boolean = {
        val diff = TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - timeWhenStopStarted))
        val timedOut = diff > stopTimeoutMs
        if (timedOut) {
          logWarning("Timed out while stopping the job generator (timeout = " + stopTimeoutMs + ")")
        }
        timedOut
      }

      // Wait until all the received blocks in the network input tracker has
      // been consumed by network input DStreams, and jobs have been generated with them
      logInfo("Waiting for all received blocks to be consumed for job generation")
      while(!hasTimedOut && jobScheduler.receiverTracker.hasUnallocatedBlocks) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for all received blocks to be consumed for job generation")

      // Stop generating jobs
      val stopTime = timer.stop(interruptTimer = false)
      logInfo("Stopped generation timer")

      // Wait for the jobs to complete and checkpoints to be written
      def haveAllBatchesBeenProcessed: Boolean = {
        lastProcessedBatch != null && lastProcessedBatch.milliseconds == stopTime
      }
      logInfo("Waiting for jobs to be processed and checkpoints to be written")
      while (!hasTimedOut && !haveAllBatchesBeenProcessed) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for jobs to be processed and checkpoints to be written")
      graph.stop()
    } else {
      logInfo("Stopping JobGenerator immediately")
      // Stop timer and graph immediately, ignore unprocessed data and pending jobs
      timer.stop(true)
      graph.stop()
    }

    // First stop the event loop, then stop the checkpoint writer; see SPARK-14701
    eventLoop.stop()
    if (shouldCheckpoint) checkpointWriter.stop()
    logInfo("Stopped JobGenerator")
  }

  /**
   * Callback called when a batch has been completely processed.
   */
  def onBatchCompletion(time: Time): Unit = {
    eventLoop.post(ClearMetadata(time))
  }

  /**
   * Callback called when the checkpoint of a batch has been written.
   */
  def onCheckpointCompletion(time: Time, clearCheckpointDataLater: Boolean): Unit = {
    if (clearCheckpointDataLater) {
      eventLoop.post(ClearCheckpointData(time))
    }
  }

  /** Processes all events */
  private def processEvent(event: JobGeneratorEvent): Unit = {
    logDebug("Got event " + event)
    event match {
      // 生成jobs
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }

  /** Starts the generator for the first time */
  private def startFirstTime(): Unit = {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("Started JobGenerator at " + startTime)
  }

  /** Restarts the generator based on the information in checkpoint */
  private def restart(): Unit = {
    // If manual clock is being used for testing, then
    // either set the manual clock to the last checkpointed time,
    // or if the property is defined set it to that time
    if (clock.isInstanceOf[ManualClock]) {
      val lastTime = ssc.initialCheckpoint.checkpointTime.milliseconds
      val jumpTime = ssc.sc.conf.get(StreamingConf.MANUAL_CLOCK_JUMP)
      clock.asInstanceOf[ManualClock].setTime(lastTime + jumpTime)
    }

    val batchDuration = ssc.graph.batchDuration

    // Batches when the master was down, that is,
    // between the checkpoint and current restart time
    val checkpointTime = ssc.initialCheckpoint.checkpointTime
    val restartTime = new Time(timer.getRestartTime(graph.zeroTime.milliseconds))
    val downTimes = checkpointTime.until(restartTime, batchDuration)
    logInfo("Batches during down time (" + downTimes.size + " batches): "
      + downTimes.mkString(", "))

    // Batches that were unprocessed before failure
    val pendingTimes = ssc.initialCheckpoint.pendingTimes.sorted(Time.ordering)
    logInfo("Batches pending processing (" + pendingTimes.length + " batches): " +
      pendingTimes.mkString(", "))
    // Reschedule jobs for these times
    val timesToReschedule = (pendingTimes ++ downTimes).filter { _ < restartTime }
      .distinct.sorted(Time.ordering)
    logInfo("Batches to reschedule (" + timesToReschedule.length + " batches): " +
      timesToReschedule.mkString(", "))
    timesToReschedule.foreach { time =>
      // Allocate the related blocks when recovering from failure, because some blocks that were
      // added but not allocated, are dangling in the queue after recovering, we have to allocate
      // those blocks to the next batch, which is the batch they were supposed to go.
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      jobScheduler.submitJobSet(JobSet(time, graph.generateJobs(time)))
    }

    // Restart the timer
    timer.start(restartTime.milliseconds)
    logInfo("Restarted JobGenerator at " + restartTime)
  }

  /** Generate jobs and perform checkpointing for the given `time`.  */
  private def generateJobs(time: Time): Unit = {
    // Checkpoint all RDDs marked for checkpointing to ensure their lineages are
    // truncated periodically. Otherwise, we may run into stack overflows (SPARK-6847).
    ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
    Try {
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
        PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }

  /** Clear DStream metadata for the given `time`. */
  private def clearMetadata(time: Time): Unit = {
    ssc.graph.clearMetadata(time)

    // If checkpointing is enabled, then checkpoint,
    // else mark batch to be fully processed
    if (shouldCheckpoint) {
      eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = true))
    } else {
      // If checkpointing is not enabled, then delete metadata information about
      // received blocks (block data not saved in any case). Otherwise, wait for
      // checkpointing of this batch to complete.
      val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
      jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
      jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
      markBatchFullyProcessed(time)
    }
  }

  /** Clear DStream checkpoint data for the given `time`. */
  private def clearCheckpointData(time: Time): Unit = {
    ssc.graph.clearCheckpointData(time)

    // All the checkpoint information about which batches have been processed, etc have
    // been saved to checkpoints, so its safe to delete block metadata and data WAL files
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
    jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
    markBatchFullyProcessed(time)
  }

  /** Perform checkpoint for the given `time`. */
  private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean): Unit = {
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
    } else if (clearCheckpointDataLater) {
      markBatchFullyProcessed(time)
    }
  }

  private def markBatchFullyProcessed(time: Time): Unit = {
    lastProcessedBatch = time
  }
}
