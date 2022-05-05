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

package org.apache.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 创建心跳线程，该线程将每隔一段时间调用指定的reportHeartbeat函数。
 * Creates a heartbeat thread which will call the specified reportHeartbeat function at
 * intervals of intervalMs.
 *
 * @param reportHeartbeat the heartbeat reporting function to call. 定时执行的函数
 * @param name the thread name for the heartbeater. 心跳线程的名字
 * @param intervalMs the interval between heartbeats. 定时间隔
 */
private[spark] class Heartbeater(
    reportHeartbeat: () => Unit,
    name: String,
    intervalMs: Long) extends Logging {
  // Executor for the heartbeat task
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor(name)

  /** Schedules a task to report a heartbeat. */
  def start(): Unit = {
    // 等待一段随机间隔，这样心跳就不会同步
    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      // logUncaughtExceptions：包装函数，有时长时打印日志但不会抓住拦截异常，会正常向上抛出，主要用于线程中执行的任务
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartbeat())
    }
    // scheduleAtFixedRate：固定速率调度，本身任务执行时长不影响
    // scheduleWithFixedDelay：固定延时调度，是在每个任务结束后延时一定时长执行任务
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }

  /** Stops the heartbeat thread. */
  def stop(): Unit = {
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
  }
}
