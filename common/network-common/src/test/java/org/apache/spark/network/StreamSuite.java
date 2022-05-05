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

package org.apache.spark.network;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class StreamSuite {
  private static final String[] STREAMS = StreamTestHelper.STREAMS;
  private static StreamTestHelper testData;

  private static TransportContext context;
  private static TransportServer server;
  private static TransportClientFactory clientFactory;

  private static ByteBuffer createBuffer(int bufSize) {
    ByteBuffer buf = ByteBuffer.allocate(bufSize);
    for (int i = 0; i < bufSize; i ++) {
      buf.put((byte) i);
    }
    buf.flip();
    return buf;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    testData = new StreamTestHelper();

    final TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    final StreamManager streamManager = new StreamManager() {
      @Override
      public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ManagedBuffer openStream(String streamId) {
        return testData.openStream(conf, streamId);
      }
    };
    RpcHandler handler = new RpcHandler() {
      @Override
      public void receive(
          TransportClient client,
          ByteBuffer message,
          RpcResponseCallback callback) {
        throw new UnsupportedOperationException();
      }

      @Override
      public StreamManager getStreamManager() {
        return streamManager;
      }
    };
    context = new TransportContext(conf, handler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }

  @AfterClass
  public static void tearDown() {
    server.close();
    clientFactory.close();
    testData.cleanup();
    context.close();
  }

  @Test
  public void testZeroLengthStream() throws Throwable {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      StreamTask task = new StreamTask(client, "emptyBuffer", TimeUnit.SECONDS.toMillis(50));
      task.run();
      task.check();
    } finally {
      client.close();
    }
  }

  /**
   * 实现文件传输
   * @throws Throwable
   */
  @Test
  public void testSingleStream() throws Throwable {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      StreamTask task = new StreamTask(client, "largeBuffer", TimeUnit.SECONDS.toMillis(5));
      task.run();
      task.check();
    } finally {
      client.close();
    }
  }

  /**
   * 这个是文件下载的场景：实现文件的传输，我这方面的知识比较匮乏，暂时不知道怎么实现的，这个就要用在shuffle中
   * [[org.apache.spark.network.server.TransportRequestHandler#processStreamRequest]]
   * [[org.apache.spark.network.client.TransportResponseHandler#handle]]处理StreamResponse的请求
   * 返回的message的body是FileSegmentManagedBuffer，convertToNetty方法中返回DefaultFileRegion
   * MessageEncoder返回的MessageWithHeader，body传的就是DefaultFileRegion。MessageEncoder实现了FileRegion，似乎可以实现零拷贝，代码比较高深，看不太懂
   *
   * 通过打断点似乎看明白了文件下载的功能：
   *    当服务端返回StreamResponse时，客户端经过TransportFrameDecoder处理后客户端第一次只会处理读取streamId, byteCount而不会读取body(文件内容)
   *    处理StreamResponse时会设置TransportFrameDecoder的interceptor
   *    第二次到TransportFrameDecoder时会直接不停的调用interceptor处理文件的内容，代码在channelRead方法中
   *    interceptor读取完文件会把interceptor置为null,TransportFrameDecoder下次会decodeNext解析Frame
   *
   *    这里TransportFrameDecoder是怎么实现返回的第一个StreamResponse不包含body呢？StreamResponse的isBodyInFrame是false，服务端编码时并不会输出body
   *    encode时isBodyInFrame = false，body实际是有的，frameLength的长度也不包含body的长度，
   *    所以客户端第一次能解析出frame并设置interceptor，之后循环处理。
   *    当interceptor读取到的字节达到期望的值时就会停止同时把interceptor置空。之后解析下一个Frame
   *    spark的代码是真nb
   *
   * 文件上传的功能在哪？看org.apache.spark.network.protocol.Message.Type#decode(io.netty.buffer.ByteBuf)
   * 看到有个UploadStream类型，为啥下载有StreamRequest和StreamResponse，上传就UploadStream？
   * 下载是客户端先请求服务端，服务端返回文件数据
   * 上传直接就是客户端请求服务端
   */
  @Test
  public void testFileStream() throws Throwable {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      StreamTask task = new StreamTask(client, "file", TimeUnit.SECONDS.toMillis(50));
      System.out.println(testData.testFile);
      task.run();
      System.out.println(testData.testFile);
      task.check();
    } finally {
      client.close();
    }
  }

  @Test
  public void testMultipleStreams() throws Throwable {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      for (int i = 0; i < 20; i++) {
        StreamTask task = new StreamTask(client, STREAMS[i % STREAMS.length],
          TimeUnit.SECONDS.toMillis(5));
        task.run();
        task.check();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testConcurrentStreams() throws Throwable {
    ExecutorService executor = Executors.newFixedThreadPool(20);
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());

    try {
      List<StreamTask> tasks = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        StreamTask task = new StreamTask(client, STREAMS[i % STREAMS.length],
          TimeUnit.SECONDS.toMillis(20));
        tasks.add(task);
        executor.submit(task);
      }

      executor.shutdown();
      assertTrue("Timed out waiting for tasks.", executor.awaitTermination(30, TimeUnit.SECONDS));
      for (StreamTask task : tasks) {
        task.check();
      }
    } finally {
      executor.shutdownNow();
      client.close();
    }
  }

  private static class StreamTask implements Runnable {

    private final TransportClient client;
    private final String streamId;
    private final long timeoutMs;
    private Throwable error;

    StreamTask(TransportClient client, String streamId, long timeoutMs) {
      this.client = client;
      this.streamId = streamId;
      this.timeoutMs = timeoutMs;
    }

    @Override
    public void run() {
      ByteBuffer srcBuffer = null;
      OutputStream out = null;
      File outFile = null;
      try {
        ByteArrayOutputStream baos = null;

        switch (streamId) {
          case "largeBuffer":
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = testData.largeBuffer;
            break;
          case "smallBuffer":
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = testData.smallBuffer;
            break;
          case "file":
            outFile = File.createTempFile("data", ".tmp", testData.tempDir);
            System.out.println(outFile);
            out = new FileOutputStream(outFile);
            break;
          case "emptyBuffer":
            baos = new ByteArrayOutputStream();
            out = baos;
            srcBuffer = testData.emptyBuffer;
            break;
          default:
            throw new IllegalArgumentException(streamId);
        }

        TestCallback callback = new TestCallback(out);
        client.stream(streamId, callback);
        callback.waitForCompletion(timeoutMs);

        if (srcBuffer == null) {
          assertTrue("File stream did not match.", Files.equal(testData.testFile, outFile));
        } else {
          ByteBuffer base;
          synchronized (srcBuffer) {
            base = srcBuffer.duplicate();
          }
          byte[] result = baos.toByteArray();
          byte[] expected = new byte[base.remaining()];
          base.get(expected);
          assertEquals(expected.length, result.length);
          assertTrue("buffers don't match", Arrays.equals(expected, result));
        }
      } catch (Throwable t) {
        error = t;
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (Exception e) {
            // ignore.
          }
        }
        if (outFile != null) {
          outFile.delete();
        }
      }
    }

    public void check() throws Throwable {
      if (error != null) {
        throw error;
      }
    }
  }

  static class TestCallback implements StreamCallback {

    private final OutputStream out;
    public volatile boolean completed;
    public volatile Throwable error;

    TestCallback(OutputStream out) {
      this.out = out;
      this.completed = false;
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      byte[] tmp = new byte[buf.remaining()];
      buf.get(tmp);
      out.write(tmp);
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      out.close();
      synchronized (this) {
        completed = true;
        notifyAll();
      }
    }

    @Override
    public void onFailure(String streamId, Throwable cause) {
      error = cause;
      synchronized (this) {
        completed = true;
        notifyAll();
      }
    }

    void waitForCompletion(long timeoutMs) {
      long now = System.currentTimeMillis();
      long deadline = now + timeoutMs;
      synchronized (this) {
        while (!completed && now < deadline) {
          try {
            wait(deadline - now);
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
          now = System.currentTimeMillis();
        }
      }
      assertTrue("Timed out waiting for stream.", completed);
      assertNull(error);
    }
  }

}
