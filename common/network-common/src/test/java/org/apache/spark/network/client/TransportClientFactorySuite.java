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

package org.apache.spark.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

public class TransportClientFactorySuite {
  private TransportConf conf;
  private TransportContext context;
  private TransportServer server1;
  private TransportServer server2;

  @Before
  public void setUp() {
    // 创建server的步骤
    conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    // RpcHandler作为仅仅作为客户端的TransportContext，receive方法会抛出异常
    RpcHandler rpcHandler = new NoOpRpcHandler();
    context = new TransportContext(conf, rpcHandler);
    // 这俩server都不能返回数据
    server1 = context.createServer();
    server2 = context.createServer();
  }

  @After
  public void tearDown() {
    JavaUtils.closeQuietly(server1);
    JavaUtils.closeQuietly(server2);
    JavaUtils.closeQuietly(context);
  }

  /**
   * 测试client重用，
   * 批量创建连接单个server的多个client，测试最大连接数，如果concurrent为true，会并行创建client
   * org.apache.spark.network.util.TransportConf.numConnectionsPerPeer() 默认是1，
   * numConnectionsPerPeer.numConnectionsPerPeer = TransportConf.numConnectionsPerPeer()
   * 也就是TransportClientFactory默认会共用一个client
   * TransportClientFactory中维护一个clientPool，大小为numConnectionsPerPeer，
   * TransportClientFactory每次createClient时从clientPool.clients数组中随机返回一个
   *
   * Request a bunch of clients to a single server to test
   * we create up to maxConnections of clients.
   *
   * If concurrent is true, create multiple threads to create clients in parallel.
   */
  private void testClientReuse(int maxConnections, boolean concurrent)
    throws IOException, InterruptedException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("spark.shuffle.io.numConnectionsPerPeer", Integer.toString(maxConnections));
    TransportConf conf = new TransportConf("shuffle", new MapConfigProvider(configMap));

    RpcHandler rpcHandler = new NoOpRpcHandler();
    try (TransportContext context = new TransportContext(conf, rpcHandler)) {
      TransportClientFactory factory = context.createClientFactory();
      Set<TransportClient> clients = Collections.synchronizedSet(
              new HashSet<>());

      AtomicInteger failed = new AtomicInteger();
      Thread[] attempts = new Thread[maxConnections * 10];

      // Launch a bunch of threads to create new clients.
      for (int i = 0; i < attempts.length; i++) {
        attempts[i] = new Thread(() -> {
          try {
            TransportClient client =
                factory.createClient(TestUtils.getLocalHost(), server1.getPort());
            assertTrue(client.isActive());
            clients.add(client);
          } catch (IOException e) {
            failed.incrementAndGet();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

        // 是否并发创建
        if (concurrent) {
          attempts[i].start();
        } else {
          attempts[i].run();
        }
      }

      // Wait until all the threads complete.
      for (Thread attempt : attempts) {
        attempt.join();
      }

      Assert.assertEquals(0, failed.get());
      Assert.assertTrue(clients.size() <= maxConnections);

      for (TransportClient client : clients) {
        client.close();
      }

      factory.close();
    }
  }

  @Test
  public void reuseClientsUpToConfigVariable() throws Exception {
    testClientReuse(1, false);
    testClientReuse(2, false);
    testClientReuse(3, false);
    testClientReuse(4, false);
  }

  @Test
  public void reuseClientsUpToConfigVariableConcurrent() throws Exception {
    testClientReuse(1, true);
    testClientReuse(2, true);
    testClientReuse(3, true);
    testClientReuse(4, true);
  }

  /**
   * 同一个factory，可以创建不同server的client
   *
   */
  @Test
  public void returnDifferentClientsForDifferentServers() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    assertNotSame(c1, c2);
    factory.close();
  }

  @Test
  public void neverReturnInactiveClients() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    // 我说咋Inactive了，这里主动关闭了
    c1.close();

    long start = System.currentTimeMillis();
    while (c1.isActive() && (System.currentTimeMillis() - start) < 3000) {
      Thread.sleep(10);
    }
    assertFalse(c1.isActive());

    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    assertNotSame(c1, c2);
    assertTrue(c2.isActive());
    factory.close();
  }

  @Test
  public void closeBlockClientsWithFactory() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    factory.close();
    assertFalse(c1.isActive());
    assertFalse(c2.isActive());
  }

  @Test
  public void closeIdleConnectionForRequestTimeOut() throws IOException, InterruptedException {
    TransportConf conf = new TransportConf("shuffle", new ConfigProvider() {

      @Override
      public String get(String name) {
        if ("spark.shuffle.io.connectionTimeout".equals(name)) {
          // We should make sure there is enough time for us to observe the channel is active
          return "1s";
        }
        String value = System.getProperty(name);
        if (value == null) {
          throw new NoSuchElementException(name);
        }
        return value;
      }

      @Override
      public Iterable<Map.Entry<String, String>> getAll() {
        throw new UnsupportedOperationException();
      }
    });

    /**
     * 这里把closeIdleConnections设置成了true，默认是false，所以会关闭限制的连接
     * 搜了一下spark中设置closeIdleConnections为true的几处都是下载文件相关的，不是长期的客户端
     *
     * 一个TransportClientFactory可以创建多个不同服务的Client，共用的workerGroup，我还以为一个Client需要从头建workerGroup，这样可以同时建很多客户端把
     *
     * client断开连接后怎么办，从方法上看send不会报错，sendRpcSync/askSync可以监控到，自己使用时怎么办呢
     *    使用spark的rpcEndpointRef测试了一下，发现断开连接(catch住异常防止程序停止)后仍能发消息，发现Outbox不是一个对象了
     *    出错时对应服务端的Outbox会被删除，重新创建[[org.apache.spark.rpc.netty.NettyRpcHandler#channelInactive]]
     *    nettyEnv断开连接后的处理，删除发件箱，下次发送时可以新建发件箱，能重新发送消息
     *
     * netty和spark的network太复杂了，要是想自己基于netty实现network通信真是太难了
     */
    try (TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
         TransportClientFactory factory = context.createClientFactory()) {
      TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
      assertTrue(c1.isActive());
      // 1s 就空闲超时， spark默认是120s
      System.out.println(System.currentTimeMillis());
      long expiredTime = System.currentTimeMillis() + 10000; // 10 seconds
      while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
        Thread.sleep(10);
      }
      System.out.println(expiredTime);
      System.out.println(System.currentTimeMillis());

      /**
       * 关闭后writeAndFlush方法不会报错
       * sendRpcSync方法中添加了回调函数才能监控到
       * 打了下断点，发现write只是把task提交到循环，不添加回调函数的话，Channel close时writeAndFlush也不会报错
       */
      c1.getChannel().writeAndFlush(ByteBuffer.allocate(10));
      System.out.println("ok");
      c1.sendRpcSync(ByteBuffer.allocate(10), 2000);

      assertFalse(c1.isActive());
    }
  }

  @Test(expected = IOException.class)
  public void closeFactoryBeforeCreateClient() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    factory.close();
    factory.createClient(TestUtils.getLocalHost(), server1.getPort());
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void fastFailConnectionInTimeWindow() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportServer server = context.createServer();
    int unreachablePort = server.getPort();
    server.close();
    try {
      factory.createClient(TestUtils.getLocalHost(), unreachablePort, true);
    } catch (Exception e) {
      assert(e instanceof IOException);
    }
    expectedException.expect(IOException.class);
    expectedException.expectMessage("fail this connection directly");
    factory.createClient(TestUtils.getLocalHost(), unreachablePort, true);
    expectedException = ExpectedException.none();
  }
}
