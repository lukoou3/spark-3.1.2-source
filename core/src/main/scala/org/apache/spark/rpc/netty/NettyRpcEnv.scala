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
package org.apache.spark.rpc.netty

import java.io._
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.nio.channels.{Pipe, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EXECUTOR_ID
import org.apache.spark.internal.config.Network._
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server._
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance, SerializationStream}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, ThreadUtils, Utils}

private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    securityManager: SecurityManager,
    numUsableCores: Int) extends RpcEnv(conf) with Logging {
  val role = conf.get(EXECUTOR_ID).map { id =>
    if (id == SparkContext.DRIVER_IDENTIFIER) "driver" else "executor"
  }

  private[netty] val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set(RPC_IO_NUM_CONNECTIONS_PER_PEER, 1),
    "rpc",
    conf.get(RPC_IO_THREADS).getOrElse(numUsableCores),
    role)

  private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)

  private val streamManager = new NettyStreamManager(this)

  // server和client都是使用的这个transportContext, rpc的处理逻辑在NettyRpcHandler中，使用dispatcher实现rpc和network模块的解耦
  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    if (securityManager.isAuthenticationEnabled()) {
      java.util.Arrays.asList(new AuthClientBootstrap(transportConf,
        securityManager.getSaslUser(), securityManager))
    } else {
      java.util.Collections.emptyList[TransportClientBootstrap]
    }
  }

  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  /**
   * A separate client factory for file downloads. This avoids using the same RPC handler as
   * the main RPC context, so that events caused by these clients are kept isolated from the
   * main RPC traffic.
   *
   * It also allows for different configuration of certain properties, such as the number of
   * connections per peer.
   */
  @volatile private var fileDownloadFactory: TransportClientFactory = _

  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.get(RPC_CONNECT_THREADS))

  /**
   * 实际的服务端。不是可以注册多个RpcEndpoint吗，咋这只有一个server。
   * RpcEndpoint就是一个逻辑的服务端点，使用name进行注册，服务端只会监听一个端口，多个RpcEndpoint用来实现分模块处理不同业务的消息
   */
  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
   * 每个地址的发信箱map，发消息时仅仅把消息放到Outbox中以实现非阻塞的send方法
   * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
   */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
   * Remove the address's Outbox and stop it.
   */
  private[netty] def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  /**
   * 初始化方法
   *
   */
  def startServer(bindAddress: String, port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
        java.util.Arrays.asList(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList()
      }
    // 实际的server:TransportServer
    server = transportContext.createServer(bindAddress, port, bootstraps)
    // 注册RpcEndpointVerifier，验证RpcEndpoint是否存在。远程检索RpcEndpointRef时会给这个发消息验证服务端RpcEndpoint是否存在。
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    // 使用name把endpoint注册到dispatcher
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  /**
   * 把消息放到Outbox，实现发消息
   * @param receiver
   * @param message
   */
  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        // 看看缓存中是否有这个地址的发信箱
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        // 向Outbox发消息
        targetOutbox.send(message)
      }
    }
  }

  /**
   * 只发消息：分为本地和远程
   *
   * @param message
   */
  private[netty] def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    // 这个当前进程连接Endpoint返回的EndpointRef是local吗？是local，同一个进程另外注册一个env，然后返回的ref发消息不会走本地。address不一样
    if (remoteAddr == address) {
      // Message to a local RPC endpoint.
      try {
        // 直接放到自己的收件箱了，省去了网络传输
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => logDebug(e.getMessage)
      }
    } else {
      // Message to a remote RPC endpoint.
      // 把OneWayOutboxMessage放到Outbox中
      postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
    }
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  private[netty] def askAbortable[T: ClassTag](
      message: RequestMessage, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address
    var rpcMsg: Option[RpcOutboxMessage] = None

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        e match {
          case e : RpcEnvStoppedException => logDebug(s"Ignored failure: $e")
          case _ => logWarning(s"Ignored failure: $e")
        }
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }

    def onAbort(t: Throwable): Unit = {
      onFailure(t)
      rpcMsg.foreach(_.onAbort())
    }

    try {
      if (remoteAddr == address) {
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message, p)
      } else {
        /**
         * 这里传入的onFailure和onSuccess回调的调用实现promise的success和failure，这里的promise是scala的promise
         *
         */
        val rpcMessage = RpcOutboxMessage(message.serialize(this),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        rpcMsg = Option(rpcMessage)
        postToOutbox(message.receiver, rpcMessage)
        promise.future.failed.foreach {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
        override def run(): Unit = {
          val remoteRecAddr = if (remoteAddr == null) {
            Try {
              message.receiver.client.getChannel.remoteAddress()
            }.toOption.orNull
          } else {
            remoteAddr
          }
          onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteRecAddr} " +
            s"in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
      promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }

    new AbortableRpcFuture[T](
      promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread),
      onAbort)
  }

  private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  /**
   * Returns [[SerializationStream]] that forwards the serialized bytes to `out`.
   */
  private[netty] def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
    if (fileDownloadFactory != null) {
      fileDownloadFactory.close()
    }
    if (transportContext != null) {
      transportContext.close()
    }
  }

  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  override def fileServer: RpcEnvFileServer = streamManager

  override def openChannel(uri: String): ReadableByteChannel = {
    val parsedUri = new URI(uri)
    require(parsedUri.getHost() != null, "Host name must be defined.")
    require(parsedUri.getPort() > 0, "Port must be defined.")
    require(parsedUri.getPath() != null && parsedUri.getPath().nonEmpty, "Path must be defined.")

    val pipe = Pipe.open()
    val source = new FileDownloadChannel(pipe.source())
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      val client = downloadClient(parsedUri.getHost(), parsedUri.getPort())
      val callback = new FileDownloadCallback(pipe.sink(), source, client)
      client.stream(parsedUri.getPath(), callback)
    })(catchBlock = {
      pipe.sink().close()
      source.close()
    })

    source
  }

  private def downloadClient(host: String, port: Int): TransportClient = {
    if (fileDownloadFactory == null) synchronized {
      if (fileDownloadFactory == null) {
        val module = "files"
        val prefix = "spark.rpc.io."
        val clone = conf.clone()

        // Copy any RPC configuration that is not overridden in the spark.files namespace.
        conf.getAll.foreach { case (key, value) =>
          if (key.startsWith(prefix)) {
            val opt = key.substring(prefix.length())
            clone.setIfMissing(s"spark.$module.io.$opt", value)
          }
        }

        val ioThreads = clone.getInt("spark.files.io.threads", 1)
        val downloadConf = SparkTransportConf.fromSparkConf(clone, module, ioThreads)
        val downloadContext = new TransportContext(downloadConf, new NoOpRpcHandler(), true)
        fileDownloadFactory = downloadContext.createClientFactory(createClientBootstraps())
      }
    }
    fileDownloadFactory.createClient(host, port)
  }

  private class FileDownloadChannel(source: Pipe.SourceChannel) extends ReadableByteChannel {

    @volatile private var error: Throwable = _

    def setError(e: Throwable): Unit = {
      // This setError callback is invoked by internal RPC threads in order to propagate remote
      // exceptions to application-level threads which are reading from this channel. When an
      // RPC error occurs, the RPC system will call setError() and then will close the
      // Pipe.SinkChannel corresponding to the other end of the `source` pipe. Closing of the pipe
      // sink will cause `source.read()` operations to return EOF, unblocking the application-level
      // reading thread. Thus there is no need to actually call `source.close()` here in the
      // onError() callback and, in fact, calling it here would be dangerous because the close()
      // would be asynchronous with respect to the read() call and could trigger race-conditions
      // that lead to data corruption. See the PR for SPARK-22982 for more details on this topic.
      error = e
    }

    override def read(dst: ByteBuffer): Int = {
      Try(source.read(dst)) match {
        // See the documentation above in setError(): if an RPC error has occurred then setError()
        // will be called to propagate the RPC error and then `source`'s corresponding
        // Pipe.SinkChannel will be closed, unblocking this read. In that case, we want to propagate
        // the remote RPC exception (and not any exceptions triggered by the pipe close, such as
        // ChannelClosedException), hence this `error != null` check:
        case _ if error != null => throw error
        case Success(bytesRead) => bytesRead
        case Failure(readErr) => throw readErr
      }
    }

    override def close(): Unit = source.close()

    override def isOpen(): Boolean = source.isOpen()

  }

  private class FileDownloadCallback(
      sink: WritableByteChannel,
      source: FileDownloadChannel,
      client: TransportClient) extends StreamCallback {

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      while (buf.remaining() > 0) {
        sink.write(buf)
      }
    }

    override def onComplete(streamId: String): Unit = {
      sink.close()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      logDebug(s"Error downloading stream $streamId.", cause)
      source.setError(cause)
      sink.close()
    }

  }
}

private[netty] object NettyRpcEnv extends Logging {
  /**
   * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
   * Use `currentEnv` to wrap the deserialization codes. E.g.,
   *
   * {{{
   *   NettyRpcEnv.currentEnv.withValue(this) {
   *     your deserialization codes
   *   }
   * }}}
   */
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
   * Similar to `currentEnv`, this variable references the client instance associated with an
   * RPC, in case it's needed to find out the remote address during deserialization.
   */
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)

}

private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {

  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    //在多线程中使用JavaSerializerInstance是安全的。然而，如果我们计划在将来支持KryoSerializer，我们必须使用ThreadLocal来存储SerializerInstance
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        config.securityManager, config.numUsableCores)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}

/**
 * RpcEndpointRef的NettyRpcEnv版本。
 * 该类的行为因其创建位置而异。在拥有RpcEndpoint的节点上，它是围绕RpcEndpointAddress实例的简单包装。
 * 在接收序列化版本引用的其他机器上，行为会发生变化。实例将跟踪发送引用的TransportClient，以便通过客户端连接发送到端点的消息，而不需要打开新连接。
 * rpcadAddress可以为空；这意味着ref只能通过客户端连接使用，因为承载端点的进程没有监听传入的连接。这些ref不应与第三方共享，因为它们将无法向端点发送消息。
 *
 * The NettyRpcEnv version of RpcEndpointRef.
 *
 * This class behaves differently depending on where it's created. On the node that "owns" the
 * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
 *
 * On other machines that receive a serialized version of the reference, the behavior changes. The
 * instance will keep track of the TransportClient that sent the reference, so that messages
 * to the endpoint are sent over the client connection, instead of needing a new connection to
 * be opened.
 *
 * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
 * a client connection, since the process hosting the endpoint is not listening for incoming
 * connections. These refs should not be shared with 3rd parties, since they will not be able to
 * send messages to the endpoint.
 *
 * @param conf Spark configuration.
 * @param endpointAddress The address where the endpoint is listening.
 * @param nettyEnv The RpcEnv associated with this ref.
 */
private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    private val endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef(conf) {

  @transient @volatile var client: TransportClient = _

  override def address: RpcAddress =
    if (endpointAddress.rpcAddress != null) endpointAddress.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = endpointAddress.name

  override def askAbortable[T: ClassTag](
      message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    nettyEnv.askAbortable(new RequestMessage(nettyEnv.address, this, message), timeout)
  }

  // 发消息，返回Future
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  /**
   * 只发消息，对方不需要返回
   * 不论是是send和ask都是调用的nettyEnv的send和ask/askAbortable
   * 我们这个对象是RpcEndpointRef
   * 发送消息的顺序：
   *    RpcEndpointRef => nettyEnv => Outbox => TransportClient
   *    RpcEndpointRef.send(message: Any) => nettyEnv.send(message: RequestMessage) => postToOutbox(message: OutboxMessage) => Outbox.send(message)
   *         postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage):  OutboxMessage的类型：OneWayOutboxMessage/RpcOutboxMessage
   *         Outbox.send(message): client.send(content) 和 requestId = client.sendRpc(content, this)
   *
   * 可以看到发送的消息都是RequestMessage，发送消息时似乎没经过dispatcher
   * OneWayOutboxMessage就是直接发送消息，RpcOutboxMessage中会额外携带回调函数实现返回数据
   * 这里client两种发送数据的方式：
   *    send方法直接调用channel.writeAndFlush(new OneWayMessage)发出消息就完事
   *    sendRpc方法中除了发送数据外还是触发回调函数的逻辑，不知道为啥，spark通过handler触发正常返回的回调通过netty的listener直接在失败时把回调函数从map中删除
   *
   * 有两个handler比较容易搞迷：
   *    TransportResponseHandler: 客户端处理返回触发回调（客户端读到数据），例如：客户端发送消息，服务端返回的数据回调需要通过这个handler实现
   *    TransportRequestHandler: 服务端处理请求（服务端读到数据），通过这个调用rpcHandler的receive, 最终会调用RpcEndpoint的receive/receiveAndReply
   *
   * spark是怎么获取每次请求返回对应的回调呢？RpcOutboxMessage和返回的RpcResponse他们的requestId是一样的，通过requestId获取回调
   * 不知道为啥没用netty内置的回调函数listener实现
   *
   * 这里似乎没有服务端主动给客户端发送数据，并读取返回数据
   *
   * @param message
   */
  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    // nettyEnv.address(发送方地址)，this(接收方)就是这个EndpointRef，message(消息内容)
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hashCode(): Int =
    if (endpointAddress == null) 0 else endpointAddress.hashCode()
}

/**
 * The message that is sent from the sender to the receiver.
 *
 * @param senderAddress the sender address. It's `null` if this message is from a client
 *                      `NettyRpcEnv`.
 * @param receiver the receiver of this message.
 * @param content the message content.
 */
private[netty] class RequestMessage(
    val senderAddress: RpcAddress,
    val receiver: NettyRpcEndpointRef,
    val content: Any) {

  /** Manually serialize [[RequestMessage]] to minimize the size. */
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new DataOutputStream(bos)
    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)
      val s = nettyEnv.serializeStream(out)
      try {
        s.writeObject(content)
      } finally {
        s.close()
      }
    } finally {
      out.close()
    }
    bos.toByteBuffer
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit = {
    if (rpcAddress == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(rpcAddress.host)
      out.writeInt(rpcAddress.port)
    }
  }

  override def toString: String = s"RequestMessage($senderAddress, $receiver, $content)"
}

private[netty] object RequestMessage {

  private def readRpcAddress(in: DataInputStream): RpcAddress = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      RpcAddress(in.readUTF(), in.readInt())
    } else {
      null
    }
  }

  def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage = {
    val bis = new ByteBufferInputStream(bytes)
    val in = new DataInputStream(bis)
    try {
      val senderAddress = readRpcAddress(in)
      val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
      val ref = new NettyRpcEndpointRef(nettyEnv.conf, endpointAddress, nettyEnv)
      ref.client = client
      new RequestMessage(
        senderAddress,
        ref,
        // The remaining bytes in `bytes` are the message content.
        nettyEnv.deserialize(client, bytes))
    } finally {
      in.close()
    }
  }
}

/**
 * A response that indicates some failure happens in the receiver side.
 */
private[netty] case class RpcFailure(e: Throwable)

/**
 * 将传入的RPC发送到已注册的端点。把消息转发给dispatcher
 * 原始的消息处理逻辑在TransportRequestHandler中，这个类就是方便network以外的模块扩展
 *
 * Dispatches incoming RPCs to registered endpoints.
 *
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
 *
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler with Logging {

  // A variable to track the remote RpcEnv addresses of all clients
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  /**
   * 把message反序列化成我们的传的对象
   * @param client
   * @param message
   * @return
   */
  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = RequestMessage(nettyEnv, client, message)
    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      logError("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  /**
   * 断开连接后的处理，删除发件箱，下次发送时可以新建发件箱，能重新发送消息
   * @param client
   */
  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      // 通知所有端点这个事件
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}
