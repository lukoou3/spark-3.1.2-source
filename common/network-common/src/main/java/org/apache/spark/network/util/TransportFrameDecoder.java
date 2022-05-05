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

package org.apache.spark.network.util;

import java.util.LinkedList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A customized frame decoder that allows intercepting raw data.
 * <p>
 * This behaves like Netty's frame decoder (with hard coded parameters that match this library's
 * needs), except it allows an interceptor to be installed to read data directly before it's
 * framed.
 * <p>
 * Unlike Netty's frame decoder, each frame is dispatched to child handlers as soon as it's
 * decoded, instead of building as many frames as the current buffer allows and dispatching
 * all of them. This allows a child handler to install an interceptor if needed.
 * <p>
 * If an interceptor is installed, framing stops, and data is instead fed directly to the
 * interceptor. When the interceptor indicates that it doesn't need to read any more data,
 * framing resumes. Interceptors should not hold references to the data buffers provided
 * to their handle() method.
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {

  public static final String HANDLER_NAME = "frameDecoder";
  private static final int LENGTH_SIZE = 8;
  private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
  private static final int UNKNOWN_FRAME_SIZE = -1;
  private static final long CONSOLIDATE_THRESHOLD = 20 * 1024 * 1024;

  private final LinkedList<ByteBuf> buffers = new LinkedList<>();
  private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE, LENGTH_SIZE);
  private final long consolidateThreshold;

  private CompositeByteBuf frameBuf = null;
  private long consolidatedFrameBufSize = 0;
  private int consolidatedNumComponents = 0;

  private long totalSize = 0;
  private long nextFrameSize = UNKNOWN_FRAME_SIZE;
  private int frameRemainingBytes = UNKNOWN_FRAME_SIZE;
  private volatile Interceptor interceptor;

  public TransportFrameDecoder() {
    this(CONSOLIDATE_THRESHOLD);
  }

  @VisibleForTesting
  TransportFrameDecoder(long consolidateThreshold) {
    this.consolidateThreshold = consolidateThreshold;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
    ByteBuf in = (ByteBuf) data;
    buffers.add(in);
    totalSize += in.readableBytes();

    while (!buffers.isEmpty()) {
      // 如果interceptor不为null，就调用interceptor，主要用于文件上传下载的流实现
      // First, feed the interceptor, and if it's still, active, try again.
      if (interceptor != null) {
        ByteBuf first = buffers.getFirst();
        int available = first.readableBytes();
        // 喂interceptor
        if (feedInterceptor(first)) {
          assert !first.isReadable() : "Interceptor still active but buffer has data.";
        }

        int read = available - first.readableBytes();
        // first读完了
        if (read == available) {
          buffers.removeFirst().release();
        }
        totalSize -= read;
      } else {
        // Interceptor is not active, so try to decode one frame.
        // 解码frame
        ByteBuf frame = decodeNext();
        if (frame == null) {
          break;
        }
        ctx.fireChannelRead(frame);
      }
    }
  }

  /**
   * 编码出frame的大小，正常情况下一个报文就能解析出，毕竟只需要读取8个字节
   */
  private long decodeFrameSize() {
    /**
     * 从MessageEncoder可以知道，spark的报文总体结构是固定的: 报文总长度(8字节) + 内容， LENGTH_SIZE就是常量8，报文总长度所占的字节
     * 下个frame的大小已知返回nextFrameSize；总长度不够8字节直接返回 -1 UNKNOWN_FRAME_SIZE
     */
    if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
      return nextFrameSize;
    }

    /**
     * 我们知道有足够的数据。如果第一个缓冲区包含所有数据，那就太好了。
     * 否则，将帧长度的字节保存在复合缓冲区中，直到有足够的数据读取帧大小。
     * 通常情况下，很少需要一个以上的缓冲区来读取帧大小。
     */
    // We know there's enough data. If the first buffer contains all the data, great. Otherwise,
    // hold the bytes for the frame length in a composite buffer until we have enough data to read
    // the frame size. Normally, it should be rare to need more than one buffer to read the frame
    // size.
    ByteBuf first = buffers.getFirst();
    if (first.readableBytes() >= LENGTH_SIZE) {
      // 这里直接调用的readLong，MessageDecoder并不需要解析这8个字节。frame的大小就是内容的长度
      nextFrameSize = first.readLong() - LENGTH_SIZE;
      totalSize -= LENGTH_SIZE;
      // 如果first读完就直接释放
      if (!first.isReadable()) {
        buffers.removeFirst().release();
      }
      return nextFrameSize;
    }

    /**
     * frameLenBuf：固定8个字节的ByteBuf，用于读取内容长度头信息
     * 读够8个字节就停止，这里没有ByteBuf的合并，高效
     * 这里肯定能读够8个字节，这个方法的开头就判断了
     */
    while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
      ByteBuf next = buffers.getFirst();
      // 需要读取的大小
      int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
      frameLenBuf.writeBytes(next, toRead);
      // 读完释放
      if (!next.isReadable()) {
        buffers.removeFirst().release();
      }
    }

    nextFrameSize = frameLenBuf.readLong() - LENGTH_SIZE;
    totalSize -= LENGTH_SIZE;
    frameLenBuf.clear(); // 这里需要清空
    return nextFrameSize;
  }

  private ByteBuf decodeNext() {
    // 解析出frame的大小，一般能解析，只需读取8个字节
    long frameSize = decodeFrameSize();
    if (frameSize == UNKNOWN_FRAME_SIZE) {
      return null;
    }

    if (frameBuf == null) {
      // frame的大小不能太大也不能小于0
      Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE,
          "Too large frame: %s", frameSize);
      Preconditions.checkArgument(frameSize > 0,
          "Frame length should be positive: %s", frameSize);
      frameRemainingBytes = (int) frameSize;

      // 如果缓冲区为空，则立即返回以获取更多输入数据。
      // If buffers is empty, then return immediately for more input data.
      if (buffers.isEmpty()) {
        return null;
      }

      // 否则，如果第一个buffer足够，解码出Frame返回。
      // Otherwise, if the first buffer holds the entire frame, we attempt to
      // build frame with it and return.
      if (buffers.getFirst().readableBytes() >= frameRemainingBytes) {
        // Reset buf and size for next frame.
        frameBuf = null;
        nextFrameSize = UNKNOWN_FRAME_SIZE;
        return nextBufferForFrame(frameRemainingBytes);
      }
      // Other cases, create a composite buffer to manage all the buffers.
      frameBuf = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
    }

    // 缓冲frame需要读的buf到frameBuf
    while (frameRemainingBytes > 0 && !buffers.isEmpty()) {
      // 下一个ByteBuf用于填充Frame
      ByteBuf next = nextBufferForFrame(frameRemainingBytes);
      frameRemainingBytes -= next.readableBytes();
      frameBuf.addComponent(true, next);
    }


    // 如果frameBuf的增量大小超过阈值，那么我们会进行整合以减少内存消耗
    // If the delta size of frameBuf exceeds the threshold, then we do consolidation
    // to reduce memory consumption.
    if (frameBuf.capacity() - consolidatedFrameBufSize > consolidateThreshold) {
      int newNumComponents = frameBuf.numComponents() - consolidatedNumComponents;
      frameBuf.consolidate(consolidatedNumComponents, newNumComponents);
      consolidatedFrameBufSize = frameBuf.capacity();
      consolidatedNumComponents = frameBuf.numComponents();
    }

    // frame没读够
    if (frameRemainingBytes > 0) {
      return null;
    }

    // 读够, 消费当前的frameBuf。frameBuf就是全部的frame
    return consumeCurrentFrameBuf();
  }

  private ByteBuf consumeCurrentFrameBuf() {
    ByteBuf frame = frameBuf;
    // Reset buf and size for next frame.
    frameBuf = null;
    consolidatedFrameBufSize = 0;
    consolidatedNumComponents = 0;
    nextFrameSize = UNKNOWN_FRAME_SIZE;
    return frame;
  }

  /**
   * 下一个ByteBuf用于填充Frame
   * Takes the first buffer in the internal list, and either adjust it to fit in the frame
   * (by taking a slice out of it) or remove it from the internal list.
   */
  private ByteBuf nextBufferForFrame(int bytesToRead) {
    ByteBuf buf = buffers.getFirst();
    ByteBuf frame;

    if (buf.readableBytes() > bytesToRead) {
      frame = buf.retain().readSlice(bytesToRead);
      totalSize -= bytesToRead;
    } else {
      frame = buf;
      buffers.removeFirst();
      totalSize -= frame.readableBytes();
    }

    return frame;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (interceptor != null) {
      interceptor.channelInactive();
    }
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (interceptor != null) {
      interceptor.exceptionCaught(cause);
    }
    super.exceptionCaught(ctx, cause);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    // Release all buffers that are still in our ownership.
    // Doing this in handlerRemoved(...) guarantees that this will happen in all cases:
    //     - When the Channel becomes inactive
    //     - When the decoder is removed from the ChannelPipeline
    for (ByteBuf b : buffers) {
      b.release();
    }
    buffers.clear();
    frameLenBuf.release();
    ByteBuf frame = consumeCurrentFrameBuf();
    if (frame != null) {
      frame.release();
    }
    super.handlerRemoved(ctx);
  }

  public void setInterceptor(Interceptor interceptor) {
    Preconditions.checkState(this.interceptor == null, "Already have an interceptor.");
    this.interceptor = interceptor;
  }

  /**
   * @return Whether the interceptor is still active after processing the data.
   */
  private boolean feedInterceptor(ByteBuf buf) throws Exception {
    // interceptor.handle返回true代表还需数据要读取，数据读完时把interceptor置空
    if (interceptor != null && !interceptor.handle(buf)) {
      interceptor = null;
    }
    return interceptor != null;
  }

  public interface Interceptor {

    /**
     * Handles data received from the remote end.
     *
     * @param data Buffer containing data.
     * @return "true" if the interceptor expects more data, "false" to uninstall the interceptor.
     */
    boolean handle(ByteBuf data) throws Exception;

    /** Called if an exception is thrown in the channel pipeline. */
    void exceptionCaught(Throwable cause) throws Exception;

    /** Called if the channel is closed and the interceptor is still installed. */
    void channelInactive() throws Exception;

  }

}
