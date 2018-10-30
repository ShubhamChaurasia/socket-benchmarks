package com.myprjs.socket.netty.tcp;

import com.myprjs.util.Utils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.util.ResourceLeakDetector;

//Reference: https://github.com/kamatama41/netty-sample-http/blob/master/src/main/java/com/kamatama41/netty/sample/http/HelloWebServer.java

public class NettyServer {

  private final int port;

  //nio, linux_native, mac_native
  private String transportType;
  private int messageLength;


  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
  }

  public NettyServer(int port, String transportType, int messageLength) {
    this.port = port;
    this.transportType = transportType;
    this.messageLength = messageLength;
  }

  public void run() throws Exception {

    if ("nio".equals(transportType)) {
      doRun(new NioEventLoopGroup(), NioServerSocketChannel.class);
    } else if ("linux_native".equals(transportType)) {
      doRun(new EpollEventLoopGroup(), EpollServerSocketChannel.class);
    } else if ("mac_native".equals(transportType)) {
      doRun(new KQueueEventLoopGroup(), KQueueServerSocketChannel.class);
    } else {
      throw new IllegalArgumentException("Illegal transportType: " + transportType + ". Should be one of nio/linux_native/mac_native");
    }
  }

  private void doRun(EventLoopGroup loopGroup, Class<? extends ServerChannel> serverChannelClass) throws InterruptedException {
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.option(ChannelOption.SO_BACKLOG, 1024);
      b.option(ChannelOption.SO_REUSEADDR, true);
      b.group(loopGroup).channel(serverChannelClass);
      b.childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          ch.pipeline().addLast(new FixedLengthFrameDecoder(messageLength));
          ch.pipeline().addLast(new GenericHandler(loopGroup.next()));
        }
      });

      b.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));
      b.childOption(ChannelOption.SO_REUSEADDR, true);


      // b.childOption(ChannelOption.MAX_MESSAGES_PER_READ, Integer.MAX_VALUE);

      Channel ch = b.bind(port).sync().channel();
      System.out.println("NettyDomainSocketBenchmarkRunner.doRun: Started server. port = " + port);
      ch.closeFuture().sync();
    } finally {
      loopGroup.shutdownGracefully().sync();
    }
  }

  private static class GenericHandler extends SimpleChannelInboundHandler<Object> {

    private long bytesRead = 0l;
    private long start = 0l;
    private long end = 0l;
    private long msgsRead = 0l;

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      System.out.println("GenericHandler.channelRegistered");
      this.start = System.nanoTime();
      super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      this.end = System.nanoTime();
      System.out.println("GenericHandler.channelUnregistered: bytesRead: " + bytesRead + " msgsRead: " + msgsRead + "  Time(millis): " + (end - start) / 1000_000);
      super.channelUnregistered(ctx);
      ctx.channel().close();
      ctx.channel().parent().close();
    }

    public GenericHandler(EventLoop eventLoop) {
      //System.out.println("GenericHandler.GenericHandler: Handler Created: " + eventLoop);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf byteBuf = (ByteBuf) msg;
      bytesRead += byteBuf.readableBytes();
      msgsRead++;
    }
  }

  public static void main(String[] args) throws Exception {
    final int port = 5678;

    final int messageLength = 10000;
    int numMessages = 100000;
    int numIterations = 3;

    for (int i = 0; i < numIterations; i++) {
      Thread t = new Thread(() -> {
        try {
          new NettyServer(port, "mac_native", messageLength).run();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      t.start();
      System.out.println("------------------------PASS - " + i + "---------------------------");
      Thread.sleep(1000);
      Utils.fireMessagesToSocket(numMessages, messageLength, port);
      t.join();
    }
  }


}
