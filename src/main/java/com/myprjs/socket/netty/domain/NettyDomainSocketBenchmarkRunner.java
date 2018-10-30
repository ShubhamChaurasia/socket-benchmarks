package com.myprjs.socket.netty.domain;

import java.net.SocketAddress;

import com.myprjs.util.Utils;
import com.myprjs.socket.AbstractBenchmarkRunner;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import org.apache.commons.lang3.SystemUtils;


public class NettyDomainSocketBenchmarkRunner extends AbstractBenchmarkRunner {

  private EventLoopGroup serverEventLoopGroup;
  private EventLoopGroup clientEventLoopGroup;
  private Class<? extends ServerChannel> serverChannelClass;
  private Class<? extends DomainSocketChannel> clientDomainSocketChannelClass;

  private SocketAddress domainSocketAddress;

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.SIMPLE);
  }

  public NettyDomainSocketBenchmarkRunner(int numMessages, int messageLength, int numIterations) {
    super(numMessages, messageLength, numIterations);
    System.out.println("NettyDomainSocketBenchmarkRunner.NettyDomainSocketBenchmarkRunner: Initialized.....");
    System.out.println("numMessages = [" + numMessages + "], messageLength = [" + messageLength + "], numIterations = [" + numIterations + "]");
  }

  private void detectOSAndInit() {
    if (SystemUtils.IS_OS_MAC) {
      serverEventLoopGroup = new KQueueEventLoopGroup();
      clientEventLoopGroup = new KQueueEventLoopGroup();
      serverChannelClass = KQueueServerDomainSocketChannel.class;
      clientDomainSocketChannelClass = KQueueDomainSocketChannel.class;
    } else if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_UNIX) {
      serverEventLoopGroup = new EpollEventLoopGroup();
      clientEventLoopGroup = new EpollEventLoopGroup();
      serverChannelClass = EpollServerDomainSocketChannel.class;
      clientDomainSocketChannelClass = EpollDomainSocketChannel.class;
    } else {
      throw new IllegalArgumentException("Domain sockets not supported on OS: " + SystemUtils.OS_NAME);
    }
    domainSocketAddress = new DomainSocketAddress("/tmp/netty.sock." + System.currentTimeMillis());
  }

  private void doRun() throws InterruptedException {
    try {
      detectOSAndInit();

      ServerBootstrap b = new ServerBootstrap();
      b.option(ChannelOption.SO_BACKLOG, 1024);
      b.option(ChannelOption.SO_REUSEADDR, true);
      b.group(serverEventLoopGroup).channel(serverChannelClass);
      b.childHandler(new ChannelInitializer<DomainSocketChannel>() {
        @Override
        protected void initChannel(DomainSocketChannel ch) throws Exception {
          ch.pipeline().addLast(new FixedLengthFrameDecoder(messageLength));
          ch.pipeline().addLast(new GenericHandler());
        }
      });

      b.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));

      Channel ch = b.bind(domainSocketAddress).sync().channel();
      System.out.println("NettyDomainSocketBenchmarkRunner.doRun: Started server. socketAddr = " + domainSocketAddress);
      ch.closeFuture().sync();
    } finally {
      serverEventLoopGroup.shutdownGracefully().sync();
    }
  }

  @Override
  protected void runBenchmark() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      Thread t = new Thread(() -> {
        try {
          doRun();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      t.start();
      System.out.println("------------------------PASS - " + i + "---------------------------");
      //let the server start....
      Thread.sleep(1000);
      fireMessagesToSocket();
      t.join();
    }
  }

  private static class GenericHandler extends SimpleChannelInboundHandler<Object> {

    private long bytesRead;
    private long start;
    private long end;
    private long msgsRead;

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      //System.out.println("GenericHandler.channelRegistered");
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

    public GenericHandler() {
      //System.out.println("GenericHandler.GenericHandler: Handler Created");
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf byteBuf = (ByteBuf) msg;
      msgsRead++;
      bytesRead += byteBuf.readableBytes();
//      System.out.println("GenericHandler.channelRead0: " + byteBuf + " Readable bytes: " + byteBuf.readableBytes() + " msgsRead:" + msgsRead + " bytesRead:" + bytesRead);
    }
  }

  private void fireMessagesToSocket() throws Exception {
    try {
      Bootstrap clientBootstrap = new Bootstrap();
      clientBootstrap.group(clientEventLoopGroup);
      clientBootstrap.remoteAddress(domainSocketAddress);

      clientBootstrap.channel(clientDomainSocketChannelClass);

      clientBootstrap.handler(new ChannelInitializer<DomainSocketChannel>() {
        protected void initChannel(DomainSocketChannel socketChannel) {
          socketChannel.pipeline().addLast(new ChannelInitializer<DomainSocketChannel>() {
            @Override
            protected void initChannel(DomainSocketChannel ch) {
              ch.pipeline().addLast(new ClientHandler(messageLength, numMessages));
            }
          });
        }
      });
      ChannelFuture channelFuture = clientBootstrap.connect().sync();
      channelFuture.channel().closeFuture().sync();
    } finally {
      clientEventLoopGroup.shutdownGracefully().sync();
    }
  }

  private static class ClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private int messageLength;
    private int numMessages;

    ClientHandler(int messageLength, int numMessages) {
      this.messageLength = messageLength;
      this.numMessages = numMessages;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
      //System.out.println("Client: " + msg.toString(CharsetUtil.UTF_8));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws InterruptedException {
      //System.out.println("ClientHandler.channelActive");
      String msg = new String(Utils.getMessage(messageLength));
      for (int i = 0; i < numMessages; i++) {
        ctx.writeAndFlush(Unpooled.wrappedBuffer(msg.getBytes()));
      }
      ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
          .addListener(ChannelFutureListener.CLOSE);
      //System.out.println("ClientHandler.channelActive: COMPLETED");
    }
  }

}
