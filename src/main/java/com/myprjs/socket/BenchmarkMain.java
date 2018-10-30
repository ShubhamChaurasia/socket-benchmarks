package com.myprjs.socket;


import com.myprjs.socket.java.TCPSocketBenchmarkRunner;
import com.myprjs.socket.netty.domain.NettyDomainSocketBenchmarkRunner;
import org.apache.commons.lang3.math.NumberUtils;

public class BenchmarkMain {

  private static final String[] DEFAULT_ARGS = {"domain_socket", "10000", "10000", "3"};

  public static void main(String[] args) throws Exception {

    System.out.println("To MODIFY defaults, run with: <tcp_socket/domain_socket> <numMessages> <messageLength> <numIterations>");

    String socketType = getArg(args, 0);
    int numMessages = NumberUtils.toInt(getArg(args, 1));
    int messageLength = NumberUtils.toInt(getArg(args, 2));
    int numIterations = NumberUtils.toInt(getArg(args, 3));

    AbstractBenchmarkRunner runner;

    if ("tcp_socket".equals(socketType)) {
      runner = new TCPSocketBenchmarkRunner(numMessages, messageLength, numIterations);
    } else if ("domain_socket".equals(socketType)) {
      runner = new NettyDomainSocketBenchmarkRunner(numMessages, messageLength, numIterations);
    } else {
      throw new IllegalArgumentException("BenchmarkMain: args[0] should be <tcp_socket/domain_socket>");
    }

    runner.runBenchmark();
  }

  private static String getArg(String[] args, int index) {
    return args.length > index ? args[index] : DEFAULT_ARGS[index];
  }


}
