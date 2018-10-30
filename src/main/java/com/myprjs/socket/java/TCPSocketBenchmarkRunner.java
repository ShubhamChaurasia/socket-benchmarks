package com.myprjs.socket.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import com.myprjs.Util.Utils;
import com.myprjs.socket.AbstractBenchmarkRunner;
import org.apache.commons.io.IOUtils;

public class TCPSocketBenchmarkRunner extends AbstractBenchmarkRunner {

  public TCPSocketBenchmarkRunner(int numMessages, int messageLength, int numIterations) {
    super(numMessages, messageLength, numIterations);
    System.out.println("TCPSocketBenchmarkRunner.TCPSocketBenchmarkRunner: Initialized.....");
    System.out.println("numMessages = [" + numMessages + "], messageLength = [" + messageLength + "], numIterations = [" + numIterations + "]");
  }

  private void createAndStartServer(int port) {
    BufferedReader in = null;
    Socket clientSocket = null;
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(port);
      clientSocket = serverSocket.accept();
      in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      long start = System.nanoTime();
      String inputLine;
      long bytesRead = 0;
      while ((inputLine = in.readLine()) != null) {
        bytesRead += inputLine.getBytes().length;
      }
      long end = System.nanoTime();
      System.out.println("TCPSocketBenchmarkRunner.createAndStartServer --- Bytes Read: " + bytesRead + " Time(milli): " + (end - start) / 1000_000);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(in);
      IOUtils.closeQuietly(clientSocket);
      IOUtils.closeQuietly(serverSocket);
    }

  }


  @Override
  protected void runBenchmark() throws Exception {
    int port = Utils.findFreePort();
    for (int i = 0; i < numIterations; i++) {
      Thread t = new Thread(() -> createAndStartServer(port));
      t.start();
      Thread.sleep(1000);
      System.out.println("------------------------PASS - " + i + "---------------------------");
      Utils.fireMessagesToSocket(numMessages, messageLength, port);
      t.join();
    }
  }
}
