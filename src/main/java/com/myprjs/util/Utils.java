package com.myprjs.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

public final class Utils {

  public static byte[] getMessage(int messageLength) {
    byte[] arr = new byte[messageLength];
    Arrays.fill(arr, (byte) 'a');
    return arr;
  }

  public static void fireMessagesToSocket(int numMessages, int messageLength, int port) {
    long start = System.nanoTime();
    try (Socket socket = new Socket("localhost", port);
         PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
      String message = new String(getMessage(messageLength));
      for (int i = 0; i < numMessages; i++) {
        out.println(message);
        out.flush();
      }
      long end = System.nanoTime();
      System.out.println("Utils.fireMessagesToSocket: Written: numMessages = [" + numMessages + "], messageLength = [" + messageLength + "]" + " Time(milli): " + (end - start) / 1000_000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static int findFreePort() throws IOException {
    try (
        ServerSocket socket = new ServerSocket(0);
    ) {
      return socket.getLocalPort();
    }
  }

}
