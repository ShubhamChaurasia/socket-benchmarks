package com.myprjs.socket;

public abstract class AbstractBenchmarkRunner {

  protected final int numMessages;
  protected final int messageLength;
  protected final int numIterations;

  public AbstractBenchmarkRunner(int numMessages, int messageLength, int numIterations) {
    this.numMessages = numMessages;
    this.messageLength = messageLength;
    this.numIterations = numIterations;
  }

  protected abstract void runBenchmark() throws Exception;

  @Override
  public String toString() {
    return "AbstractBenchmarkRunner{" +
        "numMessages=" + numMessages +
        ", messageLength=" + messageLength +
        ", numIterations=" + numIterations +
        '}';
  }
}
