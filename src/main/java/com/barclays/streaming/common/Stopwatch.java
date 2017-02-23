package com.barclays.streaming.common;

public class Stopwatch {

	  private long start = System.currentTimeMillis();

	  public String toString() {
		  return (System.currentTimeMillis() - start) + " ms";
	  }

}
