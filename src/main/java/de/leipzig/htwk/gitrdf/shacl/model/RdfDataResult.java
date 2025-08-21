package de.leipzig.htwk.gitrdf.shacl.model;

import java.io.InputStream;

public class RdfDataResult {
  public final InputStream inputStream;
  public final String platform;
  public final String orderId;
  public final String message;
  public final long timestamp;

  public RdfDataResult(InputStream inputStream, String platform, String orderId, String message) {
    this.inputStream = inputStream;
    this.platform = platform;
    this.orderId = orderId;
    this.message = message;
    this.timestamp = System.currentTimeMillis();
  }

  @Override
  public String toString() {
    return String.format("RdfDataResult{platform='%s', orderId='%s', message='%s'}",
        platform, orderId, message);
  }
}