package de.leipzig.htwk.gitrdf.shacl.model;


public class OrderInfoData {
  private final Long orderId;
  private final String platform;
  private final String ownerName;
  private final String repositoryName;
  private final String status;
  private final boolean hasRdfData;
  private final long timestamp;

  public OrderInfoData(Long orderId, String platform, String ownerName, String repositoryName,
      String status, boolean hasRdfData) {
    this.orderId = orderId;
    this.platform = platform;
    this.ownerName = ownerName;
    this.repositoryName = repositoryName;
    this.status = status;
    this.hasRdfData = hasRdfData;
    this.timestamp = System.currentTimeMillis();
  }

  // Getters
  public Long getOrderId() {
    return orderId;
  }

  public String getPlatform() {
    return platform;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public String getRepositoryName() {
    return repositoryName;
  }

  public String getStatus() {
    return status;
  }

  public boolean hasRdfData() {
    return hasRdfData;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getFullRepositoryName() {
    return ownerName + "/" + repositoryName;
  }

  public String getDisplayName() {
    return platform + ": " + getFullRepositoryName();
  }
}