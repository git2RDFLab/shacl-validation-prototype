package de.leipzig.htwk.gitrdf.shacl.model;

import java.util.List;

import de.leipzig.htwk.gitrdf.shacl.service.ShaclDiagnosticService.ViolationDetail;

public class ValidationResult {
  private final String orderId;
  private final String platform;
  private final boolean valid;
  private final long durationMs;
  private final boolean strictMode;
  private final String errorMessage;
  private final List<ViolationDetail> violations;
  private final String validationReport;
  private final long timestamp;

  public ValidationResult(String orderId, String platform, boolean valid, long durationMs, boolean strictMode) {
    this(orderId, platform, valid, durationMs, strictMode, null, List.of(), null);
  }

  public ValidationResult(String orderId, String platform, boolean valid, long durationMs,
      boolean strictMode, String errorMessage) {
    this(orderId, platform, valid, durationMs, strictMode, errorMessage, List.of(), null);
  }

  public ValidationResult(String orderId, String platform, boolean valid, long durationMs,
      boolean strictMode, String errorMessage, List<ViolationDetail> violations, String validationReport) {
    this.orderId = orderId;
    this.platform = platform;
    this.valid = valid;
    this.durationMs = durationMs;
    this.strictMode = strictMode;
    this.errorMessage = errorMessage;
    this.violations = violations != null ? List.copyOf(violations) : List.of();
    this.validationReport = validationReport;
    this.timestamp = System.currentTimeMillis();
  }

  // Getters
  public String getOrderId() {
    return orderId;
  }

  public String getPlatform() {
    return platform;
  }

  public boolean isValid() {
    return valid;
  }

  public long getDurationMs() {
    return durationMs;
  }

  public boolean isStrictMode() {
    return strictMode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean hasError() {
    return errorMessage != null;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public List<ViolationDetail> getViolations() {
    return violations;
  }

  public String getValidationReport() {
    return validationReport;
  }

  public int getViolationCount() {
    return violations.size();
  }

  public boolean hasViolations() {
    return !violations.isEmpty();
  }

  public String getDisplayName() {
    return platform + " Order " + orderId;
  }

  public String getSummary() {
    if (valid) {
      return String.format("%s is valid (took %dms)", getDisplayName(), durationMs);
    } else {
      return String.format(" %s has %d violations (took %dms)", getDisplayName(), violations.size(), durationMs);
    }
  }
}