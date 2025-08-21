package de.leipzig.htwk.gitrdf.shacl.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "shacl.validation")
public class ValidationConfig {

  private boolean strictMode = false;
  private boolean enableGitValidation = true;
  private boolean enableGithubValidation = true;
  private boolean enablePlatformValidation = true;
  private boolean enableAnalysisValidation = true;
  
  @Value("${shacl.uri-fixing-enabled:true}")
  private boolean uriFixingEnabled;
  
  @Value("${shacl.max-file-size-mb:50}")
  private int maxFileSizeMb;

  // Getters and setters
  public boolean isStrictMode() {
    return strictMode;
  }

  public void setStrictMode(boolean strictMode) {
    this.strictMode = strictMode;
  }

  public boolean isEnableGitValidation() {
    return enableGitValidation;
  }

  public void setEnableGitValidation(boolean enableGitValidation) {
    this.enableGitValidation = enableGitValidation;
  }

  public boolean isEnableGithubValidation() {
    return enableGithubValidation;
  }

  public void setEnableGithubValidation(boolean enableGithubValidation) {
    this.enableGithubValidation = enableGithubValidation;
  }

  public boolean isEnablePlatformValidation() {
    return enablePlatformValidation;
  }

  public void setEnablePlatformValidation(boolean enablePlatformValidation) {
    this.enablePlatformValidation = enablePlatformValidation;
  }

  public boolean isEnableAnalysisValidation() {
    return enableAnalysisValidation;
  }

  public void setEnableAnalysisValidation(boolean enableAnalysisValidation) {
    this.enableAnalysisValidation = enableAnalysisValidation;
  }

  public boolean isUriFixingEnabled() {
    return uriFixingEnabled;
  }

  public void setUriFixingEnabled(boolean uriFixingEnabled) {
    this.uriFixingEnabled = uriFixingEnabled;
  }

  public int getMaxFileSizeMb() {
    return maxFileSizeMb;
  }

  public void setMaxFileSizeMb(int maxFileSizeMb) {
    this.maxFileSizeMb = maxFileSizeMb;
  }
}