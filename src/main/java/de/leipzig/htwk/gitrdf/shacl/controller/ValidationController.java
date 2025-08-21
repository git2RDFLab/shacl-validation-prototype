package de.leipzig.htwk.gitrdf.shacl.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import de.leipzig.htwk.gitrdf.shacl.service.ShaclDiagnosticService;
import de.leipzig.htwk.gitrdf.shacl.service.ShaclDiagnosticService.ValidationDiagnostics;
import de.leipzig.htwk.gitrdf.shacl.service.ShaclValidationService;

@RestController
@RequestMapping("/api/validation")
@Deprecated // Legacy controller - use IntegratedValidationController for new features
public class ValidationController {

  private static final Logger logger = LoggerFactory.getLogger(ValidationController.class);

  @Autowired
  private ShaclValidationService validationService;
  @Autowired
  private ShaclDiagnosticService diagnosticService;

  // ========== STREAMLINED VALIDATION ENDPOINTS ==========

  /**
   * Validate a local RDF file from /local_data directory
   */
  @GetMapping("/local/{fileName}")
  public ResponseEntity<LocalValidationResponse> validateLocalFile(
      @PathVariable String fileName,
      @RequestParam(defaultValue = "false") boolean strict,
      @RequestParam(defaultValue = "v2") String shape) {
    
    logger.info("[VALIDATION] Local RDF validation: {} from /local_data (strict: {}, shape: {})", fileName, strict, shape);
    
    try {
      ValidationDiagnostics diagnostics = diagnosticService.validateRdfFile(fileName, strict, shape);
      
      LocalValidationResponse response = new LocalValidationResponse(
          diagnostics.fileName(),
          diagnostics.durationMs(),
          strict,
          shape,
          diagnostics.getViolationCount(),
          diagnostics.isValid() ? "VALID" : " INVALID"
      );
      
      return diagnostics.isValid() ? 
          ResponseEntity.ok(response) : 
          ResponseEntity.badRequest().body(response);
          
    } catch (IllegalArgumentException e) {
      logger.error("Invalid request for file: {} - {}", fileName, e.getMessage());
      
      LocalValidationResponse errorResponse = new LocalValidationResponse(
          fileName,
          0,
          strict,
          shape,
          0,
          " INVALID: " + e.getMessage()
      );
      
      return ResponseEntity.badRequest().body(errorResponse);
    } catch (Exception e) {
      logger.error("Local validation failed for file: {}", fileName, e);
      
      LocalValidationResponse errorResponse = new LocalValidationResponse(
          fileName,
          0,
          strict,
          shape,
          0,
          " INVALID"
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  /**
   * Validate a local RDF file from /local_data directory (query parameter version)
   * Supports nested paths like: analysis/statistics/file.ttl
   */
  @GetMapping("/local/file")
  public ResponseEntity<LocalValidationResponse> validateLocalFileByQuery(
      @RequestParam String file,
      @RequestParam(defaultValue = "false") boolean strict,
      @RequestParam(defaultValue = "v2") String shape) {
    
    logger.info("🔍 Local RDF validation: {} from /local_data (strict: {}, shape: {})", file, strict, shape);
    
    try {
      ValidationDiagnostics diagnostics = diagnosticService.validateRdfFile(file, strict, shape);
      
      LocalValidationResponse response = new LocalValidationResponse(
          diagnostics.fileName(),
          diagnostics.durationMs(),
          strict,
          shape,
          diagnostics.getViolationCount(),
          diagnostics.isValid() ? "VALID" : " INVALID"
      );
      
      return diagnostics.isValid() ? 
          ResponseEntity.ok(response) : 
          ResponseEntity.badRequest().body(response);
          
    } catch (IllegalArgumentException e) {
      logger.error("Invalid request for file: {} - {}", file, e.getMessage());
      
      LocalValidationResponse errorResponse = new LocalValidationResponse(
          file,
          0,
          strict,
          shape,
          0,
          " INVALID: " + e.getMessage()
      );
      
      return ResponseEntity.badRequest().body(errorResponse);
    } catch (Exception e) {
      logger.error("Local validation failed for file: {}", file, e);
      
      LocalValidationResponse errorResponse = new LocalValidationResponse(
          file,
          0,
          strict,
          shape,
          0,
          " INVALID"
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  /**
   * Validate an order (GitHub repository)
   */
  @GetMapping("/order/{orderId}")
  public ResponseEntity<OrderValidationResponse> validateOrder(
      @PathVariable String orderId,
      @RequestParam(defaultValue = "false") boolean strict,
      @RequestParam(defaultValue = "v2") String shape) {

    logger.info("🔍 Order validation: {} (strict: {}, shape: {})", orderId, strict, shape);

    try {
      de.leipzig.htwk.gitrdf.shacl.model.ValidationResult result = 
          validationService.validateGithubOrderWithDiagnostics(orderId, strict, shape);

      OrderValidationResponse response = new OrderValidationResponse(
          result.getOrderId(), 
          result.getPlatform(), 
          result.isValid(), 
          result.getDurationMs(), 
          result.isStrictMode(),
          result.getErrorMessage(),
          result.getViolationCount(),
          result.getSummary()
      );
      
      return result.isValid() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);

    } catch (IllegalArgumentException e) {
      logger.error("Invalid request for order: {} - {}", orderId, e.getMessage());
      OrderValidationResponse response = new OrderValidationResponse(
          orderId, "GitHub", false, 0, strict, "Invalid request: " + e.getMessage(), 0, "[INVALID_REQUEST]"
      );
      return ResponseEntity.badRequest().body(response);
    } catch (Exception e) {
      logger.error("Order validation failed: {} (strict: {})", orderId, strict, e);
      OrderValidationResponse response = new OrderValidationResponse(
          orderId, "GitHub", false, 0, strict, e.getMessage(), 0, "[VALIDATION_FAILED]"
      );
      return ResponseEntity.status(500).body(response);
    }
  }

  /**
   * List all available RDF files in /local_data directory
   */
  @GetMapping("/local/files")
  public ResponseEntity<AvailableFilesResponse> listAvailableFiles() {
    logger.info("📁 Listing available RDF files in /local_data");
    
    try {
      List<String> fileNames = diagnosticService.discoverAvailableRdfFiles();
      
      AvailableFilesResponse response = new AvailableFilesResponse(
          fileNames,
          fileNames.size(),
          "/local_data"
      );
      
      return ResponseEntity.ok(response);
      
    } catch (Exception e) {
      logger.error("Failed to list available files", e);
      
      AvailableFilesResponse errorResponse = new AvailableFilesResponse(
          List.of(),
          0,
          "/local_data"
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  /**
   * Validate all RDF files in /local_data directory
   */
  @GetMapping("/local/all")
  public ResponseEntity<AllFilesValidationResponse> validateAllLocalFiles(
      @RequestParam(defaultValue = "false") boolean strict,
      @RequestParam(defaultValue = "v2") String shape) {
    
    logger.info("🔍 Validating all RDF files in /local_data (strict: {}, shape: {})", strict, shape);
    
    try {
      List<ShaclDiagnosticService.ValidationDiagnostics> results = diagnosticService.validateAllRdfFiles(strict, shape);
      
      long totalValid = results.stream().mapToLong(r -> r.isValid() ? 1 : 0).sum();
      long totalInvalid = results.size() - totalValid;
      long totalViolations = results.stream().mapToLong(r -> r.getViolationCount()).sum();
      
      AllFilesValidationResponse response = new AllFilesValidationResponse(
          results,
          results.size(),
          (int) totalValid,
          (int) totalInvalid,
          (int) totalViolations,
          strict
      );
      
      return totalInvalid == 0 ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
      
    } catch (IllegalArgumentException e) {
      logger.error("Invalid request for validate all files - {}", e.getMessage());
      
      AllFilesValidationResponse errorResponse = new AllFilesValidationResponse(
          List.of(),
          0,
          0,
          0,
          0,
          strict
      );
      
      return ResponseEntity.badRequest().body(errorResponse);
    } catch (Exception e) {
      logger.error("Failed to validate all files", e);
      
      AllFilesValidationResponse errorResponse = new AllFilesValidationResponse(
          List.of(),
          0,
          0,
          0,
          0,
          strict
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  /**
   * List all available RDF files in a specific folder within /local_data directory
   * Supports nested paths like: some/nested/rankings
   */
  @GetMapping("/local/folder/{folderPath}/files")
  public ResponseEntity<AvailableFilesResponse> listFilesInFolder(
      @PathVariable String folderPath) {
    
    // Replace URL-encoded slashes back to normal slashes for nested paths
    folderPath = folderPath.replace("%2F", "/");
    logger.info("📁 Listing RDF files in /local_data/{}", folderPath);
    
    try {
      List<String> fileNames = diagnosticService.discoverRdfFilesInFolder(folderPath);
      
      AvailableFilesResponse response = new AvailableFilesResponse(
          fileNames,
          fileNames.size(),
          "/local_data/" + folderPath
      );
      
      return ResponseEntity.ok(response);
      
    } catch (Exception e) {
      logger.error("Failed to list files in folder: {}", folderPath, e);
      
      AvailableFilesResponse errorResponse = new AvailableFilesResponse(
          List.of(),
          0,
          "/local_data/" + folderPath
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  /**
   * List all available RDF files in a specific folder within /local_data directory (query parameter version)
   * Supports nested paths like: some/nested/rankings
   */
  @GetMapping("/local/folder/files")
  public ResponseEntity<AvailableFilesResponse> listFilesInFolderByQuery(
      @RequestParam String path) {
    
    logger.info("📁 Listing RDF files in /local_data/{}", path);
    
    try {
      List<String> fileNames = diagnosticService.discoverRdfFilesInFolder(path);
      
      AvailableFilesResponse response = new AvailableFilesResponse(
          fileNames,
          fileNames.size(),
          "/local_data/" + path
      );
      
      return ResponseEntity.ok(response);
      
    } catch (Exception e) {
      logger.error("Failed to list files in folder: {}", path, e);
      
      AvailableFilesResponse errorResponse = new AvailableFilesResponse(
          List.of(),
          0,
          "/local_data/" + path
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  /**
   * Validate all RDF files in a specific folder within /local_data directory
   * Supports nested paths like: some/nested/rankings
   */
  @GetMapping("/local/folder/{folderPath}")
  public ResponseEntity<FolderValidationResponse> validateFilesInFolder(
      @PathVariable String folderPath,
      @RequestParam(defaultValue = "false") boolean strict,
      @RequestParam(defaultValue = "v2") String shape) {
    
    // Replace URL-encoded slashes back to normal slashes for nested paths
    folderPath = folderPath.replace("%2F", "/");
    logger.info("🔍 Validating all RDF files in /local_data/{} (strict: {}, shape: {})", folderPath, strict, shape);
    
    try {
      long startTime = System.currentTimeMillis();
      List<ShaclDiagnosticService.ValidationDiagnostics> results = 
          diagnosticService.validateRdfFilesInFolder(folderPath, strict, shape);
      long durationMs = System.currentTimeMillis() - startTime;
      
      long totalValid = results.stream().mapToLong(r -> r.isValid() ? 1 : 0).sum();
      long totalInvalid = results.size() - totalValid;
      long totalViolations = results.stream().mapToLong(r -> r.getViolationCount()).sum();
      
      List<SimpleFileResult> fileResults = results.stream()
          .map(r -> new SimpleFileResult(
              r.fileName(),
              r.durationMs(),
              r.strictMode(),
              r.getViolationCount(),
              r.isValid() ? "VALID" : " INVALID"
          ))
          .toList();
      
      String folderSummary;
      if (totalInvalid == 0) {
          folderSummary = "ALL_VALID";
      } else if (totalValid == 0) {
          folderSummary = " ALL_INVALID";
      } else {
          folderSummary = "⚠️ SOME_INVALID";
      }
      
      FolderValidationResponse response = new FolderValidationResponse(
          folderPath,
          durationMs,
          strict,
          results.size(),
          (int) totalValid,
          (int) totalInvalid,
          (int) totalViolations,
          folderSummary,
          fileResults
      );
      
      return totalInvalid == 0 ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
      
    } catch (IllegalArgumentException e) {
      logger.error("Invalid request for folder validation: {} - {}", folderPath, e.getMessage());
      
      FolderValidationResponse errorResponse = new FolderValidationResponse(
          folderPath,
          0,
          strict,
          0,
          0,
          0,
          0,
          " INVALID: " + e.getMessage(),
          List.of()
      );
      
      return ResponseEntity.badRequest().body(errorResponse);
    } catch (Exception e) {
      logger.error("Failed to validate files in folder: {}", folderPath, e);
      
      FolderValidationResponse errorResponse = new FolderValidationResponse(
          folderPath,
          0,
          strict,
          0,
          0,
          0,
          0,
          " ERROR",
          List.of()
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  /**
   * Validate all RDF files in a specific folder within /local_data directory (query parameter version)
   * Supports nested paths like: some/nested/rankings
   */
  @GetMapping("/local/folder")
  public ResponseEntity<FolderValidationResponse> validateFilesInFolderByQuery(
      @RequestParam String path,
      @RequestParam(defaultValue = "false") boolean strict,
      @RequestParam(defaultValue = "v2") String shape) {
    
    logger.info("🔍 Validating all RDF files in /local_data/{} (strict: {}, shape: {})", path, strict, shape);
    
    try {
      long startTime = System.currentTimeMillis();
      List<ShaclDiagnosticService.ValidationDiagnostics> results = 
          diagnosticService.validateRdfFilesInFolder(path, strict, shape);
      long durationMs = System.currentTimeMillis() - startTime;
      
      long totalValid = results.stream().mapToLong(r -> r.isValid() ? 1 : 0).sum();
      long totalInvalid = results.size() - totalValid;
      long totalViolations = results.stream().mapToLong(r -> r.getViolationCount()).sum();
      
      List<SimpleFileResult> fileResults = results.stream()
          .map(r -> new SimpleFileResult(
              r.fileName(),
              r.durationMs(),
              r.strictMode(),
              r.getViolationCount(),
              r.isValid() ? "VALID" : " INVALID"
          ))
          .toList();
      
      String folderSummary;
      if (totalInvalid == 0) {
          folderSummary = "ALL_VALID";
      } else if (totalValid == 0) {
          folderSummary = " ALL_INVALID";
      } else {
          folderSummary = "⚠️ SOME_INVALID";
      }
      
      FolderValidationResponse response = new FolderValidationResponse(
          path,
          durationMs,
          strict,
          results.size(),
          (int) totalValid,
          (int) totalInvalid,
          (int) totalViolations,
          folderSummary,
          fileResults
      );
      
      return totalInvalid == 0 ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
      
    } catch (IllegalArgumentException e) {
      logger.error("Invalid request for folder validation: {} - {}", path, e.getMessage());
      
      FolderValidationResponse errorResponse = new FolderValidationResponse(
          path,
          0,
          strict,
          0,
          0,
          0,
          0,
          " INVALID: " + e.getMessage(),
          List.of()
      );
      
      return ResponseEntity.badRequest().body(errorResponse);
    } catch (Exception e) {
      logger.error("Failed to validate files in folder: {}", path, e);
      
      FolderValidationResponse errorResponse = new FolderValidationResponse(
          path,
          0,
          strict,
          0,
          0,
          0,
          0,
          " ERROR",
          List.of()
      );
      
      return ResponseEntity.status(500).body(errorResponse);
    }
  }

  // ========== RESPONSE DTOS ==========

  public record LocalValidationResponse(
      String fileName,
      long durationMs,
      boolean strictMode,
      String shapeVersion,
      int violationCount,
      String summary
  ) {}

  public record OrderValidationResponse(
      String orderId,
      String platform,
      boolean isValid,
      long durationMs,
      boolean strictMode,
      String errorMessage,
      int violationCount,
      String summary
  ) {
    public String getStatus() {
      return isValid ? "VALID" : "INVALID";
    }
    
    public String getMode() {
      return strictMode ? "STRICT" : "STANDARD";
    }
    
    public boolean hasViolations() {
      return violationCount > 0;
    }
    
    public boolean hasError() {
      return errorMessage != null;
    }
    
    public String getDisplayName() {
      return platform + " Order " + orderId;
    }
  }

  public record AvailableFilesResponse(
      List<String> files,
      int totalFiles,
      String directory
  ) {
    public boolean hasFiles() {
      return totalFiles > 0;
    }
    
    public String getSummary() {
      return String.format("Found %d RDF files in %s", totalFiles, directory);
    }
  }

  public record AllFilesValidationResponse(
      List<ShaclDiagnosticService.ValidationDiagnostics> results,
      int totalFiles,
      int validFiles,
      int invalidFiles,
      int totalViolations,
      boolean strictMode
  ) {
    public String getStatus() {
      return invalidFiles == 0 ? "ALL_VALID" : "SOME_INVALID";
    }
    
    public String getMode() {
      return strictMode ? "STRICT" : "STANDARD";
    }
    
    public boolean allValid() {
      return invalidFiles == 0;
    }
    
    public String getSummary() {
      if (totalFiles == 0) {
        return "No files found for validation";
      }
      return String.format("%d/%d files valid, %d total violations", 
          validFiles, totalFiles, totalViolations);
    }
  }

  public record FolderValidationResponse(
      String folderPath,
      long durationMs,
      boolean strictMode,
      int totalFiles,
      int validFiles,
      int invalidFiles,
      int totalViolations,
      String summary,
      List<SimpleFileResult> files
  ) {}

  public record SimpleFileResult(
      String fileName,
      long durationMs,
      boolean strictMode,
      int violationCount,
      String summary
  ) {}
}