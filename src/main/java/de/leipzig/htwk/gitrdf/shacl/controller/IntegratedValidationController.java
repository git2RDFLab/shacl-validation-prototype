package de.leipzig.htwk.gitrdf.shacl.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import de.leipzig.htwk.gitrdf.shacl.core.EnhancedValidationEngine;
import de.leipzig.htwk.gitrdf.shacl.model.ShapeDefinition;
import de.leipzig.htwk.gitrdf.shacl.model.ValidationResult;
import de.leipzig.htwk.gitrdf.shacl.service.ShaclDiagnosticService;
import de.leipzig.htwk.gitrdf.shacl.service.ShaclValidationService;

/**
 * Integrated validation controller that supports both legacy version-based validation
 * and new JSON-defined remote shape validation
 */
@RestController
@RequestMapping("/api/integrated-validation")
public class IntegratedValidationController {
    
    private static final Logger logger = LoggerFactory.getLogger(IntegratedValidationController.class);
    
    @Autowired
    private ShaclValidationService legacyValidationService;
    
    @Autowired
    private EnhancedValidationEngine enhancedValidationEngine;
    
    /**
     * Validate order using JSON-defined remote shapes (NEW APPROACH)
     */
    @PostMapping("/order/{orderId}")
    public ResponseEntity<IntegratedValidationResponse> validateOrderWithRemoteShapes(
            @PathVariable String orderId,
            @RequestBody ShapeDefinition shapeDefinition,
            @RequestParam(defaultValue = "false") boolean strict) {
        
        logger.info("🔍 [NEW] Integrated validation for order {} with {} remote shapes (strict: {})", 
            orderId, shapeDefinition.getShapeCount(), strict);
        
        try {
            // Validate input
            if (!shapeDefinition.hasShapes()) {
                throw new IllegalArgumentException("Shape definition must contain at least one shape URL");
            }
            
            // Use enhanced validation engine with remote shapes
            ShaclDiagnosticService.ValidationDiagnostics diagnostics = 
                enhancedValidationEngine.validateWithRemoteShapes(orderId, 
                    legacyValidationService.getGithubOrderRdfStream(orderId), strict, shapeDefinition);
            
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                orderId,
                "GitHub",
                diagnostics.isValid(),
                diagnostics.durationMs(),
                strict,
                "remote_shapes",
                shapeDefinition.getShapeCount(),
                diagnostics.errorMessage(),
                diagnostics.getViolationCount(),
                diagnostics.isValid() ? "VALID" : " INVALID",
                shapeDefinition.getShapes().keySet().toArray(new String[0])
            );
            
            return diagnostics.isValid() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
            
        } catch (IllegalArgumentException e) {
            logger.error("Invalid request for order: {} - {}", orderId, e.getMessage());
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                orderId, "GitHub", false, 0, strict, "remote_shapes", 0,
                "Invalid request: " + e.getMessage(), 0, "[INVALID_REQUEST]", new String[0]
            );
            return ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            logger.error("Integrated validation failed: {} (strict: {})", orderId, strict, e);
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                orderId, "GitHub", false, 0, strict, "remote_shapes", 0,
                e.getMessage(), 0, "[VALIDATION_FAILED]", new String[0]
            );
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Validate order using legacy version tag (LEGACY APPROACH - marked as deprecated)
     */
    @GetMapping("/order/{orderId}")
    @Deprecated
    public ResponseEntity<IntegratedValidationResponse> validateOrderWithLegacyShapes(
            @PathVariable String orderId,
            @RequestParam(defaultValue = "false") boolean strict,
            @RequestParam(defaultValue = "v2") String shape) {
        
        logger.info("🔍 [LEGACY] Integrated validation for order {} with version tag '{}' (strict: {})", 
            orderId, shape, strict);
        
        try {
            // Use legacy validation service
            ValidationResult result = legacyValidationService.validateGithubOrderWithDiagnostics(orderId, strict, shape);
            
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                result.getOrderId(),
                result.getPlatform(),
                result.isValid(),
                result.getDurationMs(),
                result.isStrictMode(),
                "version_tag",
                0, // No shape count for legacy
                result.getErrorMessage(),
                result.getViolationCount(),
                result.getSummary(),
                new String[]{shape} // Legacy shape version
            );
            
            return result.isValid() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
            
        } catch (IllegalArgumentException e) {
            logger.error("Invalid legacy request for order: {} - {}", orderId, e.getMessage());
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                orderId, "GitHub", false, 0, strict, "version_tag", 0,
                "Invalid request: " + e.getMessage(), 0, "[INVALID_REQUEST]", new String[]{shape}
            );
            return ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            logger.error("Legacy validation failed: {} (strict: {})", orderId, strict, e);
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                orderId, "GitHub", false, 0, strict, "version_tag", 0,
                e.getMessage(), 0, "[VALIDATION_FAILED]", new String[]{shape}
            );
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Validate local file using JSON-defined remote shapes (NEW APPROACH)
     */
    @PostMapping("/local/file")
    public ResponseEntity<IntegratedValidationResponse> validateLocalFileWithRemoteShapes(
            @RequestParam String file,
            @RequestBody ShapeDefinition shapeDefinition,
            @RequestParam(defaultValue = "false") boolean strict) {
        
        logger.info("🔍 [NEW] Integrated local validation for file {} with {} remote shapes (strict: {})", 
            file, shapeDefinition.getShapeCount(), strict);
        
        try {
            // Validate input
            if (!shapeDefinition.hasShapes()) {
                throw new IllegalArgumentException("Shape definition must contain at least one shape URL");
            }
            
            // Use enhanced validation engine with remote shapes
            // Note: This would need a method to get local file stream - extending for demonstration
            ShaclDiagnosticService.ValidationDiagnostics diagnostics = 
                enhancedValidationEngine.validateWithRemoteShapes(file, 
                    getLocalFileStream(file), strict, shapeDefinition);
            
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                file,
                "Local",
                diagnostics.isValid(),
                diagnostics.durationMs(),
                strict,
                "remote_shapes",
                shapeDefinition.getShapeCount(),
                diagnostics.errorMessage(),
                diagnostics.getViolationCount(),
                diagnostics.isValid() ? "VALID" : " INVALID",
                shapeDefinition.getShapes().keySet().toArray(new String[0])
            );
            
            return diagnostics.isValid() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
            
        } catch (IllegalArgumentException e) {
            logger.error("Invalid request for file: {} - {}", file, e.getMessage());
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                file, "Local", false, 0, strict, "remote_shapes", 0,
                "Invalid request: " + e.getMessage(), 0, "[INVALID_REQUEST]", new String[0]
            );
            return ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            logger.error("Integrated local validation failed: {} (strict: {})", file, strict, e);
            IntegratedValidationResponse response = new IntegratedValidationResponse(
                file, "Local", false, 0, strict, "remote_shapes", 0,
                e.getMessage(), 0, "[VALIDATION_FAILED]", new String[0]
            );
            return ResponseEntity.status(500).body(response);
        }
    }
    
    // Placeholder method - would need to be implemented to get local file streams
    private java.io.InputStream getLocalFileStream(String fileName) throws Exception {
        // This would integrate with the existing local file handling logic
        throw new UnsupportedOperationException("Local file validation with remote shapes not yet implemented");
    }
    
    // ========== RESPONSE DTO ==========
    
    public record IntegratedValidationResponse(
        String identifier,
        String platform,
        boolean isValid,
        long durationMs,
        boolean strictMode,
        String validationType, // "remote_shapes" or "version_tag"
        int shapeCount,
        String errorMessage,
        int violationCount,
        String summary,
        String[] shapes // Shape names or version tags
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
        
        public boolean isLegacyValidation() {
            return "version_tag".equals(validationType);
        }
        
        public boolean isEnhancedValidation() {
            return "remote_shapes".equals(validationType);
        }
        
        public String getDisplayName() {
            return platform + " " + identifier;
        }
    }
}