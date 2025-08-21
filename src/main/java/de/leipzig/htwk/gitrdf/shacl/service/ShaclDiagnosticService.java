package de.leipzig.htwk.gitrdf.shacl.service;

import static org.eclipse.rdf4j.model.vocabulary.RDF4J.SHACL_SHAPE_GRAPH;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.eclipse.rdf4j.sail.shacl.ShaclSailValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.shacl.config.ValidationConfig;

@Service
public class ShaclDiagnosticService {

    private static final Logger logger = LoggerFactory.getLogger(ShaclDiagnosticService.class);

    @Autowired
    private UriValidationService uriValidator;
    @Autowired
    private ValidationConfig config;

    /**
     * Validate that the requested shape version exists
     */
    private void validateShapeVersion(String shapeVersion) {
        if (shapeVersion == null || shapeVersion.trim().isEmpty()) {
            throw new IllegalArgumentException("Shape version cannot be null or empty");
        }
        
        // Check if at least one shape file exists for this version
        String testShapePath = "shapes/" + shapeVersion + "/platform.ttl";
        try (InputStream testStream = getClass().getClassLoader().getResourceAsStream(testShapePath)) {
            if (testStream == null) {
                throw new IllegalArgumentException("Invalid shape version '" + shapeVersion + "'. Shape directory 'shapes/" + shapeVersion + "/' does not exist or contains no shape files.");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Error validating shape version '" + shapeVersion + "': " + e.getMessage());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                throw e;
            }
            throw new IllegalArgumentException("Error validating shape version '" + shapeVersion + "': " + e.getMessage());
        }
    }

    public ValidationDiagnostics validateRdfFile(String fileName, boolean strictMode) {
        return validateRdfFile(fileName, strictMode, "v2");
    }

    public ValidationDiagnostics validateRdfFile(String fileName, boolean strictMode, String shapeVersion) {
        logger.info("🔍 Validating RDF file: {} (strict: {}, shape: {})", fileName, strictMode, shapeVersion);
        
        // Validate shape version first
        validateShapeVersion(shapeVersion);
        
        long startTime = System.currentTimeMillis();
        List<ViolationDetail> violations = new ArrayList<>();
        boolean isValid = false;
        String errorMessage = null;
        String validationReport = null;

        ShaclSail sail = createShaclSail();
        SailRepository repo = new SailRepository(sail);
        repo.init();

        try (RepositoryConnection conn = repo.getConnection()) {
            conn.begin();

            // Load shapes
            loadShapes(conn, strictMode, shapeVersion);

            // Load RDF data file from /local_data directory
            File rdfFile = findRdfFileInLocalData(fileName);
            if (rdfFile == null) {
                throw new IllegalArgumentException("RDF file not found in /local_data: " + fileName);
            }

            try (InputStream rdfStream = new FileInputStream(rdfFile)) {
                InputStream fixedStream = uriValidator.fixUriEncoding(rdfStream, fileName);
                logger.debug("🔄 Adding RDF data for file: {} from {}", fileName, rdfFile.getAbsolutePath());
                conn.add(fixedStream, RDFFormat.TURTLE);
                logger.debug("✓ RDF data added, now committing...");
            }

            logger.debug("🔄 Committing transaction for file: {}", fileName);
            conn.commit();
            logger.debug("Transaction committed successfully for file: {}", fileName);
            
            // If we reach here, validation passed
            isValid = true;
            logger.info("Validation passed for file: {}", fileName);

        } catch (ShaclSailValidationException e) {
            isValid = false;
            violations = extractViolationDetails(e);
            validationReport = getValidationReport(e);
            errorMessage = "SHACL validation failed with " + violations.size() + " violations";
            
            logger.warn(" Validation failed for file: {} with {} violations", fileName, violations.size());
            logViolationsToConsole(fileName, violations);

        } catch (Exception e) {
            isValid = false;
            
            logger.debug("🔍 Exception caught: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            
            // Check if the exception contains a ShaclSailValidationException
            ShaclSailValidationException shaclException = findShaclException(e);
            if (shaclException != null) {
                logger.info("🔍 Found wrapped ShaclSailValidationException, extracting violations...");
                violations = extractViolationDetails(shaclException);
                validationReport = getValidationReport(shaclException);
                errorMessage = "SHACL validation failed with " + violations.size() + " violations";
                logViolationsToConsole(fileName, violations);
            } else {
                errorMessage = "Validation error: " + e.getMessage();
                logger.error("💥 Validation error for file: {} - Exception type: {}", fileName, e.getClass().getSimpleName(), e);
            }
            
        } finally {
            // Critical: Properly cleanup both repository and sail to prevent memory leaks
            cleanupResources(repo, sail, fileName);
        }

        long duration = System.currentTimeMillis() - startTime;
        
        return new ValidationDiagnostics(
            fileName,
            isValid,
            violations,
            duration,
            strictMode,
            errorMessage,
            validationReport
        );
    }

    public List<ValidationDiagnostics> validateAllRdfFiles(boolean strictMode) {
        return validateAllRdfFiles(strictMode, "v2");
    }

    public List<ValidationDiagnostics> validateAllRdfFiles(boolean strictMode, String shapeVersion) {
        logger.info("🔍 Validating all RDF files in /local_data (strict: {}, shape: {})", strictMode, shapeVersion);
        
        // Validate shape version first
        validateShapeVersion(shapeVersion);
        
        List<ValidationDiagnostics> results = new ArrayList<>();
        List<String> rdfFiles = discoverRdfFilesInLocalData();
        
        if (rdfFiles.isEmpty()) {
            logger.warn("⚠️ No RDF files found in /local_data directory");
            return results;
        }
        
        logger.info("📁 Found {} RDF files in /local_data: {}", rdfFiles.size(), rdfFiles);
        
        for (String fileName : rdfFiles) {
            try {
                ValidationDiagnostics result = validateRdfFile(fileName, strictMode, shapeVersion);
                results.add(result);
            } catch (Exception e) {
                logger.error("Failed to validate file: {}", fileName, e);
                results.add(new ValidationDiagnostics(
                    fileName, 
                    false, 
                    List.of(), 
                    0, 
                    strictMode, 
                    "Failed to process file: " + e.getMessage(),
                    null
                ));
            }
        }
        
        return results;
    }

    private ShaclSail createShaclSail() {
        // Create optimized MemoryStore for large datasets
        MemoryStore memoryStore = new MemoryStore();
        memoryStore.setSyncDelay(0); // Immediate sync for faster processing
        
        ShaclSail sail = new ShaclSail(memoryStore);
        // High-performance settings for large RDF files (1-2GB) with memory management
        sail.setParallelValidation(true);   // ENABLE parallel processing for speed
        sail.setLogValidationPlans(false);  // Disable logging to reduce memory usage
        sail.setLogValidationViolations(false);  // Disable logging to reduce memory usage
        sail.setCacheSelectNodes(false);   // CRITICAL: Disable caching to prevent memory retention
        sail.setGlobalLogValidationExecution(false);  // Disable to reduce memory usage
        sail.setEclipseRdf4jShaclExtensions(false);
        sail.setSerializableValidation(false);  // Disable for memory efficiency
        sail.setPerformanceLogging(false); // Disable performance logging to save memory
        return sail;
    }

    private void loadShapes(RepositoryConnection conn, boolean strictMode, String shapeVersion) throws Exception {
        String[] shapeFiles = getShapeFiles(strictMode, shapeVersion);
        logger.info("📋 Loading {} shape files (strict: {}, shape: {}): {}", shapeFiles.length, strictMode, shapeVersion,
                String.join(", ", shapeFiles));

        for (String file : shapeFiles) {
            InputStream stream = null;
            InputStream fixed = null;
            try {
                stream = getClass().getClassLoader().getResourceAsStream(file);
                if (stream != null) {
                    fixed = uriValidator.fixUriEncoding(stream, file);
                    conn.add(fixed, file, RDFFormat.TURTLE, SHACL_SHAPE_GRAPH);
                    logger.debug("✓ Loaded shape file: {}", file);
                } else {
                    logger.warn("⚠️ Shape file not found: {}", file);
                }
            } finally {
                // Ensure shape file streams are properly closed
                if (fixed != null && fixed != stream) {
                    try {
                        fixed.close();
                    } catch (Exception e) {
                        logger.debug("Warning: Could not close fixed shape stream for {}: {}", file, e.getMessage());
                    }
                }
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (Exception e) {
                        logger.debug("Warning: Could not close shape stream for {}: {}", file, e.getMessage());
                    }
                }
            }
        }
    }

    private String[] getShapeFiles(boolean strictMode, String shapeVersion) {
        List<String> files = new ArrayList<>();

        if (config.isEnablePlatformValidation()) {
            files.add("shapes/" + shapeVersion + "/platform.ttl");
        }
        if (config.isEnableGitValidation()) {
            files.add("shapes/" + shapeVersion + "/git.ttl");
        }
        if (config.isEnableGithubValidation()) {
            files.add("shapes/" + shapeVersion + "/github.ttl");
        }
        if (config.isEnableAnalysisValidation()) {
            files.add("shapes/" + shapeVersion + "/analysis.ttl");
        }
        if (strictMode) {
            files.add("shapes/" + shapeVersion + "/strict-validations.ttl");
        }

        return files.toArray(new String[0]);
    }

    /**
     * Find an RDF file in the /local_data directory by searching recursively
     */
    private File findRdfFileInLocalData(String fileName) {
        Path localDataPath = Paths.get("/local_data");
        
        if (!Files.exists(localDataPath)) {
            logger.warn("⚠️ /local_data directory does not exist");
            return null;
        }
        
        // Handle both simple filenames and relative paths like "analysis/statistics/file.ttl"
        Path targetFile = localDataPath.resolve(fileName);
        if (Files.exists(targetFile) && Files.isRegularFile(targetFile)) {
            return targetFile.toFile();
        }
        
        // Fallback: search for just the filename if direct path doesn't work
        String justFileName = Paths.get(fileName).getFileName().toString();
        try (Stream<Path> paths = Files.walk(localDataPath)) {
            return paths
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().equals(justFileName))
                .filter(path -> path.toString().toLowerCase().endsWith(".ttl") || path.toString().toLowerCase().endsWith(".rdf"))
                .map(Path::toFile)
                .findFirst()
                .orElse(null);
        } catch (IOException e) {
            logger.error("Error searching for RDF file {} in /local_data", fileName, e);
            return null;
        }
    }

    /**
     * Discover all RDF (.ttl) files in the /local_data directory
     */
    private List<String> discoverRdfFilesInLocalData() {
        List<String> rdfFiles = new ArrayList<>();
        Path localDataPath = Paths.get("/local_data");
        
        if (!Files.exists(localDataPath)) {
            logger.warn("⚠️ /local_data directory does not exist");
            return rdfFiles;
        }
        
        try (Stream<Path> paths = Files.walk(localDataPath)) {
            paths
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().toLowerCase().endsWith(".ttl"))
                .map(path -> path.getFileName().toString())
                .forEach(rdfFiles::add);
        } catch (IOException e) {
            logger.error("Error discovering RDF files in /local_data", e);
        }
        
        return rdfFiles;
    }

    /**
     * Public method to discover available RDF files without validation
     */
    public List<String> discoverAvailableRdfFiles() {
        return discoverRdfFilesInLocalData();
    }

    /**
     * Discover RDF files in a specific folder within /local_data (supports nested paths)
     */
    public List<String> discoverRdfFilesInFolder(String folderPath) {
        List<String> rdfFiles = new ArrayList<>();
        Path fullFolderPath = Paths.get("/local_data").resolve(folderPath);
        
        if (!Files.exists(fullFolderPath)) {
            logger.warn("⚠️ /local_data/{} directory does not exist", folderPath);
            return rdfFiles;
        }
        
        if (!Files.isDirectory(fullFolderPath)) {
            logger.warn("⚠️ /local_data/{} is not a directory", folderPath);
            return rdfFiles;
        }
        
        try (Stream<Path> paths = Files.walk(fullFolderPath)) {
            Path localDataPath = Paths.get("/local_data");
            paths
                .filter(Files::isRegularFile)
                .filter(path -> {
                    String fileName = path.toString().toLowerCase();
                    return fileName.endsWith(".ttl") || fileName.endsWith(".rdf");
                })
                .map(path -> localDataPath.relativize(path).toString().replace("\\", "/"))
                .forEach(rdfFiles::add);
        } catch (IOException e) {
            logger.error("Error discovering RDF files in /local_data/{}", folderPath, e);
        }
        
        logger.info("📁 Found {} RDF files in /local_data/{}", rdfFiles.size(), folderPath);
        return rdfFiles;
    }

    /**
     * Validate all RDF files in a specific folder within /local_data (supports nested paths)
     */
    public List<ValidationDiagnostics> validateRdfFilesInFolder(String folderPath, boolean strictMode) {
        return validateRdfFilesInFolder(folderPath, strictMode, "v2");
    }

    public List<ValidationDiagnostics> validateRdfFilesInFolder(String folderPath, boolean strictMode, String shapeVersion) {
        logger.info("🔍 Validating all RDF files in /local_data/{} (strict: {}, shape: {})", folderPath, strictMode, shapeVersion);
        
        // Validate shape version first
        validateShapeVersion(shapeVersion);
        
        List<String> rdfFiles = discoverRdfFilesInFolder(folderPath);
        List<ValidationDiagnostics> results = new ArrayList<>();
        
        for (String fileName : rdfFiles) {
            try {
                ValidationDiagnostics result = validateRdfFile(fileName, strictMode, shapeVersion);
                results.add(result);
                String icon = result.isValid() ? "✅" : "";
                logger.info("{} Validated: {} - {}", icon, fileName, result.getStatus());
            } catch (Exception e) {
                logger.error(" Failed to validate: {}", fileName, e);
                ValidationDiagnostics errorResult = new ValidationDiagnostics(
                    fileName, false, List.of(), 0, strictMode, e.getMessage(), null
                );
                results.add(errorResult);
            }
        }
        
        long validCount = results.stream().mapToLong(r -> r.isValid() ? 1 : 0).sum();
        logger.info("📊 Folder validation complete: {}/{} files valid in /local_data/{}", 
                   validCount, results.size(), folderPath);
        
        return results;
    }

    public List<ViolationDetail> extractViolationDetails(ShaclSailValidationException e) {
        List<ViolationDetail> violations = new ArrayList<>();
        Model report = e.validationReportAsModel();

        // Get all violation results
        for (Value result : report.filter(null, SHACL.RESULT, null).objects()) {
            if (result instanceof Resource resultResource) {
                ViolationDetail violation = extractSingleViolation(report, resultResource);
                violations.add(violation);
            }
        }

        return violations;
    }

    private ViolationDetail extractSingleViolation(Model report, Resource result) {
        String message = getStringValue(report, result, SHACL.RESULT_MESSAGE);
        String focusNode = getStringValue(report, result, SHACL.FOCUS_NODE);
        String resultPath = getStringValue(report, result, SHACL.RESULT_PATH);
        String severity = getStringValue(report, result, SHACL.RESULT_SEVERITY);
        String sourceShape = getStringValue(report, result, SHACL.SOURCE_SHAPE);
        String sourceConstraintComponent = getStringValue(report, result, SHACL.SOURCE_CONSTRAINT_COMPONENT);
        String value = getStringValue(report, result, SHACL.VALUE);

        return new ViolationDetail(
            message,
            focusNode,
            resultPath,
            severity,
            sourceShape,
            sourceConstraintComponent,
            value
        );
    }

    private String getStringValue(Model model, Resource subject, IRI predicate) {
        for (Statement stmt : model.filter(subject, predicate, null)) {
            Value obj = stmt.getObject();
            if (obj instanceof Literal literal) {
                return literal.getLabel();
            } else if (obj instanceof IRI iri) {
                return iri.toString();
            } else if (obj instanceof Resource resource) {
                return resource.toString();
            }
        }
        return null;
    }

    public String getValidationReport(ShaclSailValidationException e) {
        try {
            Model report = e.validationReportAsModel();
            StringWriter writer = new StringWriter();
            Rio.write(report, writer, RDFFormat.TURTLE);
            return writer.toString();
        } catch (Exception ex) {
            logger.warn("Could not serialize validation report", ex);
            return null;
        }
    }

    public void logViolationsToConsole(String fileName, List<ViolationDetail> violations) {
        logger.info("📋 SHACL Violations for file: {}", fileName);
        logger.info("═══════════════════════════════════════════════════════════");
        
        for (int i = 0; i < violations.size(); i++) {
            ViolationDetail violation = violations.get(i);
            logger.info("🚫 Violation #{}: {}", i + 1, violation.message());
            logger.info("   Focus Node: {}", violation.focusNode());
            logger.info("   Path: {}", violation.resultPath());
            logger.info("   Value: {}", violation.value());
            logger.info("   Severity: {}", violation.severity());
            logger.info("   Source Shape: {}", violation.sourceShape());
            logger.info("   Constraint: {}", violation.sourceConstraintComponent());
            logger.info("───────────────────────────────────────────────────────────");
        }
        
        logger.info("📊 Total violations: {}", violations.size());
        logger.info("═══════════════════════════════════════════════════════════");
    }

    private ShaclSailValidationException findShaclException(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        
        if (throwable instanceof ShaclSailValidationException) {
            return (ShaclSailValidationException) throwable;
        }
        
        // Check the cause recursively
        return findShaclException(throwable.getCause());
    }

    /**
     * Aggressive cleanup of RDF4J resources for large datasets (1-2GB RDF files)
     * This mirrors the aggressive cleanup method in ShaclValidationEngine
     */
    private void cleanupResources(SailRepository repo, ShaclSail sail, String identifier) {
        try {
            logger.info("🧹 Starting aggressive cleanup for large dataset validation: {}", identifier);
            
            // Step 1: Force close all connections first
            if (repo != null && repo.isInitialized()) {
                try {
                    logger.debug("🧹 Forcing connection closure for: {}", identifier);
                    repo.shutDown();
                } catch (Exception e) {
                    logger.warn("⚠️ Error shutting down repository for {}: {}", identifier, e.getMessage());
                }
            }

            // Step 2: Shutdown the SHACL sail and underlying MemoryStore
            if (sail != null) {
                try {
                    logger.debug("🧹 Shutting down SHACL sail for: {}", identifier);
                    sail.shutDown();
                    
                    // Additional cleanup for MemoryStore if accessible
                    if (sail.getBaseSail() instanceof MemoryStore memoryStore) {
                        logger.debug("🧹 Shutting down underlying MemoryStore for: {}", identifier);
                        memoryStore.shutDown();
                    }
                } catch (Exception e) {
                    logger.warn("⚠️ Error shutting down sail for {}: {}", identifier, e.getMessage());
                }
            }

            // Step 3: Clear references and suggest GC for large datasets
            repo = null;
            sail = null;
            
            // For large datasets (1-2GB), suggest GC to free memory immediately
            logger.debug("🧹 Suggesting garbage collection for large dataset: {}", identifier);
            System.gc();
            
            // Give GC a moment to work
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            logger.info("Aggressive cleanup completed for: {}", identifier);
            
        } catch (Exception e) {
            logger.warn("⚠️ Error during aggressive cleanup for {}: {}", identifier, e.getMessage());
            // Don't rethrow - cleanup errors shouldn't fail the validation
        }
    }

    // DTOs
    public record ValidationDiagnostics(
        String fileName,
        boolean isValid,
        List<ViolationDetail> violations,
        long durationMs,
        boolean strictMode,
        String errorMessage,
        String validationReport
    ) {
        public String getStatus() {
            return isValid ? "VALID" : "INVALID";
        }
        
        public int getViolationCount() {
            return violations.size();
        }
        
        public String getSummary() {
            if (isValid) {
                return String.format("%s is valid (took %dms)", fileName, durationMs);
            } else {
                return String.format(" %s has %d violations (took %dms)", fileName, violations.size(), durationMs);
            }
        }
    }

    public record ViolationDetail(
        String message,
        String focusNode,
        String resultPath,
        String severity,
        String sourceShape,
        String sourceConstraintComponent,
        String value
    ) {
        public String getDisplayMessage() {
            StringBuilder sb = new StringBuilder();
            if (message != null) {
                sb.append(message);
            }
            if (focusNode != null) {
                sb.append(" (Focus: ").append(focusNode).append(")");
            }
            if (resultPath != null) {
                sb.append(" (Path: ").append(resultPath).append(")");
            }
            return sb.toString();
        }
    }
}