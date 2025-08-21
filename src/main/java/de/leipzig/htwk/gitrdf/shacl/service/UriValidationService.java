package de.leipzig.htwk.gitrdf.shacl.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.shacl.config.ValidationConfig;

@Service
public class UriValidationService {

  private static final Logger logger = LoggerFactory.getLogger(UriValidationService.class);
  
  @Autowired
  private ValidationConfig config;

  // Pattern to match URIs in angle brackets with invalid characters
  private static final Pattern URI_PATTERN = Pattern.compile("<([^>]+)>");

  // Characters that need to be encoded in URIs for RDF compliance
  private static final Pattern INVALID_URI_CHARS = Pattern.compile("[\\[\\]\\s\\{\\}\\|\\\\\\^`]");

  /**
   * Gets the maximum content size from configuration
   */
  private long getMaxContentSize() {
    return (long) config.getMaxFileSizeMb() * 1024 * 1024;
  }

  /**
   * Fixes URI encoding issues in RDF content by decoding over-encoded URIs
   */
  public InputStream fixUriEncoding(InputStream originalStream, String fileName) throws Exception {
    // Check if URI fixing is enabled
    if (!config.isUriFixingEnabled()) {
      logger.debug("URI fixing disabled - returning original stream for file: {}", fileName);
      return originalStream;
    }
    
    logger.debug("Fixing URI encoding issues in file: {}", fileName);

    try {
      // Read the entire content with size limit
      StringBuilder content = new StringBuilder();
      long totalSize = 0;
      
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(originalStream, StandardCharsets.UTF_8))) {
        String line;
        int lineNumber = 0;
        while ((line = reader.readLine()) != null) {
          lineNumber++;
          
          // Check size limit to prevent OOM
          totalSize += line.length() + 1; // +1 for newline
          long maxSize = getMaxContentSize();
          if (totalSize > maxSize) {
            throw new IllegalArgumentException(
                String.format("File %s is too large (>%d MB). Size limit exceeded to prevent OutOfMemoryError.", 
                    fileName, config.getMaxFileSizeMb()));
          }
          
          String fixedLine = fixUrisInLine(line, lineNumber, fileName);
          content.append(fixedLine).append("\n");
        }
      }
      
      logger.debug("Processed {} bytes from file: {}", totalSize, fileName);

      // Skip global URL decoding to prevent RDF syntax errors
      // The individual URI fixing per line is sufficient
      String originalContent = content.toString();
      String fixedContent = originalContent; // No global decoding
      
      // Log the processing completion (individual line fixes were already logged)
      logger.debug("URI processing completed for file: {} ({} bytes processed)", fileName, totalSize);

      return new ByteArrayInputStream(fixedContent.getBytes(StandardCharsets.UTF_8));

    } catch (Exception e) {
      logger.error("Failed to fix URI encoding in file: {}", fileName, e);
      throw e;
    }
  }

  /**
   * Decodes common URL encodings that cause SHACL validation issues - memory efficient version
   */
  private String decodeCommonUrlEncodings(String content) {
    // Early return if no encoded characters found
    if (!content.contains("%")) {
      return content;
    }
    
    // Use StringBuilder for memory efficiency on large strings
    StringBuilder result = new StringBuilder(content.length());
    
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      
      if (c == '%' && i + 2 < content.length()) {
        String encoded = content.substring(i, i + 3).toUpperCase();
        String decoded = decodeUrlEncoding(encoded);
        
        if (decoded != null) {
          result.append(decoded);
          i += 2; // Skip the next 2 characters
        } else {
          result.append(c);
        }
      } else {
        result.append(c);
      }
    }
    
    return result.toString();
  }
  
  /**
   * Decodes a single URL encoding, but only safe characters that won't break RDF parsing
   */
  private String decodeUrlEncoding(String encoded) {
    return switch (encoded) {
      // DON'T decode brackets - they're invalid in IRIs and break RDF parsing
      // case "%5B" -> "[";  // Left bracket - DISABLED
      // case "%5D" -> "]";  // Right bracket - DISABLED
      case "%28" -> "(";  // Left parenthesis
      case "%29" -> ")";  // Right parenthesis
      // Be cautious with spaces in URIs
      // case "%20" -> " ";  // Space - DISABLED for URI safety
      case "%22" -> "\""; // Quote
      case "%27" -> "'";  // Apostrophe
      case "%26" -> "&";  // Ampersand
      case "%2B" -> "+";  // Plus
      case "%23" -> "#";  // Hash
      case "%3D" -> "=";  // Equal
      case "%3F" -> "?";  // Question mark
      // case "%2F" -> "/";  // Forward slash - can break URI structure
      // case "%3A" -> ":";  // Colon - can break URI structure
      default -> null;    // Don't decode unknown encodings
    };
  }

  /**
   * Fixes URIs in a single line of RDF content - conservative approach
   */
  private String fixUrisInLine(String line, int lineNumber, String fileName) {
    if (line.trim().isEmpty() || line.trim().startsWith("#")) {
      return line; // Skip empty lines and comments
    }

    // Only process lines that actually contain URI brackets (likely problematic)
    if (!line.contains("<") || !line.contains(">")) {
      return line; // No URIs to fix
    }

    Matcher matcher = URI_PATTERN.matcher(line);
    StringBuffer result = new StringBuffer();
    boolean lineChanged = false;

    while (matcher.find()) {
      String originalUri = matcher.group(1);
      
      // Only fix URIs that actually have problematic characters
      if (INVALID_URI_CHARS.matcher(originalUri).find()) {
        String fixedUri = fixUri(originalUri, lineNumber, fileName);
        
        if (!originalUri.equals(fixedUri)) {
          logger.debug("Fixed URI at line {}: '{}' -> '{}'", lineNumber, originalUri, fixedUri);
          matcher.appendReplacement(result, "<" + Matcher.quoteReplacement(fixedUri) + ">");
          lineChanged = true;
        } else {
          matcher.appendReplacement(result, "<" + Matcher.quoteReplacement(originalUri) + ">");
        }
      } else {
        // URI is already valid, keep as-is
        matcher.appendReplacement(result, "<" + Matcher.quoteReplacement(originalUri) + ">");
      }
    }
    matcher.appendTail(result);

    return lineChanged ? result.toString() : line;
  }

  /**
   * Fixes a single URI by handling encoding/decoding appropriately
   */
  private String fixUri(String uri, int lineNumber, String fileName) {
    if (uri == null || uri.isEmpty()) {
      return uri;
    }

    // Check if URI contains invalid characters that need encoding
    Matcher invalidCharMatcher = INVALID_URI_CHARS.matcher(uri);
    if (!invalidCharMatcher.find()) {
      return uri; // URI is already valid
    }

    try {
      // Special handling for GitHub URIs (including bot URIs)
      if (uri.contains("github.com")) {
        return fixGitHubBotUri(uri);
      }

      // General URI fixing for non-GitHub URIs
      return fixGeneralUri(uri);

    } catch (Exception e) {
      logger.warn("Could not fix URI '{}' at line {} in file {}: {}",
          uri, lineNumber, fileName, e.getMessage());
      return uri; // Return original if fixing fails
    }
  }

  /**
   * Special handling for GitHub bot URIs like dependabot[bot]
   * Keep brackets encoded to ensure valid RDF/Turtle syntax
   */
  private String fixGitHubBotUri(String uri) {
    String fixed = uri;
    
    // If URI has unencoded brackets, encode them for valid RDF
    if (uri.contains("[") || uri.contains("]")) {
      fixed = uri.replace("[", "%5B");
      fixed = fixed.replace("]", "%5D");
      logger.debug("Encoded GitHub bot URI brackets: '{}' -> '{}'", uri, fixed);
    } else if (uri.contains("%5B") || uri.contains("%5D")) {
      // Already encoded - keep as is
      logger.debug("GitHub bot URI already properly encoded: '{}'", uri);
    }

    return fixed;
  }

  /**
   * General URI fixing - ensure RDF/Turtle compliance by encoding invalid characters
   */
  private String fixGeneralUri(String uri) {
    // For GitHub URLs, use special handling
    if (uri.contains("github.com")) {
      return fixGitHubBotUri(uri);
    }
    
    // For other URIs, encode characters that break RDF parsing
    if (uri.startsWith("http://") || uri.startsWith("https://")) {
      String scheme = uri.substring(0, uri.indexOf("://") + 3);
      String rest = uri.substring(scheme.length());

      // Encode characters that would break RDF/Turtle parsing
      String fixedRest = rest;
      fixedRest = fixedRest.replace(" ", "%20");
      fixedRest = fixedRest.replace("[", "%5B");  // Encode brackets for RDF compliance
      fixedRest = fixedRest.replace("]", "%5D");  // Encode brackets for RDF compliance
      fixedRest = fixedRest.replace("{", "%7B");
      fixedRest = fixedRest.replace("}", "%7D");
      fixedRest = fixedRest.replace("|", "%7C");
      fixedRest = fixedRest.replace("\\", "%5C");
      fixedRest = fixedRest.replace("^", "%5E");
      fixedRest = fixedRest.replace("`", "%60");

      return scheme + fixedRest;
    } else {
      // For non-HTTP URIs, encode problematic characters
      String fixed = uri;
      fixed = fixed.replace(" ", "%20");
      fixed = fixed.replace("[", "%5B");
      fixed = fixed.replace("]", "%5D");
      return fixed;
    }
  }

  /**
   * Validates if a URI string is valid for RDF/Turtle parsing
   */
  public boolean isValidUri(String uri) {
    if (uri == null || uri.isEmpty()) {
      return false;
    }

    // Check for characters that would break RDF parsing
    boolean hasInvalidChars = INVALID_URI_CHARS.matcher(uri).find();
    
    return !hasInvalidChars;
  }

  /**
   * Counts URI validation issues in a file
   */
  public int countUriIssues(InputStream stream, String fileName) throws Exception {
    int issueCount = 0;

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String line;
      int lineNumber = 0;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        Matcher matcher = URI_PATTERN.matcher(line);

        while (matcher.find()) {
          String uri = matcher.group(1);
          if (!isValidUri(uri)) {
            issueCount++;
            logger.debug("URI issue found at line {} in {}: '{}'", lineNumber, fileName, uri);
          }
        }
      }
    }

    logger.info("Found {} URI validation issues in file: {}", issueCount, fileName);
    return issueCount;
  }

  /**
   * Counts the number of encoding changes made
   */
  private int countEncodingChanges(String original, String fixed) {
    int changes = 0;
    // Count bracket decodings
    changes += countOccurrences(original, "%5B") + countOccurrences(original, "%5b");
    changes += countOccurrences(original, "%5D") + countOccurrences(original, "%5d");
    changes += countOccurrences(original, "%28") + countOccurrences(original, "%29");
    changes += countOccurrences(original, "%20");
    return changes;
  }

  /**
   * Counts occurrences of a substring
   */
  private int countOccurrences(String text, String substring) {
    int count = 0;
    int index = 0;
    while ((index = text.indexOf(substring, index)) != -1) {
      count++;
      index += substring.length();
    }
    return count;
  }

  /**
   * Logs specific encoding changes for debugging
   */
  private void logEncodingChanges(String original, String fixed, String fileName) {
    logger.debug("=== URI Encoding Changes for {} ===", fileName);
    
    // Show bracket changes
    if (original.contains("%5B") || original.contains("%5b")) {
      logger.debug("  %5B -> [ (bracket decoding)");
    }
    if (original.contains("%5D") || original.contains("%5d")) {
      logger.debug("  %5D -> ] (bracket decoding)");
    }
    if (original.contains("%28")) {
      logger.debug("  %28 -> ( (parenthesis decoding)");
    }
    if (original.contains("%29")) {
      logger.debug("  %29 -> ) (parenthesis decoding)");
    }
    if (original.contains("%20")) {
      logger.debug("  %20 -> [space] (space decoding)");
    }
    
    logger.debug("=== End URI Encoding Changes ===");
  }
}