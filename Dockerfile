FROM amazoncorretto:21.0.2-alpine3.19

# Copy the application jar
COPY target/*.jar shacl-app.jar

# Expose the application port
EXPOSE 8083

# Set default JVM options for large RDF processing
# These can be overridden with docker run -e JAVA_OPTS="..."
ENV JAVA_OPTS="-Xmx8g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=32m -XX:+UseStringDeduplication -XX:MaxMetaspaceSize=512m"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8083/api/validation/health || exit 1

# Run the application with JVM options
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /shacl-app.jar"]