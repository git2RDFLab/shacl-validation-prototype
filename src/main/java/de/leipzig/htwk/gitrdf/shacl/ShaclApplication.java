package de.leipzig.htwk.gitrdf.shacl;

import org.apache.jena.shared.impl.JenaParameters;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(
    scanBasePackages = {
        "de.leipzig.htwk.gitrdf.shacl", // This package and all sub-packages
        "de.leipzig.htwk.gitrdf.database.common.entity",
        "de.leipzig.htwk.gitrdf.database.common.repository"
    }
)
@EntityScan(basePackages = "de.leipzig.htwk.gitrdf.database.common.entity")
@EnableJpaRepositories(basePackages = "de.leipzig.htwk.gitrdf.database.common.repository")
public class ShaclApplication {
    public static void main(String[] args) {

        JenaParameters.enableEagerLiteralValidation = true;
        JenaParameters.enableSilentAcceptanceOfUnknownDatatypes = false;

        SpringApplication.run(ShaclApplication.class, args);
    }
}