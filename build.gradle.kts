/*
 * LOCKSS Core
 *
 * The Slimmed Down LOCKSS Daemon Core.
 */

plugins {
    id("lockss-java-conventions")
}

group = "org.lockss.laaws"
version = "2.10.0-SNAPSHOT"
description = "The Slimmed Down LOCKSS Daemon Core"

// Export test JAR for other projects
val publishTestJar: Boolean by extra(true)

// Configuration for javax dependencies that need to be transformed to jakarta
val javaxDeps by configurations.creating

// Configuration for the Eclipse Transformer CLI
val transformerCli by configurations.creating

// Output directory for transformed jars
val transformedDepsDir = layout.buildDirectory.dir("transformed-deps")

// Task to transform javax jars to jakarta namespace
val transformJavaxDeps by tasks.registering {
    inputs.files(javaxDeps)
    outputs.dir(transformedDepsDir)

    doLast {
        val outDir = transformedDepsDir.get().asFile
        outDir.mkdirs()

        javaxDeps.resolvedConfiguration.resolvedArtifacts.forEach { artifact ->
            val inputFile = artifact.file
            val outputFile = File(outDir, inputFile.name.replace(".jar", "-jakarta.jar"))
            if (!outputFile.exists() || inputFile.lastModified() > outputFile.lastModified()) {
                logger.lifecycle("Transforming ${inputFile.name} to Jakarta namespace...")
                project.javaexec {
                    classpath(transformerCli)
                    mainClass.set("org.eclipse.transformer.cli.JakartaTransformerCLI")
                    // JakartaTransformerCLI uses Jakarta rules by default
                    args(inputFile.absolutePath, outputFile.absolutePath, "-o")
                }
            }
        }
    }
}

// Make compileJava depend on the transformation task
tasks.named("compileJava") {
    dependsOn(transformJavaxDeps)
}

dependencies {
    // Eclipse Transformer CLI for javax to jakarta conversion
    transformerCli("org.eclipse.transformer:org.eclipse.transformer.cli:0.5.0")
    transformerCli("org.eclipse.transformer:org.eclipse.transformer.jakarta:0.5.0")

    // Javax deps to be transformed (resolved separately)
    javaxDeps(libs.lockss.legacy.org.mortbay.jetty)
    javaxDeps(libs.lockss.legacy.dk.digst.oiosaml.java)

    // Add transformed deps to API classpath
    api(fileTree(transformedDepsDir) { include("*.jar") })

    // Internal dependencies - lockss-util should be first for logging precedence
    api(project(":lockss-util:lockss-util-core"))
    api(project(":lockss-util:lockss-util-entities"))
    api(project(":lockss-util:lockss-util-rest"))

    // Derby
    api(libs.bundles.derby)

    // Bouncy Castle
    api(libs.bundles.bouncycastle)

    // TrueZip
    api(libs.bundles.truezip)

    // GraalVM (for JavaScript engine)
    api(libs.bundles.graalvm)
    api("org.graalvm.polyglot:js-community:23.1.2")

    // ActiveMQ
    api(libs.bundles.activemq)
    api(libs.activemq.kahadb.store)

    // ICU4J
    api(libs.bundles.icu4j)

    // Apache Solr
    api(libs.solr.solrj)

    // PDFBox - both legacy LOCKSS version and Apache version
    api(libs.lockss.pdfbox)
    api(libs.pdfbox)

    // Commons libraries
    api(libs.commons.csv)
    api(libs.commons.text)
    api(libs.commons.collections)
    api(libs.commons.collections4)
    api(libs.commons.configuration)
    api(libs.commons.io)
    api(libs.commons.lang)
    api(libs.commons.lang3)
    api(libs.commons.logging)
    api(libs.commons.beanutils)
    api(libs.commons.compress)
    api(libs.commons.validator)
    api(libs.commons.dbcp2)
    api(libs.commons.jxpath)
    api(libs.commons.digester)
    api(libs.commons.primitives)

    // JSoup
    api(libs.jsoup)

    // Jackson
    api(libs.jackson.databind)
    api(libs.jackson.core)
    api(libs.jackson.datatype.jsr310)

    // XML
    api(libs.xerces)
    api(libs.xalan)
    api(libs.xalan.serializer)

    // XStream
    api(libs.xstream)

    // Velocity
    api(libs.velocity)

    // JMS
    api(libs.javax.jms.api)

    // Jetty
    api(libs.jetty.util)
    api(libs.jetty.io)
    api(libs.jetty.http)

    // CXF 4.x (SOAP) - jakarta namespace for Spring Boot 3.x
    api(libs.cxf.jakarta.rt.frontend.jaxws)
    api(libs.cxf.jakarta.rt.transports.http)

    // JAX-WS and JAX-RS APIs
    api(libs.javax.jws.api)
    api(libs.jaxws.api)
    api(libs.javax.ws.rs.api)

    // JSON (org.json and json-path)
    api(libs.json.org)
    api(libs.json.path)

    // Lucene (for text analysis)
    api(libs.lucene.analyzers.common)

    // Database drivers
    api(libs.postgresql)
    api(libs.mysql.connector.j)

    // Rhino JavaScript
    api(libs.rhino)

    // Castor XML
    api(libs.castor)

    // JAF/Activation
    api(libs.javax.activation)

    // Servlet API (jakarta namespace for Spring Boot 3.x)
    api(libs.jakarta.servlet.api)

    // Webarchive Commons
    api(libs.lockss.legacy.org.netpreserve.commons.webarchive.commons)
    api(libs.jwat.warc)

    // XOAI (OAI-PMH)
    api(libs.xoai.common)
    api(libs.lockss.xoai.service.provider)

    // HTML Parser
    api(libs.lockss.legacy.org.htmlparser)

    // Jimi
    api(libs.lockss.legacy.com.sun.jimi.pro)

    // RDF API
    api(libs.lockss.legacy.edu.stanford.db.rdf.api)

    // JoSQL
    api(libs.lockss.legacy.org.josql)
    api(libs.lockss.legacy.com.gentlyweb.utils)

    // OpenSAML
    api(libs.opensaml)
    api(libs.xmltooling)

    // NekoHTML
    api(libs.nekohtml)

    // Ziplet
    api(libs.ziplet)

    // JCabi
    api(libs.jcabi.aspects)

    // JRugged
    api(libs.jrugged.core)

    // Joda Time
    api(libs.joda.time)

    // Oro
    api(libs.oro)

    // Concurrent
    api(libs.concurrent)

    // Javatar
    api(libs.javatar)

    // Gettext
    api(libs.gettext.commons)
    api(libs.ant.gettext)

    // HTTP Client
    api(libs.httpcore)
    api(libs.httpclient.cache)

    // AspectJ
    api(libs.aspectjrt)

    // Stax2
    api(libs.stax2.api)

    // Fastutil
    api(libs.fastutil)

    // ESAPI
    api(libs.esapi)

    // IPAddress
    api(libs.ipaddress)

    // HTTPUnit (test)
    testImplementation(libs.httpunit)

    // Test dependencies
    testImplementation(project(":lockss-util:lockss-util-core", configuration = "testArtifacts"))
    testImplementation(project(":lockss-util:lockss-util-rest", configuration = "testArtifacts"))
    testImplementation(platform(project(":lockss-pom-bundles:lockss-junit5-bundle")))
    testImplementation(libs.junit.jupiter.engine)
    testImplementation(libs.mockito.core)
    testImplementation(libs.solr.test.framework)
    testImplementation(libs.embedded.postgres)
    testImplementation(libs.mockserver.netty)
    testImplementation(libs.jsonassert)
    testImplementation(libs.xmlunit.core)
}

// Test artifacts configuration is provided by lockss-java-conventions plugin
