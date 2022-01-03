package codes.writeonce.maven.plugin.pkg;

import codes.writeonce.templates.ArrayDequePool;
import codes.writeonce.templates.TemplateParser;
import codes.writeonce.templates.Templates;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.maven.plugins.annotations.LifecyclePhase.PACKAGE;
import static org.apache.maven.plugins.annotations.ResolutionScope.COMPILE_PLUS_RUNTIME;

@Mojo(name = "package", requiresDependencyResolution = COMPILE_PLUS_RUNTIME, defaultPhase = PACKAGE, threadSafe = true)
public class PackageMojo extends AbstractMojo {

    private static final String MODULE_SOURCE = "<module source>";

    @Parameter
    protected String classifier;

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    @Parameter(required = true, readonly = true, defaultValue = "${project.build.directory}")
    private File buildDirectory;

    @Parameter(readonly = true, defaultValue = "${project.build.finalName}")
    private String finalName;

    @Parameter(required = true, defaultValue = "${basedir}/src/package")
    protected File sources;

    @Parameter
    private List<Attachment> attachments;

    @Component
    private MavenProjectHelper projectHelper;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            process();
        } catch (MojoExecutionException e) {
            throw e;
        } catch (Exception e) {
            getLog().error(e.getMessage());
            throw new MojoExecutionException("Failed to package: " + e.getMessage(), e);
        }
    }

    private void process() throws Exception {

        final Set<Artifact> artifacts = new TreeSet<>(project.getArtifacts());

        if (!project.getPackaging().equals("pom")) {
            final Artifact artifact = project.getArtifact();
            if (artifact != null) {
                artifacts.add(artifact);
            }
        }

        final Templates templates =
                new Templates(new ArrayDequePool<>(TemplateParser::new), name -> null, name -> null);
        final Set<String> names = new TreeSet<>();

        final Map<String, String> envPropertyArtifacts = new HashMap<>();
        final Map<String, String> systemPropertyArtifacts = new HashMap<>();
        final Map<String, String> attachmentPropertyArtifacts = new HashMap<>();
        final Properties envProperties = new Properties();
        final Properties systemProperties = new Properties();
        final Properties attachmentProperties = new Properties();
        ByteArrayOutputStream commandArguments = null;
        String commandArgumentsArtifact = null;

        final Path commandArgumentsPath = sources.toPath().resolve("commandArguments");
        if (Files.exists(commandArgumentsPath)) {
            commandArgumentsArtifact = MODULE_SOURCE;
            try (InputStream inputStream = Files.newInputStream(commandArgumentsPath)) {
                commandArguments = getCommandArguments(templates, names, inputStream);
            }
        }

        final Path envPropertiesPath = sources.toPath().resolve("environment.properties");
        if (Files.exists(envPropertiesPath)) {
            final Properties p = new Properties();
            try (InputStream inputStream = Files.newInputStream(envPropertiesPath)) {
                p.load(inputStream);
            }
            addProperties(envPropertyArtifacts, envProperties, MODULE_SOURCE, "environment", p);
        }

        final Path systemPropertiesPath = sources.toPath().resolve("system.properties");
        if (Files.exists(systemPropertiesPath)) {
            final Properties p = new Properties();
            try (InputStream inputStream = Files.newInputStream(systemPropertiesPath)) {
                p.load(inputStream);
            }
            addProperties(systemPropertyArtifacts, systemProperties, MODULE_SOURCE, "system", p);
        }

        final Path attachmentsPath = sources.toPath().resolve("attachments.xml");
        if (Files.exists(attachmentsPath)) {
            final Properties p;
            try (InputStream inputStream = Files.newInputStream(attachmentsPath)) {
                p = parseAttachments(inputStream, MODULE_SOURCE);
            }
            addAttachments(attachmentPropertyArtifacts, attachmentProperties, MODULE_SOURCE, p);
        }

        final Iterator<Artifact> iterator = artifacts.iterator();
        while (iterator.hasNext()) {
            final Artifact artifact = iterator.next();
            if ("zip".equals(artifact.getType()) && "packaging".equals(artifact.getClassifier())) {
                iterator.remove();
                try (ZipFile zipFile = new ZipFile(artifact.getFile())) {
                    addProperties(envPropertyArtifacts, envProperties, artifact.toString(), zipFile,
                            "environment.properties", "environment");
                    addProperties(systemPropertyArtifacts, systemProperties, artifact.toString(), zipFile,
                            "system.properties", "system");
                    addProperties(attachmentPropertyArtifacts, attachmentProperties, artifact.toString(), zipFile);
                    final ZipArchiveEntry commandArgumentsEntry = zipFile.getEntry("commandArguments");
                    if (commandArgumentsEntry != null) {
                        if (commandArgumentsArtifact != null) {
                            throw new MojoExecutionException(
                                    "Duplicated command arguments definition: artifacts " + commandArgumentsArtifact +
                                    " and " + artifact);
                        }
                        commandArgumentsArtifact = artifact.toString();
                        try (InputStream inputStream = zipFile.getInputStream(commandArgumentsEntry)) {
                            commandArguments = getCommandArguments(templates, names, inputStream);
                        }
                    }
                }
            }
        }

        if (commandArguments == null) {
            throw new MojoExecutionException("Package's command arguments not defined");
        }

        final String artifactFileName = finalName + (classifier == null ? ".tar.xz" : "-" + classifier + ".tar.xz");
        final File artifactFile = new File(buildDirectory, artifactFileName);
        final long now = System.currentTimeMillis();

        try (FileOutputStream outputStream = new FileOutputStream(artifactFile);
             XZCompressorOutputStream xzOutputStream = new XZCompressorOutputStream(outputStream);
             TarArchiveOutputStream tarOutputStream = new TarArchiveOutputStream(xzOutputStream, UTF_8.name())) {

            tarOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

            writeDependencies(now, tarOutputStream, artifacts);
            writeEntry(now, tarOutputStream, "environment.properties",
                    getPropertiesStream(templates, names, envProperties));
            writeEntry(now, tarOutputStream, "system.properties",
                    getPropertiesStream(templates, names, systemProperties));
            writeEntry(now, tarOutputStream, "commandArguments", commandArguments);
            writeEntry(now, tarOutputStream, "attachments.properties", getAttachmentsStream(attachmentProperties));
        }

        if (classifier == null) {
            projectHelper.attachArtifact(project, "tar.xz", artifactFile);
        } else {
            projectHelper.attachArtifact(project, "tar.xz", classifier, artifactFile);
        }

        for (final String name : names) {
            getLog().info("Configuration property name: " + name);
        }
    }

    private void addProperties(
            @Nonnull Map<String, String> propertyArtifacts,
            @Nonnull Properties properties,
            @Nonnull String artifact,
            @Nonnull ZipFile zipFile,
            @Nonnull String zipEntryName,
            @Nonnull String propertyTypeName
    ) throws IOException, MojoExecutionException {

        final ZipArchiveEntry entry = zipFile.getEntry(zipEntryName);
        if (entry != null) {
            final Properties p = new Properties();
            try (InputStream inputStream = zipFile.getInputStream(entry)) {
                p.load(inputStream);
            }
            addProperties(propertyArtifacts, properties, artifact, propertyTypeName, p);
        }
    }

    private void addProperties(
            @Nonnull Map<String, String> propertyArtifacts,
            @Nonnull Properties properties,
            @Nonnull String artifact,
            @Nonnull String propertyTypeName,
            @Nonnull Properties p
    ) throws MojoExecutionException {

        for (final String name : p.stringPropertyNames()) {
            final String definedInArtifact = propertyArtifacts.get(name);
            if (definedInArtifact != null) {
                throw new MojoExecutionException(
                        "Duplicated " + propertyTypeName + " property \"" + name + "\" definition: artifacts " +
                        definedInArtifact + " and " + artifact);
            }
            propertyArtifacts.put(name, artifact);
            properties.setProperty(name, p.getProperty(name));
        }
    }

    private void addProperties(
            @Nonnull Map<String, String> propertyArtifacts,
            @Nonnull Properties properties,
            @Nonnull String artifact,
            @Nonnull ZipFile zipFile
    ) throws IOException, MojoExecutionException, ParserConfigurationException, SAXException {

        final ZipArchiveEntry entry = zipFile.getEntry("attachments.xml");
        if (entry != null) {
            final Properties p;
            try (InputStream inputStream = zipFile.getInputStream(entry)) {
                p = parseAttachments(inputStream, artifact);
            }
            addAttachments(propertyArtifacts, properties, artifact, p);
        }
    }

    private void addAttachments(
            @Nonnull Map<String, String> propertyArtifacts,
            @Nonnull Properties properties,
            @Nonnull String artifact,
            @Nonnull Properties p
    ) throws MojoExecutionException {

        for (final String name : p.stringPropertyNames()) {
            final String definedInArtifact = propertyArtifacts.get(name);
            if (definedInArtifact != null) {
                throw new MojoExecutionException(
                        "Duplicated attachment property \"" + name + "\" definition: artifacts " +
                        definedInArtifact + " and " + artifact);
            }
            propertyArtifacts.put(name, artifact);
            properties.setProperty(name, p.getProperty(name));
        }
    }

    private ByteArrayOutputStream getPropertiesStream(Templates templates, Set<String> names, Properties properties)
            throws IOException {
        for (final String propertyName : properties.stringPropertyNames()) {
            templates.parse(properties.getProperty(propertyName), (n, a) -> names.add(n));
        }
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        properties.store(byteArrayOutputStream, null);
        return byteArrayOutputStream;
    }

    private Properties parseAttachments(InputStream inputStream, String artifact)
            throws IOException, ParserConfigurationException, SAXException, MojoExecutionException {

        final Properties properties = new Properties();

        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder db = dbf.newDocumentBuilder();
        final Document document = db.parse(inputStream);
        final Element attachmentsElement = document.getDocumentElement();
        final NodeList attachmentElements = attachmentsElement.getElementsByTagName("attachment");
        for (int i = 0; i < attachmentElements.getLength(); i++) {
            final Element item = (Element) attachmentElements.item(i);
            final String name = item.getElementsByTagName("name").item(0).getTextContent();

            if (properties.getProperty(name) != null) {
                throw new MojoExecutionException(
                        "Duplicated attachment property \"" + name + "\" definition: artifact " + artifact);
            }

            final String groupId = item.getElementsByTagName("groupId").item(0).getTextContent();
            final String artifactId = item.getElementsByTagName("artifactId").item(0).getTextContent();

            final NodeList c = item.getElementsByTagName("classifier");
            final String classifier = c.getLength() == 0 ? null : c.item(0).getTextContent();

            final String type = item.getElementsByTagName("type").item(0).getTextContent();
            final String version = item.getElementsByTagName("version").item(0).getTextContent();

            final StringBuilder builder = new StringBuilder();
            builder.append(requireNonNull(groupId));
            builder.append(':').append(requireNonNull(artifactId));
            builder.append(':').append(requireNonNull(type));

            if (classifier != null) {
                builder.append(':').append(classifier);
            }

            builder.append(':').append(requireNonNull(version));
            properties.setProperty(name, builder.toString());
        }

        return properties;
    }

    private ByteArrayOutputStream getAttachmentsStream(Properties properties) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        properties.store(byteArrayOutputStream, null);
        return byteArrayOutputStream;
    }

    private ByteArrayOutputStream getCommandArguments(Templates templates, Set<String> names, InputStream in)
            throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final byte[] buffer = new byte[0x10000];
        while (true) {
            final int read = in.read(buffer);
            if (read == -1) {
                break;
            }
            byteArrayOutputStream.write(buffer, 0, read);
        }
        templates.parse(new String(byteArrayOutputStream.toByteArray(), UTF_8), (n, a) -> names.add(n));
        return byteArrayOutputStream;
    }

    private void writeDependencies(
            long now,
            TarArchiveOutputStream tarOutputStream,
            Set<Artifact> artifacts
    ) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(byteArrayOutputStream, UTF_8);
             BufferedWriter writer = new BufferedWriter(outputStreamWriter)) {
            for (final Artifact artifact : artifacts) {
                writer.write(artifact.getId());
                writer.newLine();
            }
        }
        writeEntry(now, tarOutputStream, "dependencies", byteArrayOutputStream);
    }

    private void writeEntry(
            long now,
            TarArchiveOutputStream tarOutputStream,
            String name,
            ByteArrayOutputStream byteArrayOutputStream
    ) throws IOException {
        final byte[] bytes = byteArrayOutputStream.toByteArray();
        final TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(bytes.length);
        entry.setModTime(now);
        tarOutputStream.putArchiveEntry(entry);
        tarOutputStream.write(bytes);
        tarOutputStream.closeArchiveEntry();
    }
}
