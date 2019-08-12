/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.uberfire.java.nio.fs.k8s.integrationtests;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uberfire.java.nio.file.FileAlreadyExistsException;
import org.uberfire.java.nio.file.FileSystem;
import org.uberfire.java.nio.file.Files;
import org.uberfire.java.nio.file.NoSuchFileException;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.spi.FileSystemProvider;
import org.uberfire.java.nio.fs.k8s.K8SFileSystemProvider;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class K8SFileSystemProviderIntegrationTest {

    private static final String KUBERNETES_MASTER_API_URL = System.getProperty(Config.KUBERNETES_MASTER_SYSTEM_PROPERTY);
    private static final String KUBERNETES_MASTER_API_TOKEN = System.getProperty(Config.KUBERNETES_OAUTH_TOKEN_SYSTEM_PROPERTY);
    private static final String TEST_NAMESPACE = "k8sfsp-test";
    private static KubernetesClient client;

    protected static final FileSystemProvider fsProvider = new K8SFileSystemProvider();

    @BeforeClass
    public static void setup() {
        System.setProperty(Config.KUBERNETES_NAMESPACE_SYSTEM_PROPERTY, TEST_NAMESPACE);

        Config config = new ConfigBuilder()
                .withMasterUrl(KUBERNETES_MASTER_API_URL)
                .withOauthToken(KUBERNETES_MASTER_API_TOKEN)
                .withTrustCerts(true)
                .withNamespace(TEST_NAMESPACE)
                .build();
        client = new DefaultKubernetesClient(config);
        client.namespaces().createNew().withNewMetadata()
                                       .withName(TEST_NAMESPACE)
                                       .endMetadata()
                                       .done();
    }

    @After
    public void cleanNamespace() {
        client.configMaps().inNamespace(TEST_NAMESPACE).delete();
    }

    @AfterClass
    public static void tearDown() {
        client.namespaces().withName(TEST_NAMESPACE).delete();
        client.close();
        System.clearProperty(Config.KUBERNETES_NAMESPACE_SYSTEM_PROPERTY);
    }

    @Test
    public void simpleRootFolderCreateDeleteTest() {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        Path folderInRootFolder = fileSystem.getPath("/test");

        assertThat(Files.exists(folderInRootFolder)).isFalse();
        assertThat(Files.deleteIfExists(folderInRootFolder)).isFalse();
        Files.createDirectory(folderInRootFolder);
        assertThat(Files.deleteIfExists(folderInRootFolder)).isTrue();
    }

    @Test
    public void simpleRootFileCreateDeleteTest() throws IOException {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        Path fileInRootFolder = fileSystem.getPath("/test.txt");

        assertThat(Files.exists(fileInRootFolder)).isFalse();
        assertThat(Files.deleteIfExists(fileInRootFolder)).isFalse();

        createOrEditFile(fileInRootFolder, "Hello");
        assertThat(Files.deleteIfExists(fileInRootFolder)).isTrue();
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void simpleRootFolderCreateDuplicateFolderTest() {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        Path folderInRootFolder = fileSystem.getPath("/test");

        Files.createDirectory(folderInRootFolder);
        Files.createDirectory(folderInRootFolder);
    }

    @Test
    public void simpleRootFileEditFileTest() throws IOException {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        Path fileInRootFolder = fileSystem.getPath("/test.txt");

        createOrEditFile(fileInRootFolder, "Hello");
        assertThat(readFile(fileInRootFolder)).isEqualTo("Hello");
        createOrEditFile(fileInRootFolder, "Welcome");
        assertThat(readFile(fileInRootFolder)).isEqualTo("Welcome");
        createOrEditFile(fileInRootFolder, "Hi");
        assertThat(readFile(fileInRootFolder)).isEqualTo("Hi");
    }

    @Test(expected = NoSuchFileException.class)
    public void inputStreamFromNotExistingFileTest() throws IOException {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        Path fileInRootFolder = fileSystem.getPath("/test.txt");

        readFile(fileInRootFolder);
    }

    private void createOrEditFile(Path file, String fileContent) throws IOException {
        try (OutputStream fileStream = Files.newOutputStream(file)) {
            fileStream.write(fileContent.getBytes());
            fileStream.flush();
        }
    }

    private String readFile(Path file) throws IOException {
        try (InputStream fileStream = Files.newInputStream(file)) {
            return IOUtils.toString(fileStream, StandardCharsets.UTF_8.name());
        }
    }
}
