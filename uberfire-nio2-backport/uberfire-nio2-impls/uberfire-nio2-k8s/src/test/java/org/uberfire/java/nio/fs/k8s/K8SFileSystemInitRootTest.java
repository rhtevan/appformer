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

package org.uberfire.java.nio.fs.k8s;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Lists;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uberfire.java.nio.file.DirectoryStream;
import org.uberfire.java.nio.file.FileSystem;
import org.uberfire.java.nio.file.Files;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.spi.FileSystemProvider;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.getFileNameString;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.getFsObjCM;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.getFsObjNameElementLabel;

public class K8SFileSystemInitRootTest {

    @ClassRule
    public static KubernetesServer SERVER = new KubernetesServer(false, true);
    // The default namespace for MockKubernetes Server is 'test'
    protected static String TEST_NAMESPACE = "test";
    protected static ThreadLocal<KubernetesClient> CLIENT_FACTORY =
            ThreadLocal.withInitial(() -> SERVER.getClient());

    protected static final FileSystemProvider fsProvider = new K8SFileSystemProvider() {

        @Override
        public KubernetesClient createKubernetesClient() {
            return CLIENT_FACTORY.get();
        }

    };

    @BeforeClass
    public static void setup() {
    }

    @AfterClass
    public static void tearDown() {
        CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE).delete();
        CLIENT_FACTORY.get().close();
    }
    
    @Test
    public void testRoot() throws URISyntaxException {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        final Path root = fileSystem.getPath("/");
        Map<String, String> ne = getFsObjNameElementLabel(root);
        
        List<Path> roots = StreamSupport.stream(fileSystem.getRootDirectories().spliterator(), false)
                                        .collect(Collectors.toList());
        assertThat(roots).asList().size().isEqualTo(1);
        assertThat(roots.get(0)).isEqualTo(root);

        /**
         * Handling this hard coded dependency
         * https://github.com/kiegroup/appformer/blob/92d05f8620fb775a9fdd96574273de2deda3d215/uberfire-structure/uberfire-structure-backend/src/main/java/org/guvnor/structure/backend/config/ConfigurationServiceImpl.java#L129
         */
        assertThat(root.toUri().toString().contains("/master@")).isTrue();
        assertThat(root).isEqualTo(fileSystem.getPath("/path").getRoot());
        assertThat(root.getRoot().equals(root)).isTrue();
        assertThat(root.toString().equals("/")).isTrue();
        assertThat(root.toRealPath().toString().equals("/")).isTrue();
        assertThat(root.getParent()).isNull();
        assertThat(root.getFileName()).isNull();
        assertThat(root.getNameCount()).isEqualTo(0);
        assertThat(root.iterator().hasNext()).isEqualTo(false);
        assertThat(ne.size()).isEqualTo(0);
        assertThat(getFileNameString(root).equals("/")).isTrue();
    }

    @Test
    public void testInitRoot() {
        final FileSystem fs = fsProvider.getFileSystem(URI.create("default:///"));
        final Path root = fs.getPath("/");
        final Path testParentDir = fs.getPath("/.testParentDir");
        final Path testDir = fs.getPath("/.testParentDir/.testInitRoot");
        final Path rPath = fs.getPath("./../.rRelativePath");
        
        assertThat(root.getParent()).isNull();
        assertThat(root.isAbsolute()).isTrue();

        assertThat(rPath.isAbsolute()).isFalse();
        assertThat(rPath.getRoot()).isNull();
        
        Path aPath = ((K8SFileSystemProvider)fsProvider).toAbsoluteRealPath(rPath);
        assertThat(aPath.getRoot()).isEqualTo(root);
        assertThat(aPath.getParent()).isNotNull();
        assertThat(aPath.isAbsolute()).isTrue();
        assertThat(aPath.getFileName()).isEqualTo(rPath.getFileName());
                                                  
        assertThat(testDir.isAbsolute()).isTrue();
        assertThat(testDir.getParent()).isEqualTo(testParentDir);
        
        assertThat(testParentDir.getParent()).isEqualTo(root);
        assertThat(testParentDir).isEqualTo(((K8SFileSystemProvider)fsProvider).toAbsoluteRealPath(testParentDir));
        
        CLIENT_FACTORY.get()
                      .configMaps()
                      .inNamespace(TEST_NAMESPACE)
                      .createOrReplace(CLIENT_FACTORY.get().configMaps()
                                                     .load(K8SFileSystemTest.class.getResourceAsStream("/test-k8sfs-dir-r-empty-configmap.yml"))
                                                     .get());
        assertThat(Files.exists(root)).isTrue();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
            ArrayList<Path> dirContent = Lists.newArrayList(stream);
            assertThat(dirContent).asList().isEmpty();
        }
        
        CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE).delete();
        assertThat(Files.exists(root)).isFalse();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
            ArrayList<Path> dirContent = Lists.newArrayList(stream);
            assertThat(dirContent).asList().isEmpty();
        }
        assertThat(Files.exists(root)).isTrue();
        assertThat(getFsObjCM(CLIENT_FACTORY.get(), root).getData()).isNotNull();
        
        Files.createDirectory(testDir);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(testParentDir)) {
            ArrayList<Path> dirContent = Lists.newArrayList(stream);
            assertThat(dirContent).asList().containsExactly(testDir);
        }
    }

}
