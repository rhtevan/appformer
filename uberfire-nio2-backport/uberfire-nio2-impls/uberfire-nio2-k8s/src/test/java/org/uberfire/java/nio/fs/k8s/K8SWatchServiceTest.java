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

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uberfire.java.nio.file.FileSystem;
import org.uberfire.java.nio.file.Files;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.WatchService;
import org.uberfire.java.nio.file.spi.FileSystemProvider;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

public class K8SWatchServiceTest {

    @ClassRule
    public static KubernetesServer SERVER = new KubernetesServer(false, true);
    // The default namespace for MockKubernetes Server is 'test'
    protected static String TEST_NAMESPACE = "test";
    protected static ThreadLocal<KubernetesClient> CLIENT_FACTORY =
            new ThreadLocal<>().withInitial(() -> SERVER.getClient());

    protected static final FileSystemProvider fsProvider = new K8SFileSystemProvider() {

        @Override
        public KubernetesClient createKubernetesClient() {
            return CLIENT_FACTORY.get();
        }

    };

    @BeforeClass
    public static void setup() {
        // Load testing KieServerState ConfigMap data into mock server from file
        CLIENT_FACTORY.get()
                      .configMaps()
                      .inNamespace(TEST_NAMESPACE)
                      .createOrReplace(CLIENT_FACTORY.get().configMaps()
                                                     .load(K8SWatchServiceTest.class.getResourceAsStream("/test-k8sfs-dir-r-configmap.yml"))
                                                     .get());
        CLIENT_FACTORY.get()
                      .configMaps()
                      .inNamespace(TEST_NAMESPACE)
                      .createOrReplace(CLIENT_FACTORY.get().configMaps()
                                                     .load(K8SWatchServiceTest.class.getResourceAsStream("/test-k8sfs-dir-0-configmap.yml"))
                                                     .get());
    }

    @AfterClass
    public static void tearDown() {
        CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE).delete();
        CLIENT_FACTORY.get().close();
    }
    
    @Test
    public void testSetup() {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        final Path root = fileSystem.getPath("/");
        final Path dir = fileSystem.getPath("/testDir");
        
        assertThat(root.getFileSystem().provider()).isEqualTo(fsProvider);
        assertThat(Files.exists(root)).isTrue();
        assertThat(Files.exists(dir)).isTrue();
        assertThat(Files.isDirectory(root)).isTrue();
        assertThat(Files.isDirectory(root)).isTrue();
    }

    @Test
    public void testWatchServiceOpenAndClose() throws URISyntaxException {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        final WatchService ws = fileSystem.newWatchService();
        // MockWebServer does not support websocket; as a result, WatchService will shutdown itself.
        assertThat(ws.isClose()).isFalse();
        await().until(ws::isClose);
        ws.close();
        assertThat(ws.isClose()).isTrue();
    }
}
