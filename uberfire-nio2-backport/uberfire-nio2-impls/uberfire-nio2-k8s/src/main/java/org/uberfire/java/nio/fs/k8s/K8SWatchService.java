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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.java.nio.IOException;
import org.uberfire.java.nio.file.ClosedWatchServiceException;
import org.uberfire.java.nio.file.InterruptedException;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.WatchKey;
import org.uberfire.java.nio.file.WatchService;
import org.uberfire.java.nio.fs.cloud.CloudClientFactory;

import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.CFG_MAP_LABEL_FSOBJ_TYPE_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.getPathByFsObjCM;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.mapActionToKind;

public class K8SWatchService implements WatchService {

    private static final Logger logger = LoggerFactory.getLogger(K8SWatchService.class);
    private final CloudClientFactory ccf;
    private final K8SFileSystem fs;
    private final BlockingQueue<WatchKey> buckets = new LinkedBlockingQueue<>();
    private final Map<Path, WatchKey> registrations = new ConcurrentHashMap<>();

    private final CompletableFuture<Void> closed = new CompletableFuture<>();

    public K8SWatchService(K8SFileSystem fs) {
        this.fs = fs;
        this.ccf = (CloudClientFactory) fs.provider();
        Executors.newSingleThreadExecutor().execute(() -> 
            ccf.executeCloudFunction(this::triageEvents, KubernetesClient.class));
    }

    @Override
    public void close() throws IOException {
        logger.info("K8SFileSystem WatchService is closing.");
        if (closed.complete(null)) {
            logger.info("K8SFileSystem WatchService closed normally.");
        } else {
            logger.info("K8SFileSystem WatchService has been closed already.");
        }
        buckets.clear();
        registrations.clear();
    }

    @Override
    public WatchKey poll() throws ClosedWatchServiceException {
        checkOpen();
        return buckets.poll();
    }

    @Override
    public WatchKey poll(long timeout, TimeUnit unit) throws ClosedWatchServiceException, InterruptedException {
        WatchKey bucket = null;
        checkOpen();
        try {
            bucket = buckets.poll(timeout, unit);
        } catch (java.lang.InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException();
        }
        return bucket;
    }

    @Override
    public WatchKey take() throws ClosedWatchServiceException, InterruptedException {
        WatchKey bucket = null;
        checkOpen();
        try {
            bucket = buckets.take();
        } catch (java.lang.InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException();
        }
        return bucket;
    }

    @Override
    public boolean isClose() {
        return closed.isDone();
    }

    protected final void checkOpen() {
        if (closed.isDone()) {
            throw new ClosedWatchServiceException();
        }
    }

    private CompletableFuture<Void> triageEvents(KubernetesClient client) {
        logger.info("K8SFileSystem WatchService is starting to watch K8SFileSystem ConfigMap in namespace: [{}]",
                    client.getNamespace());
        try (Watch watchable = client.configMaps().withLabel(CFG_MAP_LABEL_FSOBJ_TYPE_KEY)
                                     .watch(new Watcher<ConfigMap>() {
            @Override
            public void eventReceived(Action action, ConfigMap fsObjCM) {
                logger.debug("Event - Action: {}, {} on ConfigMap ", action, fsObjCM.getMetadata().getLabels());
                Path path = getPathByFsObjCM(K8SWatchService.this.fs, fsObjCM);
                
                K8SWatchKey key = (K8SWatchKey) registrations
                        .computeIfAbsent(path, p -> new K8SWatchKey(K8SWatchService.this, p));
                mapActionToKind(action).ifPresent(e -> {
                    if (key.postEvent(e) && key.isValid() && !key.isQueued() && buckets.offer(key)) {
                        key.signal();
                    }
                });
            }

            @Override
            public void onClose(KubernetesClientException cause) {
                logger.info("K8SFileSystem ConfigMap Watcher closed.");
                if (cause != null) {
                    logger.info(cause.getMessage());
                }
            }
        })) {
            logger.info("K8SFileSystem ConfigMap Watcher thread started.");
            closed.get();
            logger.info("K8SFileSystem ConfigMap Watcher thread terminated.");
        } catch (ExecutionException ee) {
            logger.error("K8SFileSystem ConfigMap Watcher thread terminated with execution exception.", ee);
            closed.completeExceptionally(ee);
        } catch (Exception e) {
            if (!closed.isDone()) {
                logger.error("K8SFileSystem ConfigMap Watcher thread terminated with exception.", e);
                closed.completeExceptionally(e);
            } 
        }
        return closed;
    }
}
