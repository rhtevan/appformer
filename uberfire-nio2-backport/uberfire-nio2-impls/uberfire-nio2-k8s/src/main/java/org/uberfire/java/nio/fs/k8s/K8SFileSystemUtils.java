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

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.StandardWatchEventKind;
import org.uberfire.java.nio.file.WatchEvent.Kind;
import org.uberfire.java.nio.fs.cloud.CloudClientConstants;

import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_ANNOTATION_FSOBJ_LAST_MODIFIED_TIMESTAMP_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_ANNOTATION_FSOBJ_SIZE_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_FSOBJ_CONTENT_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_FSOBJ_NAME_PREFIX;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_LABEL_FSOBJ_APP_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_LABEL_FSOBJ_NAME_KEY_PREFIX;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_LABEL_FSOBJ_TYPE_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.K8S_FS_APP_DEFAULT_VALUE;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.K8S_FS_APP_PROPERTY_NAME;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemObjectType.UNKNOWN;


public class K8SFileSystemUtils {

    public static final String APP_NAME = System.getProperty(K8S_FS_APP_PROPERTY_NAME, K8S_FS_APP_DEFAULT_VALUE);

    private static final Logger logger = LoggerFactory.getLogger(K8SFileSystemUtils.class);
    private K8SFileSystemUtils() {}

    static Optional<ConfigMap> createOrReplaceParentDirFSCM(KubernetesClient client, 
                                                            Path self, 
                                                            long selfSize,
                                                            boolean... isUpdateForFileDeletion) {
        String selfName = getFileNameString(self);
        Path parent = Optional.ofNullable(self.getParent()).orElseThrow(IllegalArgumentException::new);
        Map<String, String> parentContent = Optional.ofNullable(getFsObjCM(client, parent))
                .map(ConfigMap::getData)
                .orElseGet(HashMap::new);
        
        if (isUpdateForFileDeletion.length != 0 && isUpdateForFileDeletion[0]) {
            parentContent.remove(selfName);
        } else {
            parentContent.put(selfName, String.valueOf(selfSize));
        }
        final long parentSize = parentContent.values().stream().mapToLong(Long::parseLong).sum();

        return Optional.of(createOrReplaceFSCM(client, parent,
                                               parent.getRoot().equals(parent)
                                                       ? Optional.empty()
                                                       : createOrReplaceParentDirFSCM(client, parent, parentSize),
                                               parentContent,
                                               true));
    }

    static ConfigMap createOrReplaceFSCM(KubernetesClient client,
                                         Path path,
                                         Optional<ConfigMap> parentOpt,
                                         Map<String, String> content,
                                         boolean isDir) {
        String fileName = getFileNameString(path);
        long size = 0;
        Map<String, String> labels = getFsObjNameElementLabel(path);
        if (isDir) {
            if (labels.isEmpty()) {
                labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.ROOT.toString());
            } else {
                labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.DIR.toString());
            }
            size = content.values().stream().mapToLong(Long::parseLong).sum();
        } else {
            labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.FILE.toString());
            size = parentOpt.map(cm -> Long.parseLong(cm.getData().get(fileName)))
                            .orElseThrow(() -> new IllegalStateException("File [" +
                                                                         fileName +
                                                                         "] is not found at parent directory [" +
                                                                         path.toRealPath().getParent().toString() +
                                                                         "]"));
        }
        labels.put(CFG_MAP_LABEL_FSOBJ_APP_KEY, APP_NAME);
        
        Map<String, String> annotations = new ConcurrentHashMap<>();
        annotations.put(CFG_MAP_ANNOTATION_FSOBJ_LAST_MODIFIED_TIMESTAMP_KEY, 
                        ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
        annotations.put(CFG_MAP_ANNOTATION_FSOBJ_SIZE_KEY, String.valueOf(size));
        
        String cmName = Optional.ofNullable(getFsObjCM(client, path))
                .map(cm -> cm.getMetadata().getName())
                .orElseGet(() -> CFG_MAP_FSOBJ_NAME_PREFIX + UUID.randomUUID().toString());
        return parentOpt.map(parent -> client.configMaps().createOrReplace(new ConfigMapBuilder()
                                             .withNewMetadata()
                                               .withName(cmName)
                                               .withLabels(labels)
                                               .withAnnotations(annotations)
                                               .withOwnerReferences(new OwnerReferenceBuilder()
                                                 .withApiVersion(parent.getApiVersion())
                                                 .withKind(parent.getKind())
                                                 .withName(parent.getMetadata().getName())
                                                 .withUid(parent.getMetadata().getUid())
                                                 .build())
                                             .endMetadata()
                                             .withData(content)
                                             .build()))
                        .orElseGet(() -> client.configMaps().createOrReplace(new ConfigMapBuilder()
                                               .withNewMetadata()
                                                 .withName(cmName)
                                                 .withLabels(labels)
                                                 .withAnnotations(annotations)
                                               .endMetadata()
                                               .withData(content)
                                               .build()));
    }

    static boolean deleteAndUpdateParentCM(KubernetesClient client, Path path) {
        ConfigMap cm = getFsObjCM(client, path);
        if (cm != null && client.configMaps().delete(cm)) {
            createOrReplaceParentDirFSCM(client, path, 0, true);
            return true;
        } else {
            return false;
        }
    }

    static ConfigMap getFsObjCM(KubernetesClient client, Path path) {
        int nameCount = path.getNameCount();
        Map<String, String> labels = getFsObjNameElementLabel(path);
        if (labels.isEmpty()) {
            labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.ROOT.toString());
        } 
        labels.put(CFG_MAP_LABEL_FSOBJ_APP_KEY, APP_NAME);
        Object[] configMaps = client.configMaps()
                                           .withLabels(labels)
                                           .list()
                                           .getItems()
                                           .stream()
                                           .filter(cm -> cm.getMetadata()
                                                           .getLabels()
                                                           .entrySet()
                                                           .stream()
                                                           .filter(entry -> entry.getKey().startsWith(CFG_MAP_LABEL_FSOBJ_NAME_KEY_PREFIX))
                                                           .count() == nameCount)
                                           .toArray();
        
        if (configMaps.length > 1) {
            throw new IllegalStateException("Ambiguous K8S FileSystem object name: [" + path.toString() +
                                            "]; should not have be associated with more than one " +
                                            "K8S FileSystem ConfigMaps.");
        }
        if (configMaps.length == 1) {
            return (ConfigMap)configMaps[0];
        }
        return null;
    }

    static byte[] getFsObjContentBytes(ConfigMap cm) {
        byte[] content = new byte[0];
        try {
            content = cm.getData().get(CFG_MAP_FSOBJ_CONTENT_KEY).getBytes(CloudClientConstants.ENCODING);
        } catch (UnsupportedEncodingException e) {
            logger.warn("Invalid encoding [{}], returns zero length byte array content.",
                        CloudClientConstants.ENCODING);
        }
        return content;
    }

    static  Map<String, String> getFsObjNameElementLabel(Path path) {
        Map<String, String> labels = new HashMap<>();
        path.toAbsolutePath().toRealPath().iterator().forEachRemaining(
            subPath -> labels.put(CFG_MAP_LABEL_FSOBJ_NAME_KEY_PREFIX + labels.size(), subPath.toString())
        );
        return labels;
    }
    
    static String getFileNameString(Path path) {
        return Optional.ofNullable(path.getFileName()).map(Path::toString).orElse("/");
    }

    static long getSize(ConfigMap fileCM) {
        return Long.parseLong(fileCM.getMetadata().getAnnotations().getOrDefault(CFG_MAP_ANNOTATION_FSOBJ_SIZE_KEY, "0"));
    }

    static long getCreationTime(ConfigMap fileCM) {
        return parseTimestamp(fileCM.getMetadata().getCreationTimestamp()).getEpochSecond();
    }
    
    static long getLastModifiedTime(ConfigMap fileCM) {
        return parseTimestamp(fileCM.getMetadata().getAnnotations()
                                                  .get(CFG_MAP_ANNOTATION_FSOBJ_LAST_MODIFIED_TIMESTAMP_KEY))
                .getEpochSecond();
    }
    
    static Path getPathByFsObjCM(K8SFileSystem fs, ConfigMap cm) {
        StringBuilder pathBuilder = new StringBuilder();
        Map<String, String> labels = cm.getMetadata().getLabels();
        if (labels.isEmpty() || !labels.containsKey(CFG_MAP_LABEL_FSOBJ_TYPE_KEY)) {
            throw new IllegalArgumentException("Invalid K8SFileSystem ConfigMap - Missing required labels");
        }
        if (labels.containsValue(K8SFileSystemObjectType.ROOT.toString())) {
            return fs.getPath("/");
        }
        labels.entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(CFG_MAP_LABEL_FSOBJ_NAME_KEY_PREFIX))
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> pathBuilder.append(fs.getSeparator()).append(entry.getValue()));
        return fs.getPath(pathBuilder.toString());
    }

    static boolean isFile(ConfigMap fileCM) {
        return K8SFileSystemObjectType.FILE.toString()
                                           .equals(fileCM.getMetadata()
                                                         .getLabels()
                                                         .getOrDefault(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, UNKNOWN.toString()));
    }

    static boolean isDirectory(ConfigMap fileCM) {
        return K8SFileSystemObjectType.DIR.toString()
                                          .equals(fileCM.getMetadata()
                                                        .getLabels()
                                                        .getOrDefault(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, UNKNOWN.toString()))
               ||                           
               K8SFileSystemObjectType.ROOT.toString()
                .equals(fileCM.getMetadata()
                              .getLabels()
                              .getOrDefault(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, UNKNOWN.toString()));
    }
    
    static Optional<Kind<Path>> mapActionToKind(Action action) {
        switch(action) {
            case ADDED:
                return Optional.of(StandardWatchEventKind.ENTRY_CREATE);
            case DELETED:
                return Optional.of(StandardWatchEventKind.ENTRY_DELETE);
            case MODIFIED:
                return Optional.of(StandardWatchEventKind.ENTRY_MODIFY);
            case ERROR:
            default:
                return Optional.empty();
        }
    }

    static Instant parseTimestamp(String timestamp) {
        return Optional.ofNullable(timestamp).map(ts -> ZonedDateTime.parse(ts, DateTimeFormatter.ISO_DATE_TIME)
                                                                     .toInstant())
                                             .orElse(Instant.now());
    }
}
