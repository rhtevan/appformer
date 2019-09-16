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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.java.nio.IOException;
import org.uberfire.java.nio.base.AbstractBasicFileAttributeView;
import org.uberfire.java.nio.base.BasicFileAttributesImpl;
import org.uberfire.java.nio.base.GeneralPathImpl;
import org.uberfire.java.nio.channels.SeekableByteChannel;
import org.uberfire.java.nio.file.AccessDeniedException;
import org.uberfire.java.nio.file.AccessMode;
import org.uberfire.java.nio.file.AtomicMoveNotSupportedException;
import org.uberfire.java.nio.file.CopyOption;
import org.uberfire.java.nio.file.DeleteOption;
import org.uberfire.java.nio.file.DirectoryNotEmptyException;
import org.uberfire.java.nio.file.DirectoryStream;
import org.uberfire.java.nio.file.FileAlreadyExistsException;
import org.uberfire.java.nio.file.FileStore;
import org.uberfire.java.nio.file.LinkOption;
import org.uberfire.java.nio.file.NoSuchFileException;
import org.uberfire.java.nio.file.NotDirectoryException;
import org.uberfire.java.nio.file.NotLinkException;
import org.uberfire.java.nio.file.OpenOption;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.attribute.BasicFileAttributes;
import org.uberfire.java.nio.file.attribute.FileAttribute;
import org.uberfire.java.nio.file.attribute.FileAttributeView;
import org.uberfire.java.nio.fs.cloud.CloudClientFactory;
import org.uberfire.java.nio.fs.file.SimpleFileSystemProvider;

import static org.kie.soup.commons.validation.PortablePreconditions.checkCondition;
import static org.kie.soup.commons.validation.PortablePreconditions.checkNotNull;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_ANNOTATION_FSOBJ_SIZE_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.CFG_MAP_FSOBJ_CONTENT_KEY;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemConstants.K8S_FS_SCHEME;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.createOrReplaceFSCM;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.createOrReplaceParentDirFSCM;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.deleteAndUpdateParentCM;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.getFsObjCM;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.getPathByFsObjCM;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.isDirectory;
import static org.uberfire.java.nio.fs.k8s.K8SFileSystemUtils.isRoot;

public class K8SFileSystemProvider extends SimpleFileSystemProvider implements CloudClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(K8SFileSystemProvider.class);
    public K8SFileSystemProvider() {
        super(null, OSType.UNIX_LIKE);
        this.fileSystem = new K8SFileSystem(this, K8SFileSystem.UNIX_SEPARATOR_STRING);
    }

    @Override
    public String getScheme() {
        return K8S_FS_SCHEME;
    }

    @Override
    public InputStream newInputStream(final Path pathIn,
                                      final OpenOption... options) throws IllegalArgumentException, 
        NoSuchFileException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        Path path = toAbsoluteRealPath(pathIn);
        checkFileNotExistThenThrow(path, false);
        logger.info("Open InputStream to file [{}]", path);
        return Channels.newInputStream(new K8SFileChannel(path, this));
    }

    @Override
    public OutputStream newOutputStream(final Path pathIn,
                                        final OpenOption... options) throws IllegalArgumentException, 
        UnsupportedOperationException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        Path path = toAbsoluteRealPath(pathIn);
        logger.info("Open OutputStream to file [{}]", path);
        return Channels.newOutputStream(new K8SFileChannel(path, this));
    }

    @Override
    public FileChannel newFileChannel(final Path pathIn,
                                      final Set<? extends OpenOption> options,
                                      final FileAttribute<?>... attrs) throws IllegalArgumentException, 
        UnsupportedOperationException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        throw new UnsupportedOperationException();
    }
    
    @Override
    public SeekableByteChannel newByteChannel(final Path pathIn,
                                              final Set<? extends OpenOption> options,
                                              final FileAttribute<?>... attrs) 
        throws IllegalArgumentException, UnsupportedOperationException, FileAlreadyExistsException, 
            IOException, SecurityException {
        Path path = toAbsoluteRealPath(pathIn);
        return new K8SFileChannel(path, this);
    }

    @Override
    public void createDirectory(final Path dirIn,
                                final FileAttribute<?>... attrs) throws UnsupportedOperationException, 
        FileAlreadyExistsException, IOException, SecurityException {
        checkNotNull("dir",dirIn);
        Path dir = toAbsoluteRealPath(dirIn);
        Optional<ConfigMap> directoryCm = executeCloudFunction(client -> getFsObjCM(client, dir), KubernetesClient.class);
        if (directoryCm.isPresent()) {
            throw new FileAlreadyExistsException(dir.toString());
        }
        
        executeCloudFunction(client -> createOrReplaceFSCM(client, 
                                                           dir,
                                                           isRoot(dir) ? Optional.empty()
                                                                       : createOrReplaceParentDirFSCM(client, dir, 0L, false),
                                                           Collections.emptyMap(),
                                                           true), 
                             KubernetesClient.class);
    }

    @Override
    protected Path[] getDirectoryContent(final Path dirIn, final DirectoryStream.Filter<Path> filter) 
            throws NotDirectoryException {
        checkNotNull("dir", dirIn);
        Path dir = toAbsoluteRealPath(dirIn);
        if (isRoot(dir) &&
            !executeCloudFunction(client -> getFsObjCM(client, dir), KubernetesClient.class).isPresent()) {
            initRoot();
        }
        ConfigMap dirCM = executeCloudFunction(client -> getFsObjCM(client, dir), KubernetesClient.class)
                .orElseThrow(() -> new NotDirectoryException(dir.toString()));
        if (dirCM.getData() == null || dirCM.getData().isEmpty()) {
            return new Path[0];
        }
        
        String separator = dir.getFileSystem().getSeparator();
        String dirPathString = getPathByFsObjCM((K8SFileSystem)fileSystem, dirCM).toString();
        return dirCM.getData()
                    .keySet()
                    .stream()
                    .map(fileName -> GeneralPathImpl.create(dir.getFileSystem(), 
                                                           (dirPathString.endsWith(separator) ? 
                                                            dirPathString :
                                                            dirPathString.concat(separator)).concat(fileName), 
                                                            false))
                    .toArray(Path[]::new);
    }
    
    private synchronized void initRoot() {
        Path root = this.fileSystem.getPath(K8SFileSystem.UNIX_SEPARATOR_STRING);
        this.createDirectory(root);
        logger.info("Root directory created.");
    }

    @Override
    public void delete(final Path pathIn, final DeleteOption... options) 
            throws NoSuchFileException, DirectoryNotEmptyException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        Path path = toAbsoluteRealPath(pathIn);
        checkFileNotExistThenThrow(path, false);
        deleteIfExists(path, options);
    }

    @Override
    public boolean deleteIfExists(final Path pathIn, final DeleteOption... options) 
            throws DirectoryNotEmptyException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        Path path = toAbsoluteRealPath(pathIn);
        synchronized (this) {
            try {
                return executeCloudFunction(client -> deleteAndUpdateParentCM(client, path), 
                                            KubernetesClient.class).get();
            } finally {
                toGeneralPathImpl(path).clearCache();
            }
        }
    }
    
    @Override
    public boolean isHidden(final Path pathIn) throws IllegalArgumentException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        checkFileNotExistThenThrow(pathIn, false);
        return pathIn.getFileName().toString().startsWith(K8SFileSystemConstants.K8S_FS_HIDDEN_FILE_INDICATOR);
    }

    @Override
    public void checkAccess(final Path pathIn, AccessMode... modes) throws UnsupportedOperationException,
        NoSuchFileException, AccessDeniedException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        checkNotNull("modes", modes);
        Path path = toAbsoluteRealPath(pathIn);
        checkFileNotExistThenThrow(path, false);

        for (final AccessMode mode : modes) {
            checkNotNull("mode", mode);
            switch (mode) {
                case READ:
                    break;
                case EXECUTE:
                    throw new AccessDeniedException(path.toString());
                case WRITE:
                    break;
            }
        }
    }

    @Override
    public FileStore getFileStore(final Path pathIn) throws IOException, SecurityException {
        checkNotNull("path", pathIn);
        Path path = toAbsoluteRealPath(pathIn);
        return new K8SFileStore(path);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(final Path pathIn,
                                                            final Class<A> type,
                                                            final LinkOption... options) 
        throws NoSuchFileException, UnsupportedOperationException, IOException, SecurityException {
        checkNotNull("path", pathIn);
        checkNotNull("type", type);
        Path path = toAbsoluteRealPath(pathIn);
        checkFileNotExistThenThrow(path, false);
        if (type == BasicFileAttributesImpl.class || type == BasicFileAttributes.class) {
            final K8SBasicFileAttributeView view = getFileAttributeView(path,
                                                                        K8SBasicFileAttributeView.class,
                                                                        options);
            return view.readAttributes();
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <V extends FileAttributeView> V createFileAttributeView(final GeneralPathImpl path, 
                                                                      final Class<V> type) {
        if (AbstractBasicFileAttributeView.class.isAssignableFrom(type)) {
            final V newView = (V) new K8SBasicFileAttributeView(path, this);
            path.addAttrView(newView);
            return newView;
        } else {
            return null;
        }
    }
    
    @Override
    public void copy(final Path sourceIn,
                     final Path targetIn,
                     final CopyOption... options) throws UnsupportedOperationException, 
        FileAlreadyExistsException, DirectoryNotEmptyException, IOException, SecurityException {
        checkNotNull("source", sourceIn);
        checkNotNull("target", targetIn);
        Path source = toAbsoluteRealPath(sourceIn);
        Path target = toAbsoluteRealPath(targetIn);

        Optional<ConfigMap> srcCMOpt = executeCloudFunction(
            client -> getFsObjCM(client, source), KubernetesClient.class);
        checkCondition("source must exist", srcCMOpt.isPresent());
        checkFileExistsThenThrow(target);

        ConfigMap srcCM = srcCMOpt.get();
        if (isDirectory(srcCMOpt.get())) {
            throw new UnsupportedOperationException(srcCMOpt.get().getMetadata().getName() + "is a directory.");
        }
        
        String content = srcCM.getData().getOrDefault(CFG_MAP_FSOBJ_CONTENT_KEY, "");
        long size = Long.parseLong(srcCM.getMetadata().getAnnotations().getOrDefault(CFG_MAP_ANNOTATION_FSOBJ_SIZE_KEY, "0"));
        executeCloudFunction(client -> createOrReplaceFSCM(client, 
                                                           target,
                                                           createOrReplaceParentDirFSCM(client, target, size, false),
                                                           Collections.singletonMap(CFG_MAP_FSOBJ_CONTENT_KEY, content),
                                                           false), 
                             KubernetesClient.class);
    }

    @Override
    public void move(final Path sourceIn,
                     final Path targetIn,
                     final CopyOption... options) throws DirectoryNotEmptyException, 
        AtomicMoveNotSupportedException, IOException, SecurityException {
        Path source = toAbsoluteRealPath(sourceIn);
        Path target = toAbsoluteRealPath(targetIn);
        try {
            copy(source, target);
        } catch (Exception e) {
            try {
                delete(target);
            } catch (NoSuchFileException nsfe) {
                throw new IOException("Moving file failed due to Copy Source Exception [" + e.getMessage() + "].");
            } catch (Exception exp) {
                throw new IOException("Moving file failed due to these errors: Copy Source Exception [" + 
                        e.getMessage() + "]; Delete Target Exception [" + exp.getMessage() + "].");
            }
        } 
        
        try {
            delete(source);
        } catch (Exception e) {
            throw new IOException("Moving file failed due to Delete Source Exception [" + e.getMessage() + "], " +
                    "which will leave file system in an inconsistent state.");
        }
    }
    
    @Override
    protected void checkFileNotExistThenThrow(final Path pathIn, boolean isLink) 
            throws NoSuchFileException, NotLinkException {
        Path path = toAbsoluteRealPath(pathIn);
        executeCloudFunction(client -> getFsObjCM(client, path), KubernetesClient.class)
            .orElseThrow(() -> {
                logger.info("File not found [{}]", path.toUri().toString());
                return new NoSuchFileException(path.toUri().toString());
            });
    }

    @Override
    protected void checkFileExistsThenThrow(final Path pathIn) throws FileAlreadyExistsException {
        Path path = toAbsoluteRealPath(pathIn);
        if (executeCloudFunction(client -> getFsObjCM(client, path), KubernetesClient.class).isPresent()) {
            throw new FileAlreadyExistsException(path.toString());
        }
    }

    protected Path toAbsoluteRealPath(Path path) {
        if (path.isAbsolute()) {
            if (path.getParent() == null) {
                return path; // Root
            } else if (path.getParent().toString().contains(".")) {
                fileSystem.getPath(path.toRealPath().toString());
            } else {
                return path; // RealPath
            }
        }
        return fileSystem.getPath(path.toAbsolutePath().toRealPath().toString());
    }
}
