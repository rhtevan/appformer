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

import org.uberfire.java.nio.IOException;
import org.uberfire.java.nio.file.WatchService;
import org.uberfire.java.nio.file.spi.FileSystemProvider;
import org.uberfire.java.nio.fs.file.SimpleUnixFileSystem;

public class K8SFileSystem extends SimpleUnixFileSystem {

    K8SFileSystem(final FileSystemProvider provider, final String path) {
        super(provider, path);
        fileStore = new K8SFileStore(null);
    }

    @Override
    public WatchService newWatchService() throws UnsupportedOperationException, IOException {
        return new K8SWatchService(this);
    }
}
