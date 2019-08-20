package org.uberfire.java.nio.fs.k8s;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.WatchEvent;
import org.uberfire.java.nio.file.WatchEvent.Kind;
import org.uberfire.java.nio.file.WatchKey;
import org.uberfire.java.nio.file.Watchable;

@SuppressWarnings("serial")
public class K8SWatchKey implements WatchKey {
    private final transient K8SWatchService service;
    private final transient Path path;
    private final AtomicReference<State> state = new AtomicReference<>(State.READY);
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final BlockingQueue<WatchEvent<?>> events = new LinkedBlockingQueue<>();
    private final transient Map<Kind<Path>, Event> eventKinds = new ConcurrentHashMap<>();

    K8SWatchKey(K8SWatchService service, Path path) {
        this.service = service;
        this.path = path;
    }

    @Override
    public boolean isValid() {
        return !service.isClose() && valid.get();
    }

    @Override
    public List<WatchEvent<?>> pollEvents() {
        List<WatchEvent<?>> result = new ArrayList<>(events.size());
        events.drainTo(result);
        eventKinds.clear();
        return Collections.unmodifiableList(result);
    }

    @Override
    public boolean reset() {
        if (isValid()) {
            events.clear();
            eventKinds.clear();
            return state.compareAndSet(State.SIGNALLED, State.READY);
        } else {
            return false;
        }
    }

    @Override
    public void cancel() {
        valid.set(false);
    }

    @Override
    public Watchable watchable() {
        return this.path;
    }

    protected boolean postEvent(WatchEvent.Kind<Path> kind) {
        Event event = eventKinds.computeIfAbsent(kind, k -> {
            Event e = new Event(kind, K8SWatchKey.this.path.getParent());
            return events.offer(e) ? e : null;
        });
        if (event == null) {
            return false;
        } else {
            event.increaseCount();
            return true;
        }
    }

    protected boolean isQueued() {
        return state.get() == State.SIGNALLED;
    }

    protected void signal() {
        state.compareAndSet(State.READY, State.SIGNALLED);
    }

    enum State {
        READY,
        SIGNALLED
    }

    private static final class Event implements WatchEvent<Path> {

        private final AtomicInteger count = new AtomicInteger(0);
        private final transient Kind<Path> kind;
        private final transient Path context;

         private Event(Kind<Path> kind, Path context) {
            this.kind = kind;
            this.context = context;
        }

        @Override
        public Kind<Path> kind() {
            return this.kind;
        }

        @Override
        public int count() {
            return count.get();
        }

        @Override
        public Path context() {
            return this.context;
        }

        private int increaseCount() {
            return count.incrementAndGet();
        }
    }
}
