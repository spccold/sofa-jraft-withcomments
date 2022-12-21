/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Snapshot storage based on local file storage.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 2:11:30 PM
 */
public class LocalSnapshotStorage implements SnapshotStorage {

    private static final Logger                      LOG       = LoggerFactory.getLogger(LocalSnapshotStorage.class);

    private static final String                      TEMP_PATH = "temp";
    private final ConcurrentMap<Long, AtomicInteger> refMap    = new ConcurrentHashMap<>();
    private final String                             path;
    private Endpoint                                 addr;
    private boolean                                  filterBeforeCopyRemote;
    private long                                     lastSnapshotIndex;
    private final Lock                               lock;
    private final RaftOptions                        raftOptions;
    private SnapshotThrottle                         snapshotThrottle;

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public boolean hasServerAddr() {
        return this.addr != null;
    }

    public void setServerAddr(Endpoint addr) {
        this.addr = addr;
    }

    public LocalSnapshotStorage(String path, RaftOptions raftOptions) {
        super();
        this.path = path;
        this.lastSnapshotIndex = 0;
        this.raftOptions = raftOptions;
        this.lock = new ReentrantLock();
    }

    public long getLastSnapshotIndex() {
        this.lock.lock();
        try {
            return this.lastSnapshotIndex;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean init(final Void v) {
        // exp: xxx/snapshot
        final File dir = new File(this.path);

        try {
            FileUtils.forceMkdir(dir);
        } catch (final IOException e) {
            LOG.error("Fail to create directory {}.", this.path, e);
            return false;
        }

        // delete temp snapshot
        if (!this.filterBeforeCopyRemote) { // default is false
            // xxx/snapshot/temp, 用于临时存放snapshot文件
            final String tempSnapshotPath = this.path + File.separator + TEMP_PATH;
            final File tempFile = new File(tempSnapshotPath);
            if (tempFile.exists()) {
                try {
                    FileUtils.forceDelete(tempFile);
                } catch (final IOException e) {
                    LOG.error("Fail to delete temp snapshot path {}.", tempSnapshotPath, e);
                    return false;
                }
            }
        }
        // delete old snapshot
        final List<Long> snapshots = new ArrayList<>();
        // 获取xxx/snapshot目录下所有的子目录, 例如snapshot_1, snapshot_2, snapshot_3......
        final File[] oldFiles = dir.listFiles();
        if (oldFiles != null) {
            for (final File sFile : oldFiles) {
                final String name = sFile.getName();
                if (!name.startsWith(Snapshot.JRAFT_SNAPSHOT_PREFIX)) { // 排查非raft snapshot文件
                    continue;
                }
                // 解析获取子目录后缀
                final long index = Long.parseLong(name.substring(Snapshot.JRAFT_SNAPSHOT_PREFIX.length()));
                snapshots.add(index);
            }
        }

        // TODO: add snapshot watcher

        // get last_snapshot_index
        if (!snapshots.isEmpty()) {
            Collections.sort(snapshots);
            final int snapshotCount = snapshots.size();
            // 启动的时候, 除了最后一个snapshot外, 其它的snapshot全部删除
            for (int i = 0; i < snapshotCount - 1; i++) {
                final long index = snapshots.get(i);
                final String snapshotPath = getSnapshotPath(index);
                if (!destroySnapshot(snapshotPath)) {
                    return false;
                }
            }
            // 保留最后一个snapshot index
            this.lastSnapshotIndex = snapshots.get(snapshotCount - 1);
            // 最后的snapshot引用+1, 避免被意外删除
            ref(this.lastSnapshotIndex);
        }

        return true;
    }

    private String getSnapshotPath(final long index) {
        return this.path + File.separator + Snapshot.JRAFT_SNAPSHOT_PREFIX + index;
    }

    void ref(final long index) {
        final AtomicInteger refs = getRefs(index);
        refs.incrementAndGet();
    }

    private boolean destroySnapshot(final String path) {
        LOG.info("Deleting snapshot {}.", path);
        final File file = new File(path);
        try {
            FileUtils.deleteDirectory(file);
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to destroy snapshot {}.", path, e);
            return false;
        }
    }

    void unref(final long index) {
        final AtomicInteger refs = getRefs(index);
        if (refs.decrementAndGet() == 0) { // 某个snapshot没有引用时, 进行清理操作
            if (this.refMap.remove(index, refs)) {
                destroySnapshot(getSnapshotPath(index));
            }
        }
    }

    AtomicInteger getRefs(final long index) {
        AtomicInteger refs = this.refMap.get(index);
        if (refs == null) {
            refs = new AtomicInteger(0);
            final AtomicInteger eRefs = this.refMap.putIfAbsent(index, refs);
            if (eRefs != null) {
                refs = eRefs;
            }
        }
        return refs;
    }

    void close(final LocalSnapshotWriter writer, final boolean keepDataOnError) throws IOException {
        int ret = writer.getCode();
        IOException ioe = null;

        // noinspection ConstantConditions
        do {
            if (ret != 0) {
                break;
            }
            try {
                if (!writer.sync()) {// meta 写入磁盘
                    ret = RaftError.EIO.getNumber();
                    break;
                }
            } catch (final IOException e) {
                LOG.error("Fail to sync writer {}.", writer.getPath(), e);
                ret = RaftError.EIO.getNumber();
                ioe = e;
                break;
            }
            final long oldIndex = getLastSnapshotIndex();
            final long newIndex = writer.getSnapshotIndex();
            if (oldIndex == newIndex) {
                ret = RaftError.EEXISTS.getNumber();
                break;
            }
            // exp: xxx/snapshot/temp 内容搬到 exp: xxx/snapshot/snapshot_${last_include_index}
            // rename temp to new
            final String tempPath = this.path + File.separator + TEMP_PATH;
            final String newPath = getSnapshotPath(newIndex);

            if (!destroySnapshot(newPath)) {
                LOG.warn("Delete new snapshot path failed, path is {}.", newPath);
                ret = RaftError.EIO.getNumber();
                ioe = new IOException("Fail to delete new snapshot path: " + newPath);
                break;
            }
            LOG.info("Renaming {} to {}.", tempPath, newPath);
            if (!Utils.atomicMoveFile(new File(tempPath), new File(newPath), true)) {
                LOG.error("Renamed temp snapshot failed, from path {} to path {}.", tempPath, newPath);
                ret = RaftError.EIO.getNumber();
                ioe = new IOException("Fail to rename temp snapshot from: " + tempPath + " to: " + newPath);
                break;
            }
            // 新的snapshot目录引用+1
            ref(newIndex);
            this.lock.lock();
            try {
                Requires.requireTrue(oldIndex == this.lastSnapshotIndex);
                this.lastSnapshotIndex = newIndex;
            } finally {
                this.lock.unlock();
            }
            // 老的snapshot目录(上一次的snapshot)引用-1, 如果引用降为0, 则删除整个目录
            unref(oldIndex);
        } while (false);

        if (ret != 0) {
            LOG.warn("Close snapshot writer {} with exit code: {}.", writer.getPath(), ret);
            if (!keepDataOnError) {
                destroySnapshot(writer.getPath());
            }
        }

        if (ioe != null) {
            throw ioe;
        }

    }

    @Override
    public void shutdown() {
        // ignore
    }

    @Override
    public boolean setFilterBeforeCopyRemote() {
        this.filterBeforeCopyRemote = true;
        return true;
    }

    @Override
    public SnapshotWriter create() {
        return create(true);
    }

    public SnapshotWriter create(final boolean fromEmpty) {
        LocalSnapshotWriter writer = null;
        // noinspection ConstantConditions
        do {
            // exp: xxx/snapshot/temp, 每次快照先写到temp目录, 然后再移动到xxx/snapshot/snapshot_${log_index}目录中
            // but why ?
            final String snapshotPath = this.path + File.separator + TEMP_PATH;
            // delete temp
            // TODO: Notify watcher before deleting
            if (new File(snapshotPath).exists() && fromEmpty) {
                if (!destroySnapshot(snapshotPath)) {
                    break;
                }
            }
            writer = new LocalSnapshotWriter(snapshotPath, this, this.raftOptions);
            if (!writer.init(null)) {
                LOG.error("Fail to init snapshot writer.");
                writer = null;
                break;
            }
        } while (false);
        return writer;
    }

    @Override
    public SnapshotReader open() {
        long lsIndex = 0;
        this.lock.lock();
        try {
            if (this.lastSnapshotIndex != 0) {
                lsIndex = this.lastSnapshotIndex;
                ref(lsIndex);
            }
        } finally {
            this.lock.unlock();
        }
        if (lsIndex == 0) {
            LOG.warn("No data for snapshot reader {}.", this.path);
            return null;
        }
        final String snapshotPath = getSnapshotPath(lsIndex);
        final SnapshotReader reader = new LocalSnapshotReader(this, this.snapshotThrottle, this.addr, this.raftOptions,
            snapshotPath);
        // 读取last snapshot的meta信息
        if (!reader.init(null)) {
            LOG.error("Fail to init reader for path {}.", snapshotPath);
            unref(lsIndex);
            return null;
        }
        return reader;
    }

    @Override
    public SnapshotReader copyFrom(final String uri, final SnapshotCopierOptions opts) {
        final SnapshotCopier copier = startToCopyFrom(uri, opts);
        if (copier == null) {
            return null;
        }
        try {
            copier.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Join on snapshot copier was interrupted.");
            return null;
        }
        final SnapshotReader reader = copier.getReader();
        Utils.closeQuietly(copier);
        return reader;
    }

    @Override
    public SnapshotCopier startToCopyFrom(final String uri, final SnapshotCopierOptions opts) {
        final LocalSnapshotCopier copier = new LocalSnapshotCopier();
        copier.setStorage(this);
        copier.setSnapshotThrottle(this.snapshotThrottle);
        copier.setFilterBeforeCopyRemote(this.filterBeforeCopyRemote);
        if (!copier.init(uri, opts)) {
            LOG.error("Fail to init copier to {}.", uri);
            return null;
        }
        copier.start();
        return copier;
    }

}
