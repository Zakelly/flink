/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.segmented;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.segmented.LogicalFile.LogicalFileId;
import org.apache.flink.runtime.checkpoint.segmented.SegmentCheckpointUtils.SegmentSnapshotFileSystemInfo;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsSegmentCheckpointStateOutputStream;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.apache.flink.runtime.checkpoint.segmented.FileOperationReason.FileOperationReasonType.FILE_WRITE_OR_CLOSE_FAILURE;

/** Base implementation of {@link SegmentSnapshotManager}. */
public abstract class SegmentSnapshotManagerBase implements SegmentSnapshotManager {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentSnapshotManager.class);

    private final String id;

    private final SegmentType segmentType;

    protected final long physicalFileSizeThreshold;

    protected final Executor ioExecutor;

    protected final Object lock = new Object();

    protected Map<Path, PhysicalFile> physicalFiles = new ConcurrentHashMap<>();

    @GuardedBy("lock")
    protected NavigableMap<Long, Set<LogicalFile>> uploadedStates = new TreeMap<>();

    // file system and directories
    protected FileSystem fs;
    protected Path checkpointDir;
    protected Path sharedStateDir;
    protected Path taskOwnedStateDir;

    protected boolean entropyInjecting;
    protected int writeBufferSize;
    private boolean fileSystemInitiated = false;

    protected boolean syncAfterClosingLogicalFile;

    protected PhysicalFile.PhysicalFileDeleter physicalFileDeleter = this::deletePhysicalFile;

    private final Map<SubtaskKey, Path> ssmManagedSharedStateDir = new ConcurrentHashMap<>();

    protected Path ssmManagedExclusiveStateDir;

    public SegmentSnapshotManagerBase(
            String id, SegmentType segmentType, long maxFileSize, Executor ioExecutor) {
        this.id = id;
        this.segmentType = segmentType;
        this.physicalFileSizeThreshold = maxFileSize > 0 ? maxFileSize : Long.MAX_VALUE;
        this.ioExecutor = ioExecutor;
    }

    // ------------------------------------------------------------------------
    //  CheckpointListener
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(SubtaskKey subtaskKey, long checkpointId)
            throws Exception {
        LOG.info("notifyCheckpointComplete: {}", checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(SubtaskKey subtaskKey, long checkpointId) throws Exception {
        LOG.info("notifyCheckpointAborted: {}", checkpointId);
        synchronized (lock) {
            Set<LogicalFile> logicalFilesForCurrentCp = uploadedStates.get(checkpointId);
            if (logicalFilesForCurrentCp == null) {
                return;
            }
            Iterator<LogicalFile> logicalFileIterator = logicalFilesForCurrentCp.iterator();
            while (logicalFileIterator.hasNext()) {
                LogicalFile logicalFile = logicalFileIterator.next();
                if (logicalFile.getSubtaskKey().equals(subtaskKey)
                        && logicalFile.getLastCheckpointId() <= checkpointId) {
                    logicalFile.discardWithCheckpointID(
                            checkpointId, FileOperationReason.checkpointSubsumption(checkpointId));
                    logicalFileIterator.remove();
                }
            }

            if (logicalFilesForCurrentCp.isEmpty()) {
                uploadedStates.remove(checkpointId);
                endCheckpointOfAllSubtasks(checkpointId);
            }
        }
    }

    @Override
    public void notifyCheckpointSubsumed(SubtaskKey subtaskKey, long checkpointId)
            throws Exception {
        LOG.info("notifyCheckpointSubsumed: {}", checkpointId);
        synchronized (lock) {
            Iterator<Map.Entry<Long, Set<LogicalFile>>> uploadedStatesIterator =
                    uploadedStates.entrySet().iterator();
            while (uploadedStatesIterator.hasNext()) {
                Map.Entry<Long, Set<LogicalFile>> entry = uploadedStatesIterator.next();
                if (entry.getKey() > checkpointId) {
                    break;
                }

                Set<LogicalFile> logicalFilesForCurrentCp = entry.getValue();
                Iterator<LogicalFile> logicalFileIterator = logicalFilesForCurrentCp.iterator();
                while (logicalFileIterator.hasNext()) {
                    LogicalFile logicalFile = logicalFileIterator.next();
                    if (logicalFile.getSubtaskKey().equals(subtaskKey)
                            && logicalFile.getLastCheckpointId() <= checkpointId) {
                        logicalFile.discardWithCheckpointID(
                                checkpointId,
                                FileOperationReason.checkpointSubsumption(checkpointId));
                        logicalFileIterator.remove();
                    }
                }

                if (logicalFilesForCurrentCp.isEmpty()) {
                    uploadedStatesIterator.remove();
                    endCheckpointOfAllSubtasks(entry.getKey());
                }
            }
        }
    }

    @Override
    public boolean isEnabled() {
        return segmentType != SegmentType.UNSEGMENTED;
    }

    @Override
    public void initFileSystem(SegmentSnapshotFileSystemInfo fileSystemInfo) throws IOException {
        initFileSystem(
                fileSystemInfo.fs,
                fileSystemInfo.checkpointBaseDirectory,
                fileSystemInfo.sharedStateDirectory,
                fileSystemInfo.taskOwnedStateDirectory,
                fileSystemInfo.writeBufferSize,
                EntropyInjector.isEntropyInjecting(fileSystemInfo.fs));
    }

    @Override
    public void addSubtask(SubtaskKey subtaskKey) {
        try {
            String managedDirName = subtaskKey.getManagedDirName();
            Path managedPath = new Path(sharedStateDir, managedDirName);
            fs.mkdirs(managedPath);
            ssmManagedSharedStateDir.put(subtaskKey, managedPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------------
    //  snapshot
    // ------------------------------------------------------------------------

    @Override
    public FsSegmentCheckpointStateOutputStream createCheckpointStateOutputStream(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope) {

        // TODO: for rocksdb, we need to deal with incremental checkpointing, where we can encounter
        //  duplicated logical files
        return new FsSegmentCheckpointStateOutputStream(
                checkpointId,
                writeBufferSize,
                new FsSegmentCheckpointStateOutputStream
                        .SegmentedCheckpointStateOutputStreamHandler() {
                    PhysicalFile physicalFile;
                    LogicalFile logicalFile;

                    @Override
                    public Tuple2<FSDataOutputStream, Path> providePhysicalFile()
                            throws IOException {
                        physicalFile = getPhysicalFile(subtaskKey, checkpointId, scope);
                        return new Tuple2<>(
                                physicalFile.getOutputStream(), physicalFile.getFilePath());
                    }

                    @Override
                    public SegmentFileStateHandle closeStreamAndCreateStateHandle(
                            Path filePath, long startPos, long stateSize) throws IOException {
                        if (physicalFile == null) {
                            return null;
                        } else {
                            // deal with logical file
                            logicalFile = createLogicalFile(physicalFile, subtaskKey);
                            logicalFile.advanceLastCheckpointId(checkpointId);
                            synchronized (lock) {
                                uploadedStates
                                        .computeIfAbsent(checkpointId, key -> new HashSet<>())
                                        .add(logicalFile);
                            }

                            // deal with physicalFile file
                            physicalFile.incSize(stateSize);
                            tryReusingPhysicalFileAfterClosingStream(
                                    subtaskKey, checkpointId, physicalFile);

                            return new SegmentFileStateHandle(
                                    physicalFile.getFilePath(),
                                    startPos,
                                    stateSize,
                                    logicalFile.getFileID(),
                                    scope);
                        }
                    }

                    @Override
                    public void closeStream() throws IOException {
                        if (physicalFile != null) {
                            FileOperationReason reason =
                                    new FileOperationReason(
                                            FILE_WRITE_OR_CLOSE_FAILURE, checkpointId);
                            if (logicalFile != null) {
                                logicalFile.discardWithCheckpointID(checkpointId, reason);
                            } else {
                                // The physical file should be closed anyway. This is because the
                                // last segment write on this file is likely to have failed, and we
                                // want to prevent further reusing of this file.
                                physicalFile.deleteIfNecessary(reason, true);
                            }
                        }
                    }
                });
    }

    // ------------------------------------------------------------------------
    //  restore
    // ------------------------------------------------------------------------

    @Override
    public void addOperatorStateHandles(
            SubtaskKey subtaskKey,
            long checkpointId,
            List<StateObjectCollection<OperatorStateHandle>> stateHandles) {
        List<SegmentFileStateHandle> stateHandlesToAdd = new ArrayList<>();
        stateHandles.forEach(
                collection ->
                        collection.forEach(
                                operatorStateHandle -> {
                                    StreamStateHandle streamStateHandle =
                                            operatorStateHandle.getDelegateStateHandle();
                                    if (streamStateHandle instanceof SegmentFileStateHandle) {
                                        stateHandlesToAdd.add(
                                                (SegmentFileStateHandle) streamStateHandle);
                                    }
                                }));
        addStateHandles(checkpointId, subtaskKey, stateHandlesToAdd, (e) -> false);
        this.physicalFiles.forEach(
                ((fileID, physicalFile) ->
                        LOG.info("Restored physical file: " + physicalFile.toString())));
    }

    @Override
    public void addKeyedStateHandles(
            SubtaskKey subtaskKey,
            long checkpointId,
            List<StateObjectCollection<KeyedStateHandle>> stateHandles) {
        List<SegmentFileStateHandle> stateHandlesToAdd = new ArrayList<>();
        stateHandles.forEach(
                collection ->
                        collection.forEach(
                                keyedStateHandle -> {
                                    StreamStateHandle streamStateHandle =
                                            getStreamStateHandle(keyedStateHandle);
                                    if (streamStateHandle instanceof SegmentFileStateHandle) {
                                        stateHandlesToAdd.add(
                                                (SegmentFileStateHandle) streamStateHandle);
                                    }
                                }));
        addStateHandles(
                checkpointId, subtaskKey, stateHandlesToAdd, (path) -> isResponsibleForFile(path));
        this.physicalFiles.forEach(
                ((fileID, physicalFile) ->
                        LOG.info("Restored physical file: " + physicalFile.toString())));
    }

    public void addStateHandles(
            long checkpointId,
            SubtaskKey subtaskKey,
            Collection<SegmentFileStateHandle> stateHandles,
            Function<Path, Boolean> needDelete) {
        // if anything fails here, we simply propagate the exception and fail this restore
        Set<LogicalFile> uploadedLogicalFiles;
        synchronized (lock) {
            uploadedLogicalFiles =
                    this.uploadedStates.computeIfAbsent(checkpointId, k -> new HashSet<>());
        }
        for (SegmentFileStateHandle stateHandle : stateHandles) {
            Path physicalFilepath = stateHandle.getFilePath();
            PhysicalFile physicalFile =
                    this.physicalFiles.computeIfAbsent(
                            physicalFilepath,
                            fileID ->
                                    PhysicalFile.getClosedInstance(
                                            physicalFilepath,
                                            needDelete.apply(physicalFilepath)
                                                    ? physicalFileDeleter
                                                    : null,
                                            stateHandle.getScope()));

            LogicalFile logicalFile =
                    LogicalFile.getRestoredInstance(
                            stateHandle.getLogicalFileId(), physicalFile, subtaskKey);

            synchronized (lock) {
                uploadedLogicalFiles.add(logicalFile);
            }
        }
    }

    private StreamStateHandle getStreamStateHandle(KeyedStateHandle keyedStateHandle) {
        if (keyedStateHandle instanceof KeyGroupsSavepointStateHandle) {
            return ((KeyGroupsSavepointStateHandle) keyedStateHandle).getDelegateStateHandle();
        } else {
            // currently do not support other types of KeyedStateHandle
            return null;
        }
    }

    // ------------------------------------------------------------------------
    //  logical & physical file
    // ------------------------------------------------------------------------

    protected LogicalFile createLogicalFile(
            @Nonnull PhysicalFile physicalFile, @Nonnull SubtaskKey subtaskKey) {
        LogicalFileId fileID = LogicalFileId.generateRandomID();
        LogicalFile logicalFile = new LogicalFile(fileID, physicalFile, subtaskKey);
        return logicalFile;
    }

    public @Nonnull PhysicalFile getPhysicalFile(
            SubtaskKey subtaskKey, long checkpointID, CheckpointedStateScope scope)
            throws IOException {
        @Nonnull
        PhysicalFile physicalFile =
                getOrCreatePhysicalFileForCheckpoint(subtaskKey, checkpointID, scope);
        LOG.info(
                "Get physical file {} for checkpoint {} of subtask {}.",
                physicalFile.getFilePath(),
                checkpointID,
                subtaskKey);
        Preconditions.checkArgument(physicalFile.isOpen());

        this.physicalFiles.put(physicalFile.getFilePath(), physicalFile);
        return physicalFile;
    }

    @Nonnull
    protected PhysicalFile createPhysicalFile(SubtaskKey subtaskKey, CheckpointedStateScope scope)
            throws IOException {
        PhysicalFile result;
        Exception latestException = null;

        Path dirPath =
                scope.equals(CheckpointedStateScope.SHARED)
                        ? ssmManagedSharedStateDir.get(subtaskKey)
                        : ssmManagedExclusiveStateDir;

        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                OutputStreamAndPath streamAndPath =
                        EntropyInjector.createEntropyAware(
                                fs,
                                createPhysicalFilePath(dirPath),
                                FileSystem.WriteMode.NO_OVERWRITE);
                FSDataOutputStream outputStream = streamAndPath.stream();
                Path filePath = streamAndPath.path();
                result = new PhysicalFile(outputStream, filePath, this.physicalFileDeleter, scope);
                updateFileCreationMetrics(filePath);
                return result;
            } catch (Exception e) {
                latestException = e;
            }
        }

        throw new IOException("Could not open output stream for state backend", latestException);
    }

    private void updateFileCreationMetrics(Path path) {
        // todo: use io metrics
        LOG.info("Create a new physical file {}.", path);
    }

    protected Path createPhysicalFilePath(Path dirPath) {
        // this must be called after initFileSystem() is called
        // so the checkpoint directories must be not null if we reach here
        final String fileName = UUID.randomUUID().toString();
        return new Path(dirPath, fileName);
    }

    boolean isResponsibleForFile(Path filePath) {
        // SSM is only responsible for (deleting) the files in its own directory.
        // If SSM is restored after a rescaling operation, this check method also excludes the files
        // in the previous ssmManagedDir before the rescaling.
        Path parent = filePath.getParent();
        return parent.equals(ssmManagedExclusiveStateDir)
                || ssmManagedSharedStateDir.containsValue(parent);
    }

    protected final void deletePhysicalFile(
            FSDataOutputStream outputStream, Path filePath, FileOperationReason reason) {

        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOG.warn("Fail to close output stream when deleting file: {}", filePath);
            }
        }
        ioExecutor.execute(
                () -> {
                    try {
                        fs.delete(filePath, false);
                        LOG.info("Physical file deleted: {}, reason: {}", filePath, reason);
                    } catch (IOException e) {
                        LOG.warn("Fail to delete file: {}", filePath);
                    }
                });
    }

    // ------------------------------------------------------------------------
    //  abstract methods
    // ------------------------------------------------------------------------

    protected abstract void tryReusingPhysicalFileAfterClosingStream(
            SubtaskKey subtaskKey, long checkpointId, PhysicalFile physicalFile) throws IOException;

    @Nonnull
    protected abstract PhysicalFile getOrCreatePhysicalFileForCheckpoint(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope)
            throws IOException;

    protected abstract void endCheckpointOfAllSubtasks(long checkpointId) throws IOException;

    // ------------------------------------------------------------------------
    //  file system
    // ------------------------------------------------------------------------

    /**
     * Initiate the {@link SegmentSnapshotManager} so that it can interact with the file systems,
     * e.g. recording path information, creating working directories, etc.
     *
     * @param fileSystem The filesystem to write to.
     * @param checkpointBaseDir The base directory for checkpoints.
     * @param sharedStateDir The directory for shared checkpoint data.
     * @param taskOwnedStateDir The name of the directory for state not owned/released by the
     *     master, but by the TaskManagers.
     * @param writeBufferSize The write buffer size.
     * @param entropyInjecting Whether the file system dynamically injects entropy into the file
     *     paths.
     * @throws IOException Thrown, if the working directory cannot be created.
     */
    private void initFileSystem(
            FileSystem fileSystem,
            Path checkpointBaseDir,
            Path sharedStateDir,
            Path taskOwnedStateDir,
            int writeBufferSize,
            boolean entropyInjecting)
            throws IOException {
        if (fileSystemInitiated) {
            Preconditions.checkArgument(
                    checkpointBaseDir.equals(this.checkpointDir),
                    "The checkpoint base dir is not deterministic across subtasks.");
            Preconditions.checkArgument(
                    sharedStateDir.equals(this.sharedStateDir),
                    "The shared checkpoint dir is not deterministic across subtasks.");
            Preconditions.checkArgument(
                    taskOwnedStateDir.equals(this.taskOwnedStateDir),
                    "The task-owned checkpoint dir is not deterministic across subtasks.");
            return;
        }
        this.fs = fileSystem;
        this.checkpointDir = checkpointBaseDir;
        this.sharedStateDir = sharedStateDir;
        this.taskOwnedStateDir = taskOwnedStateDir;
        this.fileSystemInitiated = true;
        this.entropyInjecting = entropyInjecting;
        this.writeBufferSize = writeBufferSize;
        this.syncAfterClosingLogicalFile = shouldSyncAfterClosingLogicalFile(checkpointBaseDir);
        // Initialize the managed exclusive path using id as the child path name.
        Path managedExclusivePath = new Path(taskOwnedStateDir, id);
        fs.mkdirs(managedExclusivePath);
        this.ssmManagedExclusiveStateDir = managedExclusivePath;
    }

    @Override
    public Path getSsmManagedDir(SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        if (scope.equals(CheckpointedStateScope.SHARED)) {
            return ssmManagedSharedStateDir.get(subtaskKey);
        } else {
            return ssmManagedExclusiveStateDir;
        }
    }

    boolean shouldSyncAfterClosingLogicalFile(Path checkpointDir) {
        // currently only sync for hdfs
        try {
            return checkpointDir.toUri().getScheme().equals("hdfs");
        } catch (Exception e) {
            return false;
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Map<Long, Set<LogicalFile>> getUploadedStates() {
        return uploadedStates;
    }

    @Override
    public void close() throws IOException {}
}
