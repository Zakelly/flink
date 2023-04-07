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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsSegmentCheckpointStateOutputStream;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.segmented.SegmentType.SEGMENTED_ACROSS_BOUNDARY;
import static org.apache.flink.runtime.checkpoint.segmented.SegmentType.SEGMENTED_WITHIN_BOUNDARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link SegmentSnapshotManager}. */
@RunWith(Parameterized.class)
public class SegmentSnapshotManagerTest {

    @Parameterized.Parameters(name = "segmentType = {0}")
    public static List<SegmentType> parameters() {
        return Arrays.asList(SEGMENTED_ACROSS_BOUNDARY, SEGMENTED_WITHIN_BOUNDARY);
    }

    @Parameterized.Parameter public SegmentType segmentType;

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private Environment env;

    private String tmId = "Testing";

    private SegmentSnapshotManager.SubtaskKey subtaskKey;

    // use simplified job ids for the tests
    private long jobId;

    private final int maxPhysicalFileSize = 128;

    @Before
    public void setup() {
        env = createMockEnvironment();
        jobId = 1;
        subtaskKey = SegmentSnapshotManager.SubtaskKey.of(env.getTaskInfo());
    }

    private void advanceJobId() {
        jobId++;
    }

    private static final List<LogicalFile> EMPTY_LOGICAL_FILE_LIST = new ArrayList<>();

    @Test
    public void testRestoreSegmentSnapshotManager() throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager();
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            SegmentFileStateHandle lastCheckpoint = null;
            long lastCheckpointId = 0;

            for (long checkpointId = 1; checkpointId < 17; checkpointId++) {
                FsSegmentCheckpointStateOutputStream stream =
                        writeCheckpointAndGetStream(checkpointId, ssm, closeableRegistry);
                if (checkpointId % 4 == 0) {
                    // fail some checkpoints
                    ssm.notifyCheckpointAborted(subtaskKey, checkpointId);
                } else {
                    lastCheckpoint = stream.closeAndGetHandle();
                    ssm.notifyCheckpointComplete(subtaskKey, checkpointId);
                    ssm.notifyCheckpointSubsumed(subtaskKey, checkpointId - 2);
                    lastCheckpointId = checkpointId;
                }
                SegmentSnapshotManager restored =
                        restoreSegmentSnapshotManager(
                                lastCheckpointId, Collections.singleton(lastCheckpoint));
                assertNotNull(restored);
                checkRestoredSegmentSnapshotManager(ssm, restored, lastCheckpointId);
            }
        }
    }

    @Test
    public void testRestoreSegmentSnapshotManagerAfterFailOver() throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager();
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            FsSegmentCheckpointStateOutputStream cp1Stream =
                    writeCheckpointAndGetStream(1, ssm, closeableRegistry);
            SegmentFileStateHandle cp1StateHandle = cp1Stream.closeAndGetHandle();
            ssm.notifyCheckpointComplete(subtaskKey, 1);

            SegmentSnapshotManager restored =
                    restoreSegmentSnapshotManager(1, Collections.singleton(cp1StateHandle));

            checkRestoredSegmentSnapshotManager(ssm, restored, 1);

            // complete checkpoint-2
            FsSegmentCheckpointStateOutputStream cp2Stream =
                    writeCheckpointAndGetStream(
                            2, restored, closeableRegistry, maxPhysicalFileSize);
            SegmentFileStateHandle cp2StateHandle = cp2Stream.closeAndGetHandle();
            restored.notifyCheckpointComplete(subtaskKey, 2);
            assertFileInSsmManagedDir(restored, cp2StateHandle);

            // subsume checkpoint-1
            restored.notifyCheckpointSubsumed(subtaskKey, 1);
            assertFalse(fileExists(cp1StateHandle));

            // subsume checkpoint-2
            restored.notifyCheckpointSubsumed(subtaskKey, 2);
            assertFalse(fileExists(cp2StateHandle));
        }
    }

    @Test
    public void testRestoreSegmentSnapshotManagerAfterRescaling() throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager();
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            FsSegmentCheckpointStateOutputStream cp1Stream =
                    writeCheckpointAndGetStream(1, ssm, closeableRegistry);
            SegmentFileStateHandle cp1StateHandle = cp1Stream.closeAndGetHandle();
            ssm.notifyCheckpointComplete(subtaskKey, 1);

            // update the job id like in a rescaling operation
            advanceJobId();

            // restore 2 ssm from checkpoint-1
            SegmentSnapshotManager restored1 =
                    restoreSegmentSnapshotManager(1, Collections.singleton(cp1StateHandle));
            SegmentSnapshotManager restored2 =
                    restoreSegmentSnapshotManager(1, Collections.singleton(cp1StateHandle));

            checkRestoredSegmentSnapshotManager(ssm, restored1, 1);
            checkRestoredSegmentSnapshotManager(ssm, restored2, 1);

            // complete checkpoint-2
            FsSegmentCheckpointStateOutputStream cp2Stream1 =
                    writeCheckpointAndGetStream(
                            2, restored1, closeableRegistry, maxPhysicalFileSize);
            SegmentFileStateHandle cp2StateHandle1 = cp2Stream1.closeAndGetHandle();
            restored1.notifyCheckpointComplete(subtaskKey, 2);
            FsSegmentCheckpointStateOutputStream cp2Stream2 =
                    writeCheckpointAndGetStream(
                            2, restored2, closeableRegistry, maxPhysicalFileSize);
            SegmentFileStateHandle cp2StateHandle2 = cp2Stream2.closeAndGetHandle();
            restored2.notifyCheckpointComplete(subtaskKey, 2);
            assertFileInSsmManagedDir(restored1, cp2StateHandle1);
            assertFileInSsmManagedDir(restored2, cp2StateHandle2);

            // subsume checkpoint-1
            restored1.notifyCheckpointSubsumed(subtaskKey, 1);
            restored2.notifyCheckpointSubsumed(subtaskKey, 1);
            assertTrue(fileExists(cp1StateHandle));

            // subsume checkpoint-2
            restored1.notifyCheckpointSubsumed(subtaskKey, 2);
            restored2.notifyCheckpointSubsumed(subtaskKey, 2);
            assertFalse(fileExists(cp2StateHandle1));
            assertFalse(fileExists(cp2StateHandle2));
        }
    }

    @Test
    public void testCreateSegmentSnapshotManager() throws IOException {
        FileSystem fs = LocalFileSystem.getSharedInstance();
        Path checkpointBaseDir = new Path(tmp.getRoot().toString(), String.valueOf(jobId));
        Path sharedStateDir = new Path(checkpointBaseDir, "shared");
        Path taskOwnedStateDir = new Path(checkpointBaseDir, "taskowned");
        if (!fs.exists(checkpointBaseDir)) {
            fs.mkdirs(checkpointBaseDir);
            fs.mkdirs(sharedStateDir);
            fs.mkdirs(taskOwnedStateDir);
        }
        SegmentSnapshotManager ssm =
                SegmentCheckpointUtils.createSegmentSnapshotManager(
                        tmId, segmentType, maxPhysicalFileSize, false);
        ssm.initFileSystem(
                SegmentCheckpointUtils.packFileSystemInfo(
                        LocalFileSystem.getSharedInstance(),
                        checkpointBaseDir,
                        sharedStateDir,
                        taskOwnedStateDir,
                        null,
                        256,
                        256));
        assertNotNull(ssm);
        assertTrue(ssm instanceof SegmentSnapshotManagerBase);
        SegmentSnapshotManagerBase created = (SegmentSnapshotManagerBase) ssm;
        assertEquals(
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR
                                + "/"
                                + tmId),
                created.getSsmManagedDir(subtaskKey, CheckpointedStateScope.EXCLUSIVE));
        assertEquals(
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR
                                + "/"
                                + subtaskKey.getManagedDirName()),
                created.getSsmManagedDir(subtaskKey, CheckpointedStateScope.SHARED));
    }

    @Test
    public void testConcurrentWriting() throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager();
        long checkpointId = 1;
        int numThreads = 12;
        int perStreamWriteNum = 128;
        Set<Future<SegmentFileStateHandle>> futures = new HashSet<>();
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            // write data concurrently
            for (int i = 0; i < numThreads; i++) {
                futures.add(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    FsSegmentCheckpointStateOutputStream stream =
                                            ssm.createCheckpointStateOutputStream(
                                                    subtaskKey,
                                                    checkpointId,
                                                    CheckpointedStateScope.EXCLUSIVE);
                                    try {
                                        closeableRegistry.registerCloseable(stream);
                                        for (int j = 0; j < perStreamWriteNum; j++) {
                                            stream.write(j);
                                        }
                                        return stream.closeAndGetHandle();
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }));
            }

            // assert that multiple segments in the same file were not written concurrently
            for (Future<SegmentFileStateHandle> future : futures) {
                SegmentFileStateHandle segmentFileStateHandle = future.get();
                FSDataInputStream is = segmentFileStateHandle.openInputStream();
                closeableRegistry.registerCloseable(is);
                int readValue;
                int expected = 0;
                while ((readValue = is.read()) != -1) {
                    assertEquals(expected++, readValue);
                }
            }
        }
    }

    private SegmentSnapshotManager createSegmentSnapshotManager() throws IOException {
        FileSystem fs = LocalFileSystem.getSharedInstance();
        Path checkpointBaseDir = new Path(tmp.getRoot().toString(), String.valueOf(jobId));
        Path sharedStateDir =
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR);
        Path taskOwnedStateDir =
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR);
        if (!fs.exists(checkpointBaseDir)) {
            fs.mkdirs(checkpointBaseDir);
            fs.mkdirs(sharedStateDir);
            fs.mkdirs(taskOwnedStateDir);
        }
        SegmentSnapshotManager ssm =
                SegmentCheckpointUtils.createSegmentSnapshotManager(
                        tmId, segmentType, maxPhysicalFileSize, false);
        ssm.initFileSystem(
                SegmentCheckpointUtils.packFileSystemInfo(
                        LocalFileSystem.getSharedInstance(),
                        checkpointBaseDir,
                        sharedStateDir,
                        taskOwnedStateDir,
                        null,
                        256,
                        256));
        assertNotNull(ssm);
        return ssm;
    }

    private SegmentSnapshotManager restoreSegmentSnapshotManager(
            long checkpointId, Collection<SegmentFileStateHandle> stateHandles) throws IOException {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager();
        assertNotNull(ssm);

        ArrayList<StateObjectCollection<OperatorStateHandle>> list = new ArrayList<>();
        assertTrue(ssm instanceof SegmentSnapshotManagerBase);
        ((SegmentSnapshotManagerBase) ssm)
                .addStateHandles(checkpointId, subtaskKey, stateHandles, (e) -> false);

        return ssm;
    }

    private FsSegmentCheckpointStateOutputStream writeCheckpointAndGetStream(
            long checkpointId, SegmentSnapshotManager ssm, CloseableRegistry closeableRegistry)
            throws IOException {
        return writeCheckpointAndGetStream(checkpointId, ssm, closeableRegistry, 32);
    }

    private FsSegmentCheckpointStateOutputStream writeCheckpointAndGetStream(
            long checkpointId,
            SegmentSnapshotManager ssm,
            CloseableRegistry closeableRegistry,
            int numBytes)
            throws IOException {
        FsSegmentCheckpointStateOutputStream stream =
                ssm.createCheckpointStateOutputStream(
                        subtaskKey, checkpointId, CheckpointedStateScope.EXCLUSIVE);
        closeableRegistry.registerCloseable(stream);
        for (int i = 0; i < numBytes; i++) {
            stream.write(i);
        }
        return stream;
    }

    private void assertFileInSsmManagedDir(
            SegmentSnapshotManager ssm, SegmentFileStateHandle stateHandle) {
        assertTrue(ssm instanceof SegmentSnapshotManagerBase);
        assertNotNull(stateHandle);
        Path filePath = stateHandle.getFilePath();
        assertNotNull(filePath);
        assertTrue(((SegmentSnapshotManagerBase) ssm).isResponsibleForFile(filePath));
    }

    private boolean fileExists(SegmentFileStateHandle stateHandle) throws IOException {
        assertNotNull(stateHandle);
        Path filePath = stateHandle.getFilePath();
        assertNotNull(filePath);
        return filePath.getFileSystem().exists(filePath);
    }

    private static Environment createMockEnvironment() {
        Environment env = mock(Environment.class);
        when(env.getExecutionConfig()).thenReturn(new ExecutionConfig());
        when(env.getTaskConfiguration()).thenReturn(new Configuration());
        when(env.getUserCodeClassLoader())
                .thenReturn(TestingUserCodeClassLoader.newBuilder().build());

        TaskInfo taskInfo = mock(TaskInfo.class);
        when(taskInfo.getTaskName()).thenReturn("testTask");
        when(taskInfo.getIndexOfThisSubtask()).thenReturn(0);
        when(taskInfo.getNumberOfParallelSubtasks()).thenReturn(1);
        when(env.getTaskInfo()).thenReturn(taskInfo);

        return env;
    }

    private void checkRestoredSegmentSnapshotManager(
            SegmentSnapshotManager original,
            SegmentSnapshotManager restored,
            long lastCheckpointId) {
        // check whether the metadata in ssm has been correctly restored from the checkpoint
        checkUploadedFileSetEqual(original, restored, lastCheckpointId);
    }

    private List<LogicalFile> getSortedLogicalFiles(SegmentSnapshotManager ssm, long checkpointId) {
        Set<LogicalFile> logicalFiles =
                ((SegmentSnapshotManagerBase) ssm).getUploadedStates().get(checkpointId);
        return logicalFiles == null || logicalFiles.isEmpty()
                ? EMPTY_LOGICAL_FILE_LIST
                : logicalFiles.stream()
                        .sorted(Comparator.comparing(a -> a.fileID.getKeyString()))
                        .collect(Collectors.toList());
    }

    private void checkUploadedFileSetEqual(
            SegmentSnapshotManager ssm1, SegmentSnapshotManager ssm2, long checkpointId) {
        List<LogicalFile> list1 = getSortedLogicalFiles(ssm1, checkpointId);
        List<LogicalFile> list2 = getSortedLogicalFiles(ssm2, checkpointId);
        assertEquals(list1.size(), list2.size());
        Iterator<LogicalFile> it1 = list1.listIterator();
        Iterator<LogicalFile> it2 = list2.listIterator();
        while (it1.hasNext()) {
            assertTrue(it2.hasNext());
            assertTrue(it1.next().identicalTo(it2.next()));
        }
        assertFalse(it2.hasNext());
    }
}
