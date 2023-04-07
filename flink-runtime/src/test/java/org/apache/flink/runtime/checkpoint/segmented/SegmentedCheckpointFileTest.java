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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link PhysicalFile} and {@link LogicalFile}. */
public class SegmentedCheckpointFileTest {
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final SegmentSnapshotManager.SubtaskKey subtaskKey =
            SegmentSnapshotManager.SubtaskKey.of(new TaskInfo("Testing", 100, 5, 10, 1));

    /**
     * A {@link SegmentSnapshotManager} for testing. It creates and deletes files but does not
     * maintain the meta information of files.
     */
    class TestingSegmentSnapshotManager extends SegmentSnapshotManagerBase {

        public TestingSegmentSnapshotManager(SegmentType segmentType, long maxFileSize) {
            // perform io operations in the current thread to make the test easier
            super("testing", segmentType, maxFileSize, Runnable::run);
            this.fs = LocalFileSystem.getSharedInstance();
            // this.ssmManagedDir = new Path(tmp.getRoot().getPath());
        }

        @Override
        public void initFileSystem(
                SegmentCheckpointUtils.SegmentSnapshotFileSystemInfo fileSystemInfo) {}

        @Override
        protected void tryReusingPhysicalFileAfterClosingStream(
                SubtaskKey subtaskKey, long checkpointId, PhysicalFile physicalFile) {}

        @NotNull
        @Override
        protected PhysicalFile getOrCreatePhysicalFileForCheckpoint(
                SubtaskKey subtaskKey, long checkpointID, CheckpointedStateScope scope)
                throws IOException {
            final String fileName = UUID.randomUUID().toString();
            Path filePath = new Path(tmp.getRoot().getPath(), fileName);
            FSDataOutputStream outputStream =
                    fs.create(filePath, FileSystem.WriteMode.NO_OVERWRITE);
            return new PhysicalFile(
                    outputStream,
                    filePath,
                    this.physicalFileDeleter,
                    CheckpointedStateScope.EXCLUSIVE);
        }

        @Override
        protected void endCheckpointOfAllSubtasks(long checkpointId) {}

        @Override
        protected LogicalFile createLogicalFile(
                @Nonnull PhysicalFile physicalFile, @Nonnull SubtaskKey subtaskKey) {
            LogicalFile.LogicalFileId fileID = LogicalFile.LogicalFileId.generateRandomID();
            LogicalFile logicalFile = new LogicalFile(fileID, physicalFile, subtaskKey);
            return logicalFile;
        }
    }

    SegmentSnapshotManagerBase ssm;

    @Before
    public void setup() throws IOException {
        ssm = new TestingSegmentSnapshotManager(SegmentType.SEGMENTED_ACROSS_BOUNDARY, -1);
    }

    @Test
    public void testDiscardOneLogicalFile() throws IOException {
        PhysicalFile physicalFile = createPhysicalFile();
        LogicalFile logicalFile = createLogicalFile(physicalFile);
        assertPhysicalFileExist(physicalFile);
        logicalFile.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileExist(physicalFile);
        physicalFile.close();
        assertPhysicalFileDeleted(physicalFile);
    }

    @Test
    public void testDiscardTwoLogicalFile() throws IOException {
        PhysicalFile physicalFile = createPhysicalFile();
        LogicalFile logicalFile1 = createLogicalFile(physicalFile);
        LogicalFile logicalFile2 = createLogicalFile(physicalFile);
        assertPhysicalFileExist(physicalFile);
        logicalFile1.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileExist(physicalFile);
        physicalFile.close();
        logicalFile2.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileDeleted(physicalFile);
    }

    @Test
    public void testRepeatedlyDiscardLogicalFile() throws IOException {
        PhysicalFile physicalFile = createPhysicalFile();
        LogicalFile logicalFile = createLogicalFile(physicalFile);
        assertPhysicalFileExist(physicalFile);
        physicalFile.close();
        logicalFile.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileDeleted(physicalFile);
        logicalFile.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileDeleted(physicalFile);

        PhysicalFile physicalFile1 = createPhysicalFile();
        LogicalFile logicalFile1 = createLogicalFile(physicalFile1);
        LogicalFile logicalFile2 = createLogicalFile(physicalFile1);
        logicalFile1.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileExist(physicalFile1);
        logicalFile1.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileExist(physicalFile1);
        logicalFile2.discardWithCheckpointID(0, FileOperationReason.checkpointSubsumption(0));
        assertPhysicalFileExist(physicalFile1);
        physicalFile1.close();
        assertPhysicalFileDeleted(physicalFile1);
    }

    @Test
    public void testConcurrentRefSizeModification() throws Exception {
        PhysicalFile physicalFile = createPhysicalFile();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                physicalFile.incRefCount();
                                physicalFile.incSize(100);
                                try {
                                    physicalFile.decRefCount(null);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(
                        () -> {
                            assertEquals(0, physicalFile.getRefCount());
                            assertEquals(1000, physicalFile.getSize());
                        })
                .get();
    }

    private void assertPhysicalFileDeleted(PhysicalFile physicalFile) throws IOException {
        assertFalse(physicalFile.isOpen());
        assertFalse(ssm.fs.exists(physicalFile.getFilePath()));
    }

    private void assertPhysicalFileExist(PhysicalFile physicalFile) throws IOException {
        assertTrue(physicalFile.isOpen());
        assertTrue(ssm.fs.exists(physicalFile.getFilePath()));
    }

    private PhysicalFile createPhysicalFile() throws IOException {
        SegmentSnapshotManager.SubtaskKey subtaskKey =
                SegmentSnapshotManager.SubtaskKey.of(new TaskInfo("task", 100, 5, 10, 1));
        return ssm.getOrCreatePhysicalFileForCheckpoint(
                subtaskKey, 0, CheckpointedStateScope.EXCLUSIVE);
    }

    private LogicalFile createLogicalFile(PhysicalFile physicalFile) {
        return ssm.createLogicalFile(physicalFile, subtaskKey);
    }
}
