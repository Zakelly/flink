/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.segmented.LogicalFile;
import org.apache.flink.runtime.checkpoint.segmented.SegmentCheckpointUtils;
import org.apache.flink.runtime.checkpoint.segmented.SegmentFileStateHandle;
import org.apache.flink.runtime.checkpoint.segmented.SegmentSnapshotManager;
import org.apache.flink.runtime.checkpoint.segmented.SegmentSnapshotManagerBase;
import org.apache.flink.runtime.checkpoint.segmented.SegmentType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;
import static org.apache.flink.runtime.checkpoint.segmented.SegmentType.SEGMENTED_ACROSS_BOUNDARY;
import static org.apache.flink.runtime.checkpoint.segmented.SegmentType.SEGMENTED_WITHIN_BOUNDARY;

/** Tests for {@link FsSegmentCheckpointStorageLocation}. */
public class FsSegmentCheckpointStorageLocationTest {
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    public static File sharedStateDir;
    public static File taskOwnedStateDir;

    private final Random random = new Random();

    private static final int fileStateSizeThreshold = 100;

    private final SegmentSnapshotManager.SubtaskKey subtaskKey =
            SegmentSnapshotManager.SubtaskKey.of(new TaskInfo("test", 100, 5, 10, 1));

    private final String managerId = "testing";

    @Before
    public void prepareDirectories() throws IOException {
        sharedStateDir = tmp.newFolder("sharedStateDir");
        taskOwnedStateDir = tmp.newFolder("taskOwnedStateDir");
    }

    @Test
    public void testWriteMultipleStateFilesAcrossBoundary() throws Exception {
        testWriteMultipleStateFiles(SEGMENTED_ACROSS_BOUNDARY);
    }

    @Test
    public void testWriteMultipleStateFilesWithinBoundary() throws Exception {
        testWriteMultipleStateFiles(SEGMENTED_WITHIN_BOUNDARY);
    }

    private void testWriteMultipleStateFiles(SegmentType segmentType) throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager(segmentType);
        long checkpointID = 1;
        FsSegmentCheckpointStorageLocation storageLocation =
                createFsSegmentCheckpointStorageLocation(checkpointID, ssm);
        FileSystem fs = storageLocation.getFileSystem();

        Assert.assertTrue(
                fs.exists(ssm.getSsmManagedDir(subtaskKey, CheckpointedStateScope.EXCLUSIVE)));

        int numStates = 3;
        List<byte[]> states = generateRandomByteStates(numStates, 2, 16);
        List<SegmentFileStateHandle> stateHandles = new ArrayList<>(numStates);
        for (byte[] s : states) {
            SegmentFileStateHandle segmentFileStateHandle =
                    uploadOneStateFileAndGetStateHandle(checkpointID, storageLocation, s);
            stateHandles.add(segmentFileStateHandle);
        }

        ssm.notifyCheckpointComplete(subtaskKey, checkpointID);

        // 1. verify there is only one physical file
        verifyStateHandlesAllPointToTheSameFile(stateHandles);

        // 2. verify the states can be correctly restored
        verifyRestoringStates(stateHandles, states);
    }

    @Test
    public void testCompleteAndSubsumeCheckpointWithinBoundary() throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager(SEGMENTED_WITHIN_BOUNDARY);

        // ------------------- Checkpoint:1 -------------------

        FsSegmentCheckpointStorageLocation storageLocation1 =
                createFsSegmentCheckpointStorageLocation(1, ssm);
        FileSystem fs = storageLocation1.getFileSystem();
        List<SegmentFileStateHandle> cp1StateHandles =
                uploadCheckpointStates(1, generateRandomByteStates(3, 2, 16), storageLocation1);
        ssm.notifyCheckpointComplete(subtaskKey, 1);
        verifyCheckpointFilesAllClosed(ssm, 1);

        // ------------------- Checkpoint:2 -------------------

        FsSegmentCheckpointStorageLocation storageLocation2 =
                createFsSegmentCheckpointStorageLocation(2, ssm);
        List<SegmentFileStateHandle> cp2StateHandles =
                uploadCheckpointStates(2, generateRandomByteStates(3, 2, 16), storageLocation2);

        verifyStateHandlesAllPointToTheSameFile(cp1StateHandles);
        verifyStateHandlesAllPointToTheSameFile(cp2StateHandles);
        Path cp1FilePath = cp1StateHandles.get(0).getFilePath();
        Path cp2FilePath = cp2StateHandles.get(0).getFilePath();

        // two checkpoints should use different physical files
        Assert.assertNotEquals(cp1FilePath, cp2FilePath);

        // verify that cp1 and cp2 files exist
        Assert.assertTrue(fs.exists(cp1FilePath));
        Assert.assertTrue(fs.exists(cp2FilePath));

        ssm.notifyCheckpointComplete(subtaskKey, 2);
        ssm.notifyCheckpointSubsumed(subtaskKey, 1);
        verifyCheckpointFilesAllRemoved(ssm, 1);
        verifyCheckpointFilesAllClosed(ssm, 2);

        // verify that cp1 file is deleted after cp1 is subsumed
        Assert.assertFalse(fs.exists(cp1FilePath));
        Assert.assertTrue(fs.exists(cp2FilePath));

        // ------------------- Checkpoint:3 -------------------

        FsSegmentCheckpointStorageLocation storageLocation3 =
                createFsSegmentCheckpointStorageLocation(3, ssm);
        List<SegmentFileStateHandle> cp3StateHandles =
                uploadCheckpointStates(3, generateRandomByteStates(3, 2, 16), storageLocation3);
        ssm.notifyCheckpointAborted(subtaskKey, 3);

        verifyStateHandlesAllPointToTheSameFile(cp3StateHandles);
        Path cp3FilePath = cp3StateHandles.get(0).getFilePath();

        // aborting a checkpoint should not delete the uploaded files immediately
        Assert.assertTrue(fs.exists(cp3FilePath));

        // subsuming a checkpoint should result in the cleanup of previous checkpoint files
        ssm.notifyCheckpointSubsumed(subtaskKey, 3);
        Assert.assertFalse(fs.exists(cp2FilePath));
        Assert.assertFalse(fs.exists(cp3FilePath));
    }

    @Test
    public void testCompleteAndSubsumeCheckpointAcrossBoundary() throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager(SEGMENTED_ACROSS_BOUNDARY, 32);
        FsSegmentCheckpointStorageLocation storageLocation1 =
                createFsSegmentCheckpointStorageLocation(1, ssm);
        FileSystem fs = storageLocation1.getFileSystem();

        // 1. multiple checkpoints should reuse the same file

        // ------------------- Checkpoint:1 -------------------

        List<byte[]> cp1States = generateRandomByteStates(3, 6, 6);
        List<SegmentFileStateHandle> cp1StateHandles =
                uploadCheckpointStates(1, cp1States, storageLocation1);
        ssm.notifyCheckpointComplete(subtaskKey, 1);
        verifyRestoringStates(cp1StateHandles, cp1States);

        // ------------------- Checkpoint:2 -------------------

        FsSegmentCheckpointStorageLocation storageLocation2 =
                createFsSegmentCheckpointStorageLocation(2, ssm);
        List<byte[]> cp2States = generateRandomByteStates(3, 6, 6);
        List<SegmentFileStateHandle> cp2StateHandles =
                uploadCheckpointStates(2, cp2States, storageLocation2);
        ssm.notifyCheckpointComplete(subtaskKey, 2);

        verifyStateHandlesAllPointToTheSameFile(cp1StateHandles);
        verifyStateHandlesAllPointToTheSameFile(cp2StateHandles);
        Path cp1FilePath = cp1StateHandles.get(0).getFilePath();
        Path cp2FilePath = cp2StateHandles.get(0).getFilePath();

        Assert.assertEquals(cp1FilePath, cp2FilePath);
        Assert.assertTrue(fs.exists(cp1FilePath));
        Assert.assertTrue(fs.exists(cp2FilePath));

        // 2. should open a new file when the previous one has reached the limit size

        // ------------------- Checkpoint:3 -------------------

        FsSegmentCheckpointStorageLocation storageLocation3 =
                createFsSegmentCheckpointStorageLocation(3, ssm);

        List<byte[]> cp3States = generateRandomByteStates(3, 4, 4);
        List<SegmentFileStateHandle> cp3StateHandles =
                uploadCheckpointStates(3, cp3States, storageLocation3);
        ssm.notifyCheckpointComplete(subtaskKey, 3);
        verifyStateHandlesAllPointToTheSameFile(cp3StateHandles);
        Path cp3FilePath = cp3StateHandles.get(0).getFilePath();
        Assert.assertNotEquals(cp2FilePath, cp3FilePath);
        verifyRestoringStates(cp3StateHandles, cp3States);

        // the previous file should be closed
        verifyCheckpointFilesAllClosed(ssm, 1);
        verifyCheckpointFilesAllClosed(ssm, 2);

        // 3. a large checkpoint with multiple state handles can use multiple files

        // ------------------- Checkpoint:4 -------------------

        FsSegmentCheckpointStorageLocation storageLocation4 =
                createFsSegmentCheckpointStorageLocation(4, ssm);

        List<byte[]> cp4States = generateRandomByteStates(3, 10, 10);
        List<SegmentFileStateHandle> cp4StateHandles =
                uploadCheckpointStates(4, cp4States, storageLocation4);
        ssm.notifyCheckpointComplete(subtaskKey, 4);
        Assert.assertTrue(
                cp4StateHandles.stream()
                        .map(FileStateHandle::getFilePath)
                        .collect(Collectors.toList())
                        .contains(cp3FilePath));
        Assert.assertTrue(cp4StateHandles.stream().map(FileStateHandle::getFilePath).count() > 1);
        verifyRestoringStates(cp4StateHandles, cp4States);

        // the previous file should be closed
        verifyCheckpointFilesAllClosed(ssm, 3);
    }

    @Test
    public void testCheckpointStreamClosedExceptionally() throws Exception {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager(SEGMENTED_ACROSS_BOUNDARY);

        Path filePath1 = null;
        try (FsSegmentCheckpointStateOutputStream stream1 =
                ssm.createCheckpointStateOutputStream(
                        subtaskKey, 1, CheckpointedStateScope.EXCLUSIVE)) {
            stream1.flushToFile();
            filePath1 = stream1.getFilePath();
            assertPathNotNullAndCheckExistence(filePath1, true);
            throw new IOException();
        } catch (IOException ignored) {
        }
        assertPathNotNullAndCheckExistence(filePath1, false);

        Path filePath2;
        Path filePath3 = null;
        try (FsSegmentCheckpointStateOutputStream stream2 =
                ssm.createCheckpointStateOutputStream(
                        subtaskKey, 2, CheckpointedStateScope.EXCLUSIVE)) {
            stream2.flushToFile();
            stream2.closeAndGetHandle();
            filePath2 = stream2.getFilePath();
            ssm.notifyCheckpointComplete(subtaskKey, 2);
        }

        try (FsSegmentCheckpointStateOutputStream stream3 =
                ssm.createCheckpointStateOutputStream(
                        subtaskKey, 3, CheckpointedStateScope.EXCLUSIVE)) {
            stream3.flushToFile();
            filePath3 = stream3.getFilePath();
            assertPathNotNullAndCheckExistence(filePath3, true);
            throw new IOException();
        } catch (IOException ignored) {
        }

        // stream3 is closed exceptionally, but the physical file should not be deleted because it
        // is still referenced by checkpoint-2
        Assert.assertEquals(filePath2, filePath3);
        assertPathNotNullAndCheckExistence(filePath2, true);

        ssm.notifyCheckpointSubsumed(subtaskKey, 2);
        assertPathNotNullAndCheckExistence(filePath2, false);
    }

    private void assertPathNotNullAndCheckExistence(Path path, boolean exist) throws IOException {
        Assert.assertNotNull(path);
        Assert.assertEquals(exist, path.getFileSystem().exists(path));
    }

    @Test
    public void testWritingToClosedStream() throws IOException {
        SegmentSnapshotManager ssm = createSegmentSnapshotManager(SEGMENTED_ACROSS_BOUNDARY);
        FsSegmentCheckpointStorageLocation storageLocation =
                createFsSegmentCheckpointStorageLocation(1, ssm);
        try (FsSegmentCheckpointStateOutputStream stream =
                storageLocation.createCheckpointStateOutputStream(
                        CheckpointedStateScope.EXCLUSIVE)) {
            stream.flushToFile();
            stream.closeAndGetHandle();
            stream.flushToFile();
            Assert.fail();
        } catch (IOException e) {
            Assert.assertEquals("closed", e.getMessage());
        }
    }

    @Test
    public void testWriteAndReadPositionInformation() throws Exception {
        long maxFileSize = 128;
        SegmentSnapshotManager ssm =
                createSegmentSnapshotManager(SEGMENTED_ACROSS_BOUNDARY, maxFileSize);
        FsSegmentCheckpointStorageLocation storageLocation1 =
                createFsSegmentCheckpointStorageLocation(1, ssm);

        // ------------------- Checkpoint: 1 -------------------
        int stateSize1 = 10;
        List<byte[]> cp1States = generateRandomByteStates(1, stateSize1, stateSize1);
        uploadCheckpointStates(1, cp1States, storageLocation1);
        ssm.notifyCheckpointComplete(subtaskKey, 1);

        // ------------------- Checkpoint: 2-9 -------------------
        for (int checkpointId = 2; checkpointId < 10; checkpointId++) {
            testWriteAndReadPositionInformationInCheckpoint(checkpointId, maxFileSize, ssm);
        }
    }

    private void testWriteAndReadPositionInformationInCheckpoint(
            long checkpointId, long maxFileSize, SegmentSnapshotManager ssm) throws IOException {
        FsSegmentCheckpointStorageLocation storageLocation =
                createFsSegmentCheckpointStorageLocation(checkpointId, ssm);
        // test whether the input and output streams perform position-related operations correctly
        try (FsSegmentCheckpointStateOutputStream stateOutputStream =
                storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED)) {

            // 1. Write some bytes to the file.
            int stateSize = 64;
            byte[] expectedBytes = new byte[10];
            byte[] stateValues = generateRandomBytes(stateSize);
            stateOutputStream.write(stateValues);

            // 2. Write some positions, which should be relative values in the file segments.
            //    Each of them points to a previously written byte in the file.
            for (int i = 0; i < 10; i++) {
                int position = random.nextInt(stateSize);
                byte[] positionBytes = longToBytes(position);
                expectedBytes[i] = stateValues[position];
                stateOutputStream.write(positionBytes);
            }
            SegmentFileStateHandle cpStateHandle = stateOutputStream.closeAndGetHandle();
            Assert.assertNotNull(cpStateHandle);

            // 3. Read from the file.
            //    It repeatedly reads a position value -> seek to the corresponding position to read
            //    the expected bytes -> seek back to read the next position value.
            byte[] actualBytes = new byte[10];
            byte[] oneByte = new byte[1];
            FSDataInputStream inputStream = cpStateHandle.openInputStream();
            Assert.assertNotNull(inputStream);
            inputStream.seek(stateSize);
            for (int i = 0; i < 10; i++) {
                byte[] longBytes = new byte[8];
                int readContent = inputStream.read(longBytes);
                Assert.assertEquals(8, readContent);
                inputStream.mark((int) maxFileSize);
                inputStream.seek(bytesToLong(longBytes));
                Assert.assertTrue(inputStream.read(oneByte) >= 0);
                actualBytes[i] = oneByte[0];
                inputStream.reset();
            }
            Assert.assertTrue(bytesEqual(expectedBytes, actualBytes));
        }
    }

    private SegmentSnapshotManager createSegmentSnapshotManager(SegmentType segmentType) {
        return createSegmentSnapshotManager(segmentType, -1);
    }

    private SegmentSnapshotManager createSegmentSnapshotManager(
            SegmentType segmentType, long maxFileSize) {
        try {
            SegmentSnapshotManager ssm =
                    SegmentCheckpointUtils.createSegmentSnapshotManager(
                            managerId, segmentType, maxFileSize, false);
            ssm.initFileSystem(
                    SegmentCheckpointUtils.packFileSystemInfo(
                            getSharedInstance(),
                            new Path(tmp.getRoot().getPath()),
                            fromLocalFile(sharedStateDir),
                            fromLocalFile(taskOwnedStateDir),
                            null,
                            fileStateSizeThreshold,
                            fileStateSizeThreshold));
            return ssm;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FsSegmentCheckpointStorageLocation createFsSegmentCheckpointStorageLocation(
            long checkpointId, @Nonnull SegmentSnapshotManager ssm) throws IOException {
        int threshold = 100;
        File checkpointsDir = tmp.newFolder("checkpointsDir-" + checkpointId);
        CheckpointStorageLocationReference cslReference =
                AbstractFsCheckpointStorageAccess.encodePathAsReference(
                        fromLocalFile(checkpointsDir));
        Assert.assertNotNull(ssm);

        return new FsSegmentCheckpointStorageLocation(
                subtaskKey,
                getSharedInstance(),
                fromLocalFile(checkpointsDir.getParentFile()),
                fromLocalFile(sharedStateDir),
                fromLocalFile(taskOwnedStateDir),
                cslReference,
                threshold,
                threshold,
                ssm,
                checkpointId);
    }

    private SegmentFileStateHandle uploadOneStateFileAndGetStateHandle(
            long checkpointID,
            FsSegmentCheckpointStorageLocation storageLocation,
            byte[] stateContent)
            throws IOException {

        // upload a (logical) state file
        try (FsSegmentCheckpointStateOutputStream stateOutputStream =
                storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED)) {
            stateOutputStream.write(stateContent);
            return stateOutputStream.closeAndGetHandle();
        }
    }

    private boolean bytesEqual(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == null || bytes2 == null) {
            return false;
        }

        if (bytes1.length == bytes2.length) {
            for (int i = 0; i < bytes1.length; i++) {
                if (bytes1[i] != bytes2[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private List<SegmentFileStateHandle> uploadCheckpointStates(
            long checkpointID,
            List<byte[]> states,
            FsSegmentCheckpointStorageLocation storageLocation)
            throws IOException {
        List<SegmentFileStateHandle> stateHandles = new ArrayList<>(states.size());
        for (byte[] state : states) {
            SegmentFileStateHandle segmentFileStateHandle =
                    uploadOneStateFileAndGetStateHandle(checkpointID, storageLocation, state);
            stateHandles.add(segmentFileStateHandle);
        }
        return stateHandles;
    }

    private byte[] generateRandomBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private List<byte[]> generateRandomByteStates(
            int numStates, int perStateMinSize, int perStateMaxSize) {
        List<byte[]> result = new ArrayList<>(numStates);
        for (int i = 0; i < numStates; i++) {
            int stateSize = random.nextInt(perStateMaxSize - perStateMinSize + 1) + perStateMinSize;
            result.add(generateRandomBytes(stateSize));
        }
        return result;
    }

    private void verifyStateHandlesAllPointToTheSameFile(
            List<SegmentFileStateHandle> stateHandles) {
        Path lastFilePath = null;
        for (SegmentFileStateHandle stateHandle : stateHandles) {
            Assert.assertTrue(
                    lastFilePath == null || lastFilePath.equals(stateHandle.getFilePath()));
            lastFilePath = stateHandle.getFilePath();
        }
    }

    private void verifyRestoringStates(
            List<SegmentFileStateHandle> stateHandles, List<byte[]> states) {
        Assert.assertEquals(stateHandles.size(), states.size());
        for (int i = 0; i < stateHandles.size(); i++) {
            FileStateHandle stateHandle = stateHandles.get(i);
            try (FSDataInputStream inputStream = stateHandle.openInputStream()) {
                long stateSize = stateHandle.getStateSize();
                byte[] bytes = new byte[(int) stateSize];
                inputStream.read(bytes);
                Assert.assertTrue(bytesEqual(bytes, states.get(i)));
            } catch (Exception e) {
                Assert.fail("Error reading states from state handles: " + e.getMessage());
            }
        }
    }

    private void verifyCheckpointFilesAllRemoved(
            SegmentSnapshotManager segmentSnapshotManager, long checkpointID) {
        SegmentSnapshotManagerBase ssm = (SegmentSnapshotManagerBase) segmentSnapshotManager;
        Set<LogicalFile> logicalFiles = ssm.getUploadedStates().get(checkpointID);
        Assert.assertNull(logicalFiles);
    }

    private void verifyCheckpointFilesAllClosed(
            SegmentSnapshotManager segmentSnapshotManager, long checkpointID) {
        SegmentSnapshotManagerBase ssm = (SegmentSnapshotManagerBase) segmentSnapshotManager;

        Set<LogicalFile> logicalFiles = ssm.getUploadedStates().get(checkpointID);
        if (logicalFiles != null) {
            logicalFiles.forEach(
                    logicalFile -> {
                        if (logicalFile.getPhysicalFile() != null) {
                            Assert.assertFalse(logicalFile.getPhysicalFile().isOpen());
                        }
                    });
        }
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }
}
