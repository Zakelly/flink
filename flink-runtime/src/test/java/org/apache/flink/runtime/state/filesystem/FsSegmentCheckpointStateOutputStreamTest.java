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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.segmented.PhysicalFile;
import org.apache.flink.runtime.checkpoint.segmented.SegmentFileStateHandle;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/** Tests for {@link FsSegmentCheckpointStateOutputStream}. */
public class FsSegmentCheckpointStateOutputStreamTest {

    @Rule public final TemporaryFolder tempDir = new TemporaryFolder();

    private static boolean failWhenClosePhysicalFile = false;

    private static final String CLOSE_FILE_FAILURE_MESSAGE = "Cannot close physical file.";

    private static final int WRITE_BUFFER_SIZE = 256;

    private static boolean isPhysicalFileProvided = false;

    private static boolean physicalFileCanBeReused;

    private static PhysicalFile lastPhysicalFile;

    @Before
    public void setEnv() {
        failWhenClosePhysicalFile = false;
        physicalFileCanBeReused = false;
    }

    private FsSegmentCheckpointStateOutputStream getNewStream() throws IOException {
        return getNewStream(false);
    }

    private FsSegmentCheckpointStateOutputStream getNewStream(boolean reuseLastPhysicalFile)
            throws IOException {

        PhysicalFile physicalFile;
        if (reuseLastPhysicalFile) {
            Assert.assertNotNull(lastPhysicalFile);
            physicalFile = lastPhysicalFile;
        } else {
            Path dirPath = Path.fromLocalFile(tempDir.newFolder());
            String fileName = UUID.randomUUID().toString();
            Path physicalFilePath = new Path(dirPath, fileName);
            OutputStreamAndPath streamAndPath =
                    EntropyInjector.createEntropyAware(
                            dirPath.getFileSystem(),
                            physicalFilePath,
                            FileSystem.WriteMode.NO_OVERWRITE);
            physicalFile =
                    new PhysicalFile(
                            streamAndPath.stream(),
                            physicalFilePath,
                            (stream, path, reason) -> {},
                            EXCLUSIVE);
        }
        isPhysicalFileProvided = false;

        // a simplified implementation that excludes the meta info management of files
        return new FsSegmentCheckpointStateOutputStream(
                1,
                WRITE_BUFFER_SIZE,
                new FsSegmentCheckpointStateOutputStream
                        .SegmentedCheckpointStateOutputStreamHandler() {
                    @Override
                    public Tuple2<FSDataOutputStream, Path> providePhysicalFile() {
                        isPhysicalFileProvided = true;
                        lastPhysicalFile = physicalFile;

                        Preconditions.checkArgument(physicalFile.isOpen());
                        return new Tuple2<>(
                                physicalFile.getOutputStream(), physicalFile.getFilePath());
                    }

                    @Override
                    public SegmentFileStateHandle closeStreamAndCreateStateHandle(
                            Path filePath, long startPos, long stateSize) throws IOException {
                        if (isPhysicalFileProvided) {
                            if (failWhenClosePhysicalFile) {
                                throw new IOException(CLOSE_FILE_FAILURE_MESSAGE);
                            } else if (!physicalFileCanBeReused) {
                                physicalFile.close();
                            }
                        }
                        return new SegmentFileStateHandle(
                                filePath, startPos, stateSize, null, EXCLUSIVE);
                    }

                    @Override
                    public void closeStream() throws IOException {
                        if (isPhysicalFileProvided) {
                            if (failWhenClosePhysicalFile) {
                                throw new IOException(CLOSE_FILE_FAILURE_MESSAGE);
                            } else {
                                physicalFile.close();
                            }
                        }
                    }
                });
    }

    @Test
    public void testGetHandleFromStream() throws IOException {

        FsSegmentCheckpointStateOutputStream stream = getNewStream();
        Assert.assertFalse(isPhysicalFileProvided);
        Assert.assertNull(stream.closeAndGetHandle());

        stream = getNewStream();
        stream.flush();
        Assert.assertFalse(isPhysicalFileProvided);
        Assert.assertNull(stream.closeAndGetHandle());

        // return a non-null state handle if flushToFile has been called even if nothing was written
        stream = getNewStream();
        stream.flushToFile();
        Assert.assertTrue(isPhysicalFileProvided);
        SegmentFileStateHandle stateHandle = stream.closeAndGetHandle();
        Assert.assertNotNull(stateHandle);
        Assert.assertEquals(0, stateHandle.getStateSize());

        stream = getNewStream();
        stream.write(new byte[0]);
        stream.flushToFile();
        stateHandle = stream.closeAndGetHandle();
        Assert.assertNotNull(stateHandle);
        Assert.assertEquals(0, stateHandle.getStateSize());

        stream = getNewStream();
        stream.write(new byte[10]);
        stream.flushToFile();
        stateHandle = stream.closeAndGetHandle();
        Assert.assertNotNull(stateHandle);
        Assert.assertEquals(10, stateHandle.getStateSize());

        // closeAndGetHandle should internally call flushToFile
        stream = getNewStream();
        stream.write(new byte[10]);
        stateHandle = stream.closeAndGetHandle();
        Assert.assertNotNull(stateHandle);
        Assert.assertEquals(10, stateHandle.getStateSize());
    }

    @Test
    public void testGetHandleFromClosedStream() throws IOException {
        FsSegmentCheckpointStateOutputStream stream = getNewStream();
        stream.close();
        try {
            stream.closeAndGetHandle();
        } catch (Exception ignored) {
            // expected
        }
    }

    @Test
    public void testWhetherFileIsCreatedWhenWritingStream() throws IOException {

        FsSegmentCheckpointStateOutputStream stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE - 1]);
        Assert.assertFalse(isPhysicalFileProvided);
        stream.write(new byte[2]);
        Assert.assertTrue(isPhysicalFileProvided);

        stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE]);
        Assert.assertTrue(isPhysicalFileProvided);

        stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE - 1]);
        stream.close();
        Assert.assertFalse(isPhysicalFileProvided);

        stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE - 1]);
        stream.closeAndGetHandle();
        Assert.assertTrue(isPhysicalFileProvided);
    }

    @Test
    public void testCloseStream() throws IOException {

        // cannot write anything to a closed stream
        FsSegmentCheckpointStateOutputStream stream = getNewStream();
        stream.flushToFile();
        stream.close();
        stream.write(new byte[0]);
        try {
            stream.write(new byte[1]);
        } catch (IOException e) {
            Assert.assertEquals("closed", e.getMessage());
        }

        failWhenClosePhysicalFile = true;

        // close() throws no exception if it fails to close the file
        stream = getNewStream();
        stream.flushToFile();
        Assert.assertTrue(isPhysicalFileProvided);

        stream.close();

        // closeAndGetHandle() throws exception if it fails to close the file
        stream = getNewStream();
        stream.flushToFile();
        Assert.assertTrue(isPhysicalFileProvided);
        try {
            stream.closeAndGetHandle();
        } catch (IOException e) {
            if (!e.getMessage().equals(CLOSE_FILE_FAILURE_MESSAGE)) {
                throw e;
            }
        }
    }

    @Test
    public void testStateAboveBufferSize() throws Exception {
        runTest(576446);
    }

    @Test
    public void testStateUnderBufferSize() throws Exception {
        runTest(100);
    }

    @Test
    public void testGetPos() throws Exception {
        FsSegmentCheckpointStateOutputStream stream = getNewStream();

        // write one byte one time
        for (int i = 0; i < 64; ++i) {
            Assert.assertEquals(i, stream.getPos());
            stream.write(0x42);
        }

        stream.closeAndGetHandle();

        // write random number of bytes one time
        stream = getNewStream();

        Random rnd = new Random();
        long expectedPos = 0;
        for (int i = 0; i < 7; ++i) {
            int numBytes = rnd.nextInt(16);
            expectedPos += numBytes;
            stream.write(new byte[numBytes]);
            Assert.assertEquals(expectedPos, stream.getPos());
        }

        physicalFileCanBeReused = true;
        SegmentFileStateHandle stateHandle = stream.closeAndGetHandle();

        // reuse the last physical file
        Assert.assertNotNull(stateHandle);
        expectedPos = 0;
        stream = getNewStream(true);
        stream.flushToFile();
        for (int i = 0; i < 7; ++i) {
            int numBytes = rnd.nextInt(16);
            expectedPos += numBytes;
            stream.write(new byte[numBytes]);
            Assert.assertEquals(expectedPos, stream.getPos());
        }

        stream.closeAndGetHandle();
    }

    @Test
    public void testCannotReuseClosedFile() throws IOException {
        FsSegmentCheckpointStateOutputStream stream = getNewStream();
        stream.flushToFile();
        Assert.assertTrue(isPhysicalFileProvided);

        stream.close();
        stream = getNewStream(true);
        try {
            stream.flushToFile();
            fail("Cannot reuse a closed physical file.");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testWriteFailsFastWhenClosed() throws Exception {
        FsSegmentCheckpointStateOutputStream stream = getNewStream();
        stream.flushToFile();
        Assert.assertTrue(isPhysicalFileProvided);

        stream.close();
        try {
            stream.write(1);
            fail();
        } catch (IOException ignored) {
            // expected
        }
    }

    private void runTest(int numBytes) throws Exception {
        CheckpointStateOutputStream stream = getNewStream();

        Random rnd = new Random();
        byte[] original = new byte[numBytes];
        byte[] bytes = new byte[original.length];

        rnd.nextBytes(original);
        System.arraycopy(original, 0, bytes, 0, original.length);

        // the test writes a mixture of writing individual bytes and byte arrays
        int pos = 0;
        while (pos < bytes.length) {
            boolean single = rnd.nextBoolean();
            if (single) {
                stream.write(bytes[pos++]);
            } else {
                int num =
                        rnd.nextBoolean() ? (bytes.length - pos) : rnd.nextInt(bytes.length - pos);
                stream.write(bytes, pos, num);
                pos += num;
            }
        }

        StreamStateHandle handle = stream.closeAndGetHandle();
        Assert.assertNotNull(handle);

        // make sure the writing process did not alter the original byte array
        assertArrayEquals(original, bytes);

        try (InputStream inStream = handle.openInputStream()) {
            byte[] validation = new byte[bytes.length];

            DataInputStream dataInputStream = new DataInputStream(inStream);
            dataInputStream.readFully(validation);

            assertArrayEquals(bytes, validation);
        }

        handle.discardState();
    }
}
