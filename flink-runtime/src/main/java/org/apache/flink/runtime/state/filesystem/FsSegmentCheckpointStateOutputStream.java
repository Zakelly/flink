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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.segmented.SegmentFileStateHandle;
import org.apache.flink.runtime.checkpoint.segmented.SegmentSnapshotManager;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link CheckpointStateOutputStream} that writes into a file and returns a {@link
 * SegmentFileStateHandle} upon closing. Multiple {@link FsSegmentCheckpointStateOutputStream}
 * objects can reuse the same underlying file.
 *
 * <p><strong>Important</strong>: This implementation is not thread-safe. Multiple data streams
 * multiplexing the same file should not write files concurrently. Instead, it is expected that only
 * after one data stream is closed, will other data streams reuse and write to the same underlying
 * file.
 */
public class FsSegmentCheckpointStateOutputStream
        extends FsCheckpointStreamFactory.FsCheckpointStateOutputStream {

    /**
     * A manager which stands between a {@link SegmentSnapshotManager} and a {@link
     * FsSegmentCheckpointStateOutputStream}. It works as a delegation of the {@link
     * SegmentSnapshotManager} to deal with the physical file of the {@link
     * FsSegmentCheckpointStateOutputStream}.
     */
    public abstract static class SegmentedCheckpointStateOutputStreamHandler {
        /**
         * Provide a physical file.
         *
         * @return Output stream and path of the physical file.
         * @throws IOException if the physical file cannot be created or opened.
         */
        public abstract Tuple2<FSDataOutputStream, Path> providePhysicalFile() throws IOException;

        /**
         * Close the stream and create a {@link SegmentFileStateHandle} for a file segment.
         *
         * @param filePath Path of the physical file.
         * @param startPos Start position of the segment in the physical file.
         * @param stateSize Size of the segment.
         * @return The state handle of the segment.
         * @throws IOException Thrown, if any exception happens when closing/deleting file.
         */
        public abstract SegmentFileStateHandle closeStreamAndCreateStateHandle(
                Path filePath, long startPos, long stateSize) throws IOException;

        /**
         * Notify the {@link SegmentSnapshotManager} that the stream is closed exceptionally.
         *
         * @throws IOException Thrown, if any exception happens when closing/deleting file.
         */
        public abstract void closeStream() throws IOException;
    }

    private static final Logger LOG =
            LoggerFactory.getLogger(FsSegmentCheckpointStateOutputStream.class);

    private @Nullable FSDataOutputStream outputStream;

    private final SegmentedCheckpointStateOutputStreamHandler ssmDelegation;

    private final byte[] writeBuffer;

    /** pos in the writeBuffer. */
    int bufferPos;

    /** pos in the physical file (relative to the starting position). */
    long filePosRelative = 0;

    private long startPos;

    private Path filePath;

    private volatile boolean closed;

    public FsSegmentCheckpointStateOutputStream(
            long checkpointID,
            int bufferSize,
            SegmentedCheckpointStateOutputStreamHandler ssmDelegation) {
        super(null, null, bufferSize, -1);
        this.ssmDelegation = ssmDelegation;
        this.writeBuffer = new byte[bufferSize];
    }

    @Override
    public long getPos() throws IOException {
        // The starting position is not determined until a physical file has been assigned, so
        // we return the relative value to the starting position in this method
        return bufferPos + filePosRelative;
    }

    @Override
    public void write(int b) throws IOException {
        if (bufferPos >= writeBuffer.length) {
            flushToFile();
        }
        writeBuffer[bufferPos++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len < writeBuffer.length) {
            // copy it into our write buffer first
            final int remaining = writeBuffer.length - bufferPos;
            if (len > remaining) {
                // copy as much as fits
                System.arraycopy(b, off, writeBuffer, bufferPos, remaining);
                off += remaining;
                len -= remaining;
                bufferPos += remaining;

                // flushToFile the write buffer to make it clear again
                flushToFile();
            }

            // copy what is in the buffer
            System.arraycopy(b, off, writeBuffer, bufferPos, len);
            bufferPos += len;
        } else {
            // flushToFile the current buffer
            flushToFile();
            // write the bytes directly
            outputStream.write(b, off, len);
            filePosRelative += len;
        }
    }

    @Override
    public void flush() throws IOException {
        if (outputStream != null) {
            flushToFile();
        }
    }

    @Override
    public void sync() throws IOException {
        if (outputStream != null) {
            outputStream.sync();
        }
    }

    @Nullable
    @Override
    public SegmentFileStateHandle closeAndGetHandle() throws IOException {
        // check if there was nothing ever written
        if (outputStream == null && bufferPos == 0) {
            return null;
        }

        synchronized (this) {
            if (!closed) {
                try {
                    flushToFile();

                    bufferPos = writeBuffer.length;

                    return ssmDelegation.closeStreamAndCreateStateHandle(
                            filePath, startPos, filePosRelative);
                } catch (Exception exception) {
                    ssmDelegation.closeStream();

                    throw new IOException(
                            "Could not flush to file and close the file system "
                                    + "output stream to "
                                    + filePath
                                    + " in order to obtain the "
                                    + "stream state handle",
                            exception);
                } finally {
                    closed = true;
                }
            } else {
                throw new IOException("Stream has already been closed and discarded.");
            }
        }
    }

    /**
     * This method throws no exception if the deletion fails, but only logs the error. This is to be
     * consistent with {@link FsCheckpointStreamFactory.FsCheckpointStateOutputStream#close()}.
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;

            // make sure write requests need to go to 'flushToFile()' where they recognized
            // that the stream is closed
            bufferPos = writeBuffer.length;
            try {
                ssmDelegation.closeStream();
            } catch (Throwable throwable) {
                LOG.warn("Could not close the state stream for {}.", filePath, throwable);
            }
        }
    }

    public void flushToFile() throws IOException {
        if (!closed) {
            if (outputStream == null) {
                initializeOutputStream();
            }

            if (bufferPos > 0) {
                outputStream.write(writeBuffer, 0, bufferPos);
                filePosRelative += bufferPos;
                bufferPos = 0;
            }
        } else {
            throw new IOException("closed");
        }
    }

    private void initializeOutputStream() throws IOException {
        Tuple2<FSDataOutputStream, Path> streamAndPath = ssmDelegation.providePhysicalFile();
        outputStream = streamAndPath.f0;
        startPos = outputStream.getPos();
        filePath = streamAndPath.f1;
    }

    @VisibleForTesting
    @Nullable
    public Path getFilePath() {
        return filePath;
    }
}
