/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.segmented.SegmentCheckpointUtils;
import org.apache.flink.runtime.checkpoint.segmented.SegmentSnapshotManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** This class holds the all {@link StateChangelogStorage} objects for a task executor (manager). */
@ThreadSafe
public class TaskExecutorSegmentSnapshotManager {

    /** Logger for this class. */
    private static final Logger LOG =
            LoggerFactory.getLogger(TaskExecutorSegmentSnapshotManager.class);

    private final ResourceID resourceID;

    /**
     * This map holds all segment snapshot managers for tasks running on the task manager / executor
     * that own the instance of this. Maps from job id to all the subtask's segment snapshot
     * managers. Value type Optional is for containing the null value.
     */
    @GuardedBy("lock")
    private final Map<JobID, Optional<SegmentSnapshotManager>> segmentSnapshotManagerByJobId;

    @GuardedBy("lock")
    private boolean closed;

    private final Object lock = new Object();

    /** shutdown hook for this manager. */
    private final Thread shutdownHook;

    public TaskExecutorSegmentSnapshotManager(ResourceID resourceID) {
        this.resourceID = resourceID;
        this.segmentSnapshotManagerByJobId = new HashMap<>();
        this.closed = false;

        // register a shutdown hook
        this.shutdownHook =
                ShutdownHookUtil.addShutdownHook(this::shutdown, getClass().getSimpleName(), LOG);
    }

    @Nullable
    public SegmentSnapshotManager segmentSnapshotManagerForJob(
            @Nonnull JobID jobId, Configuration configuration) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException(
                        "TaskExecutorSegmentSnapshotManager is already closed and cannot "
                                + "register a new SegmentSnapshotManager.");
            }

            Optional<SegmentSnapshotManager> segmentSnapshotManager =
                    segmentSnapshotManagerByJobId.get(jobId);

            if (segmentSnapshotManager == null) {
                SegmentSnapshotManager loaded =
                        SegmentCheckpointUtils.createSegmentSnapshotManager(
                                resourceID.getResourceIdString(), configuration);
                segmentSnapshotManager = Optional.ofNullable(loaded);
                segmentSnapshotManagerByJobId.put(jobId, segmentSnapshotManager);

                if (loaded != null) {
                    LOG.debug(
                            "Registered new segment snapshot manager for job {} : {}.",
                            jobId,
                            loaded);
                } else {
                    LOG.info(
                            "Try to registered new segment snapshot manager for job {},"
                                    + " but result is null.",
                            jobId);
                }
            } else if (segmentSnapshotManager.isPresent()) {
                LOG.debug(
                        "Found existing segment snapshot manager for job {}: {}.",
                        jobId,
                        segmentSnapshotManager.get());
            } else {
                LOG.debug(
                        "Found a previously loaded NULL segment snapshot manager for job {}.",
                        jobId);
            }

            return segmentSnapshotManager.orElse(null);
        }
    }

    public void releaseSegmentSnapshotManagerForJob(@Nonnull JobID jobId) {
        LOG.debug("Releasing segment snapshot manager under job id {}.", jobId);
        Optional<SegmentSnapshotManager> cleanupSegmentSnapshotManager;
        synchronized (lock) {
            if (closed) {
                return;
            }
            cleanupSegmentSnapshotManager = segmentSnapshotManagerByJobId.remove(jobId);
        }

        if (cleanupSegmentSnapshotManager != null) {
            cleanupSegmentSnapshotManager.ifPresent(this::doRelease);
        }
    }

    public void shutdown() {
        HashMap<JobID, Optional<SegmentSnapshotManager>> toRelease;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;

            toRelease = new HashMap<>(segmentSnapshotManagerByJobId);
            segmentSnapshotManagerByJobId.clear();
        }

        ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

        LOG.info("Shutting down TaskExecutorStateChangelogStoragesManager.");

        for (Map.Entry<JobID, Optional<SegmentSnapshotManager>> entry : toRelease.entrySet()) {
            entry.getValue().ifPresent(this::doRelease);
        }
    }

    private void doRelease(SegmentSnapshotManager manager) {
        if (manager != null) {
            try {
                manager.close();
            } catch (Exception e) {
                LOG.warn("Exception while disposing segment snapshot manager {}.", manager, e);
            }
        }
    }
}
