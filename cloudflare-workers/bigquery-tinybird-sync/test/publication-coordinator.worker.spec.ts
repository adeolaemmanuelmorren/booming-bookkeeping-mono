import { env, fetchMock, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  applyIdentityOnlyPublicationMigration,
  applyRawRunRecoveryMigration,
  applyVersionedIdentityBatchMigration,
  applyWorkerIdentityEnqueueMigration,
  applyWorkerIdentityCursorResetMigration,
  applyBootstrapIdentityCutoffMigration,
  bootstrapIdentityCutoffMigrationValue,
  shouldEnqueueSourceFact,
  validateTinybirdDateTime,
  type PublicationCoordinator,
} from "../src/publication-coordinator";
import { IDENTITY_ENQUEUE_BATCHES } from "../src/copy-plan";

const tinybirdOrigin = "https://api.us-east.tinybird.co";
const zeroHash = "0".repeat(64);

beforeAll(() => {
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

afterEach(() => {
  fetchMock.assertNoPendingInterceptors();
});

describe("publication coordinator", () => {
  it("skips source-version churn when identity semantics are unchanged", () => {
    const stable = {
      factKind: "identity_observation",
      factKey: "activecampaign:registration:42",
      sourceFactVersion: 100,
      factDeleted: false,
      factObservedAt: "2026-08-28 10:00:00.000000",
      factPayloadHash: "a".repeat(64),
      evidenceKeys: ["email:test@example.com"],
      firstName: null,
      lastName: null,
      isDeleted: false,
    };
    const incoming = {
      eventId: "event-42",
      producerId: "source_identity:activecampaign",
      observedAt: stable.factObservedAt,
      ingestedAt: "2026-08-28 15:00:00.000000",
      factKind: stable.factKind,
      factKey: stable.factKey,
      sourceFactVersion: 200,
      factDeleted: false,
      factPayloadHash: stable.factPayloadHash,
      factPayload: "{}",
      evidenceKeys: stable.evidenceKeys,
    };

    expect(shouldEnqueueSourceFact(incoming, stable)).toBe(false);
    expect(shouldEnqueueSourceFact(
      { ...incoming, factPayloadHash: "b".repeat(64) },
      stable,
    )).toBe(true);
    expect(shouldEnqueueSourceFact(
      { ...incoming, factDeleted: true },
      stable,
    )).toBe(true);
  });

  it("validates Tinybird DateTime settings exactly", () => {
    expect(validateTinybirdDateTime(
      "2026-08-26 23:05:00",
      "IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM",
    )).toBe("2026-08-26 23:05:00");

    expect(() => validateTinybirdDateTime(
      "2026-08-26T23:05:00Z",
      "IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM",
    )).toThrow("must use Tinybird DateTime format YYYY-MM-DD HH:MM:SS");
    expect(() => validateTinybirdDateTime(
      "2026-02-30 23:05:00",
      "IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM",
    )).toThrow("must use Tinybird DateTime format YYYY-MM-DD HH:MM:SS");
  });

  it("repairs only a bootstrap that has not reached identity enqueue", () => {
    const cutoff = "2026-08-26 23:05:00";

    expect(bootstrapIdentityCutoffMigrationValue(
      "bootstrap",
      "pre_identity",
      cutoff,
    )).toBe(cutoff);
    expect(bootstrapIdentityCutoffMigrationValue(
      "recurring",
      "pre_identity",
      "invalid but unused",
    )).toBeNull();
    expect(bootstrapIdentityCutoffMigrationValue(
      "bootstrap",
      "identity",
      "invalid but preserved",
    )).toBeNull();
  });

  it("migrates an in-flight pre-identity bootstrap once", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("bootstrap-cutoff-migration-test");
    const seedCutoff = "2026-08-26 23:05:00";

    await runInDurableObject(stub, async (_instance: PublicationCoordinator, state) => {
      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version = 7");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            publication_kind = 'bootstrap',
            publication_section = 'pre_identity',
            publication_index = 6,
            identity_overlap_cutoff = '2026-08-28 00:13:00'
          WHERE id = 1
        `,
      );

      applyBootstrapIdentityCutoffMigration(state.storage, seedCutoff, 100);
      applyBootstrapIdentityCutoffMigration(
        state.storage,
        "2026-08-27 00:00:00",
        200,
      );

      expect(state.storage.sql.exec<{ value: string }>(
        "SELECT identity_overlap_cutoff AS value FROM coordinator_state WHERE id = 1",
      ).one().value).toBe(seedCutoff);
      expect(state.storage.sql.exec<{ value: number }>(
        "SELECT COUNT(*) AS value FROM publication_schema_migrations WHERE version = 7",
      ).one().value).toBe(1);
    });
  });

  it("migration preserves recurring and started identity cutoffs", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("cutoff-migration-preservation-test");
    const existingCutoff = "2026-08-28 00:13:00";

    await runInDurableObject(stub, async (_instance: PublicationCoordinator, state) => {
      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version = 7");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            publication_kind = 'recurring',
            publication_section = 'pre_identity',
            identity_overlap_cutoff = ?
          WHERE id = 1
        `,
        existingCutoff,
      );
      applyBootstrapIdentityCutoffMigration(state.storage, "invalid but unused", 100);
      expect(state.storage.sql.exec<{ value: string }>(
        "SELECT identity_overlap_cutoff AS value FROM coordinator_state WHERE id = 1",
      ).one().value).toBe(existingCutoff);

      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version = 7");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET publication_kind = 'bootstrap', publication_section = 'identity'
          WHERE id = 1
        `,
      );
      applyBootstrapIdentityCutoffMigration(state.storage, "invalid but preserved", 200);
      expect(state.storage.sql.exec<{ value: string }>(
        "SELECT identity_overlap_cutoff AS value FROM coordinator_state WHERE id = 1",
      ).one().value).toBe(existingCutoff);
    });
  });

  it("uses the immutable seed cutoff for bootstrap identity enqueue", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("bootstrap-identity-cutoff-test");
    const generationId = "generation_bootstrap_identity_cutoff";
    const cutoff = "2026-08-26 23:05:00";
    const firstBatch = IDENTITY_ENQUEUE_BATCHES[0];

    mockEmptySourcePage(firstBatch.producerId, cutoff);

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedAwaitingPublication(
        state,
        generationId,
        "2026-08-27T05:34:00.000Z",
      );

      await instance.configureBootstrap("run");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET publication_section = 'identity', identity_phase = 'enqueue'
          WHERE id = 1
        `,
      );
      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.publicationKind).toBe("bootstrap");
      expect(status.identityEnqueue).toEqual({
        completedBatches: 1,
        totalBatches: 7,
        currentProducerId: "source_identity:segment_form",
        sourceIngestedFrom: cutoff,
      });
      expect(status.activeCopyJobId).toBeNull();
    });
  });

  it("keeps the generation overlap cutoff for recurring identity enqueue", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("recurring-identity-cutoff-test");
    const generationId = "generation_recurring_identity_cutoff";
    const scheduledAt = "2026-08-27T05:34:00.000Z";
    const cutoff = "2026-08-27 05:14:00";
    const firstBatch = IDENTITY_ENQUEUE_BATCHES[0];

    mockEmptySourcePage(firstBatch.producerId, cutoff);

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedAwaitingPublication(state, generationId, scheduledAt);

      await instance.configureBootstrap("acknowledge_existing");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET publication_section = 'identity', identity_phase = 'enqueue'
          WHERE id = 1
        `,
      );
      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.publicationKind).toBe("recurring");
      expect(status.identityEnqueue).toEqual({
        completedBatches: 1,
        totalBatches: 8,
        currentProducerId: "source_identity:segment_form",
        sourceIngestedFrom: cutoff,
      });
      expect(status.activeCopyJobId).toBeNull();
    });
  });

  it("maps persisted bootstrap enqueue index 5 to Stripe", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName(
      "bootstrap-identity-index-five-test",
    );
    const cutoff = "2026-08-26 23:05:00";

    mockEmptySourcePage("source_identity:stripe", cutoff);

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedIdentityEnqueue(
        state,
        "generation_bootstrap_identity_index_five",
        cutoff,
        "bootstrap",
        5,
      );

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.identityEnqueue).toEqual({
        completedBatches: 6,
        totalBatches: 7,
        currentProducerId: "source_identity:stripe_kajabi",
        sourceIngestedFrom: cutoff,
      });
      expect(status.activeCopyJobId).toBeNull();
    });
  });

  it("migrates to the activated-delta flow without starting external work", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("activated-delta-migration-test");

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      const status = await instance.status();
      await state.storage.deleteAlarm();

      expect(status.phase).toBe("collecting_raw");
      expect(status.identityPhase).toBe("enqueue");
      expect(status.identityBatch).toBeNull();
      expect(state.storage.sql.exec<{ version: number }>(
        "SELECT MAX(version) AS version FROM publication_schema_migrations",
      ).one().version).toBe(20);
    });
  });

  it("marks the historical journey seed complete without recomputing it", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("journey-seed-overlay-test");

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      expect((await instance.status()).journeyBackfill.status).toBe("complete");
      await expect(instance.startJourneyBackfill()).rejects.toThrow(
        "Journey backfill is retired.",
      );
      await state.storage.deleteAlarm();
    });
  });

  it("replaces a stale alarm when the coordinator is rearmed", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("stale-alarm-rearm-test");

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      await state.storage.setAlarm(Date.now() - 1_000);
      const rearmedAfter = Date.now();

      await instance.setOperatorPaused(false);

      expect(await state.storage.getAlarm()).toBeGreaterThan(rearmedAfter);
    });
  });

  it("clears a stale SQL batch before the Worker resumes from the active cursor", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("worker-cursor-reset-migration-test");

    await runInDurableObject(stub, async (_instance: PublicationCoordinator, state) => {
      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version >= 9");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            phase = 'copying',
            publication_section = 'identity',
            identity_phase = 'compute',
            identity_batch_version = 1787878380000,
            identity_batch_id = 'stale-active-batch',
            identity_cursor_ingested_at = '1970-01-01 00:00:00.000000',
            identity_cursor_event_id = ?,
            identity_manifest_json = '{"stale":true}'
          WHERE id = 1
        `,
        zeroHash,
      );

      expect(applyWorkerIdentityCursorResetMigration(state.storage, 100)).toBe(true);
      expect(applyWorkerIdentityCursorResetMigration(state.storage, 200)).toBe(false);

      const status = state.storage.sql.exec<{
        batchVersion: number | null;
        batchId: string | null;
        cursorIngestedAt: string | null;
        cursorEventId: string | null;
      }>(
        `
          SELECT
            identity_batch_version AS batchVersion,
            identity_batch_id AS batchId,
            identity_cursor_ingested_at AS cursorIngestedAt,
            identity_cursor_event_id AS cursorEventId
          FROM coordinator_state
          WHERE id = 1
        `,
      ).one();
      expect(status).toEqual({
        batchVersion: null,
        batchId: null,
        cursorIngestedAt: null,
        cursorEventId: null,
      });
    });
  });

  it("retires a failed post-identity Copy without replaying it", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("identity-only-publication-migration");

    await runInDurableObject(stub, async (_instance: PublicationCoordinator, state) => {
      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version >= 10");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            phase = 'failed',
            publication_section = 'post_identity',
            publication_index = 2,
            active_pipe = 'snapshot_mart_payments',
            active_job_id = 'failed-reporting-copy',
            last_error = 'reporting Copy timed out'
          WHERE id = 1
        `,
      );

      expect(applyIdentityOnlyPublicationMigration(state.storage, 100)).toBe(true);
      expect(applyIdentityOnlyPublicationMigration(state.storage, 200)).toBe(false);

      const migrated = state.storage.sql.exec<{
        phase: string;
        publicationIndex: number;
        activePipe: string | null;
        activeJobId: string | null;
        lastError: string | null;
      }>(
        `
          SELECT
            phase,
            publication_index AS publicationIndex,
            active_pipe AS activePipe,
            active_job_id AS activeJobId,
            last_error AS lastError
          FROM coordinator_state
          WHERE id = 1
        `,
      ).one();
      expect(migrated).toEqual({
        phase: "copying",
        publicationIndex: 0,
        activePipe: null,
        activeJobId: null,
        lastError: null,
      });
    });
  });

  it("recovers a failed pre-identity Copy into Worker enqueue", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("worker-enqueue-migration");
    const generationId = "generation_worker_enqueue_migration";

    await runInDurableObject(stub, async (_instance: PublicationCoordinator, state) => {
      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version >= 12");
      seedPublication(state, generationId);
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            phase = 'failed',
            active_raw_generation = ?,
            publication_kind = 'recurring',
            publication_section = 'pre_identity',
            active_pipe = 'snapshot_activecampaign_registrations',
            active_job_id = 'failed-snapshot-job',
            last_error = 'Copy timed out'
          WHERE id = 1
        `,
        generationId,
      );

      expect(applyWorkerIdentityEnqueueMigration(state.storage, 100)).toBe(true);
      expect(applyWorkerIdentityEnqueueMigration(state.storage, 200)).toBe(false);

      const migrated = state.storage.sql.exec<{
        phase: string;
        section: string;
        identityPhase: string;
        activePipe: string | null;
        activeJobId: string | null;
      }>(
        `
          SELECT
            phase,
            publication_section AS section,
            identity_phase AS identityPhase,
            active_pipe AS activePipe,
            active_job_id AS activeJobId
          FROM coordinator_state
          WHERE id = 1
        `,
      ).one();
      expect(migrated).toEqual({
        phase: "copying",
        section: "identity",
        identityPhase: "enqueue",
        activePipe: null,
        activeJobId: null,
      });
    });
  });

  it("re-prepares an unfinished identity batch with the versioned batch ID", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("versioned-identity-batch-migration");

    await runInDurableObject(stub, async (_instance: PublicationCoordinator, state) => {
      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version >= 17");
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            phase = 'failed',
            publication_section = 'identity',
            identity_phase = 'compute',
            identity_batch_version = 1787935740004,
            identity_batch_id = 'legacy-batch-id',
            identity_cursor_ingested_at = '2026-08-29 23:48:10.823000',
            identity_cursor_event_id = ?,
            last_error = 'Identity journal verification failed for evidence.'
          WHERE id = 1
        `,
        zeroHash,
      );

      expect(applyVersionedIdentityBatchMigration(state.storage, 100)).toBe(true);
      expect(applyVersionedIdentityBatchMigration(state.storage, 200)).toBe(false);

      const migrated = state.storage.sql.exec<{
        phase: string;
        batchVersion: number | null;
        batchId: string | null;
        lastError: string | null;
      }>(
        `
          SELECT
            phase,
            identity_batch_version AS batchVersion,
            identity_batch_id AS batchId,
            last_error AS lastError
          FROM coordinator_state
          WHERE id = 1
        `,
      ).one();
      expect(migrated).toEqual({
        phase: "copying",
        batchVersion: null,
        batchId: null,
        lastError: null,
      });
    });
  });

  it("requeues a raw run left in progress by an interrupted alarm", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("raw-run-recovery-migration");

    await runInDurableObject(stub, async (_instance: PublicationCoordinator, state) => {
      state.storage.sql.exec("DELETE FROM publication_schema_migrations WHERE version >= 11");
      state.storage.sql.exec(
        `
          INSERT INTO raw_runs (
            request_key,
            scheduled_at,
            slot,
            generation_id,
            status,
            attempt_count,
            last_error,
            created_at_ms,
            updated_at_ms
          ) VALUES (
            'raw_interrupted',
            '2026-08-28T11:14:00.000Z',
            7,
            'generation_interrupted',
            'running',
            1,
            NULL,
            1,
            1
          )
        `,
      );

      expect(applyRawRunRecoveryMigration(state.storage, 100)).toBe(true);
      expect(applyRawRunRecoveryMigration(state.storage, 200)).toBe(false);
      expect(state.storage.sql.exec<{ status: string }>(
        "SELECT status FROM raw_runs WHERE request_key = 'raw_interrupted'",
      ).one().status).toBe("queued");
    });
  });

  it("recovers only an expired raw-run lease while paused", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("raw-run-lease-recovery");

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      state.storage.sql.exec(
        `
          INSERT INTO raw_runs (
            request_key,
            scheduled_at,
            slot,
            generation_id,
            status,
            attempt_count,
            last_error,
            created_at_ms,
            updated_at_ms
          ) VALUES
            ('expired', '2026-08-28T11:14:00.000Z', 7, 'generation', 'running', 1, NULL, 1, 1),
            ('current', '2026-08-28T11:15:00.000Z', 8, 'generation', 'running', 1, NULL, ?, ?)
        `,
        Date.now(),
        Date.now(),
      );
      await instance.setOperatorPaused(true);

      const result = await instance.recoverExpiredRawRunLease();

      expect(result.recoveredRuns).toBe(1);
      expect(result.runningRuns).toBe(2);
      expect(state.storage.sql.exec<{ status: string }>(
        "SELECT status FROM raw_runs WHERE request_key = 'expired'",
      ).one().status).toBe("queued");
      expect(state.storage.sql.exec<{ status: string }>(
        "SELECT status FROM raw_runs WHERE request_key = 'current'",
      ).one().status).toBe("running");
    });
  });

  it("queues raw arrivals while a publication owns the coordinator", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("raw-during-publication-test");

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            phase = 'copying',
            active_raw_generation = 'generation_in_progress',
            publication_kind = 'recurring',
            publication_section = 'pre_identity'
          WHERE id = 1
        `,
      );
      await state.storage.deleteAlarm();

      const result = await instance.enqueueRawRun({
        scheduledAt: "2026-08-26T12:34:00.000Z",
      });

      expect(result.status).toBe("queued");
      expect((await instance.status()).queuedRawRuns).toBe(1);
      expect(await state.storage.getAlarm()).toBeNull();
    });
  });

  it("reuses an exact completed raw slot across generation membership", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("raw-generation-reuse-test");
    const generationId = "generation_raw_20260828165000000_slot_0";
    const scheduledAt = "2026-08-28T16:50:00.000Z";

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            phase = 'collecting_raw',
            bootstrap_status = 'complete',
            active_raw_generation = ?
          WHERE id = 1
        `,
        generationId,
      );
      for (let slot = 0; slot < 10; slot += 1) {
        state.storage.sql.exec(
          `
            INSERT INTO raw_runs (
              request_key,
              scheduled_at,
              slot,
              generation_id,
              status,
              attempt_count,
              last_error,
              created_at_ms,
              updated_at_ms
            ) VALUES (?, ?, ?, ?, 'completed', 1, NULL, 1, 1)
          `,
          `raw_20260828165000000_slot_${slot}`,
          scheduledAt,
          slot,
          slot === 4 ? "generation_previous" : generationId,
        );
      }

      expect((await instance.status()).completedRawSlots).toEqual([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
      ]);

      await instance.alarm();
      await state.storage.deleteAlarm();

      expect((await instance.status()).phase).toBe("settling_raw");
    });
  });

  it("compacts a paused raw backlog without skipping the active generation", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("raw-backlog-compaction-test");

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            phase = 'collecting_raw',
            bootstrap_status = 'complete',
            active_raw_generation = 'generation_active'
          WHERE id = 1
        `,
      );
      for (const slot of [6, 7, 8]) {
        state.storage.sql.exec(
          `
            INSERT INTO raw_runs (
              request_key,
              scheduled_at,
              slot,
              generation_id,
              status,
              attempt_count,
              last_error,
              created_at_ms,
              updated_at_ms
            ) VALUES (?, ?, ?, 'generation_active', 'completed', 1, NULL, 1, 1)
          `,
          `completed_${slot}`,
          `2026-08-28T13:${String(slot).padStart(2, "0")}:00.000Z`,
          slot,
        );
      }
      for (let hour = 13; hour <= 16; hour += 1) {
        for (let slot = 0; slot < 10; slot += 1) {
          state.storage.sql.exec(
            `
              INSERT INTO raw_runs (
                request_key,
                scheduled_at,
                slot,
                generation_id,
                status,
                attempt_count,
                last_error,
                created_at_ms,
                updated_at_ms
              ) VALUES (?, ?, ?, NULL, 'queued', 0, NULL, 1, 1)
            `,
            `queued_${hour}_${slot}`,
            `2026-08-28T${hour}:${String(slot).padStart(2, "0")}:00.000Z`,
            slot,
          );
        }
      }

      await instance.setOperatorPaused(true);
      const result = await instance.compactRawBacklog();

      expect(result).toMatchObject({
        queuedBefore: 40,
        queuedAfter: 17,
        compactedRuns: 23,
        protectedActiveGenerationRuns: 7,
        retainedCatchUpRuns: 10,
      });
      expect(state.storage.sql.exec<{ value: number }>(
        "SELECT overlap_minutes AS value FROM raw_runs WHERE request_key = 'queued_16_6'",
      ).one().value).toBe(200);
      expect(state.storage.sql.exec<{ value: number }>(
        "SELECT overlap_minutes AS value FROM raw_runs WHERE request_key = 'queued_16_0'",
      ).one().value).toBe(140);
      expect(state.storage.sql.exec<{ value: number }>(
        "SELECT COUNT(*) AS value FROM raw_runs WHERE status = 'compacted'",
      ).one().value).toBe(23);
    });
  });

  it("keeps bootstrap Copies but sends recurring publication directly to identity", async () => {
    const bootstrapStub = env.PUBLICATION_COORDINATOR.getByName(
      "bootstrap-normal-compute-test",
    );
    mockCopySubmission(
      "backfill_activecampaign_contacts_current",
      "bootstrap-copy-job",
    );

    await runInDurableObject(
      bootstrapStub,
      async (instance: PublicationCoordinator, state) => {
        seedCopyPublication(state, "generation_bootstrap", "bootstrap");

        await instance.alarm();
        await state.storage.deleteAlarm();

        expect((await instance.status()).activeCopyJobId).toBe("bootstrap-copy-job");
      },
    );

    const recurringStub = env.PUBLICATION_COORDINATOR.getByName(
      "recurring-normal-compute-test",
    );
    await runInDurableObject(
      recurringStub,
      async (instance: PublicationCoordinator, state) => {
        seedCopyPublication(state, "generation_recurring", "recurring");

        await instance.alarm();
        await state.storage.deleteAlarm();

        const status = await instance.status();
        expect(status.publicationSection).toBe("identity");
        expect(status.activeCopyJobId).toBeNull();
      },
    );
  });

  it("resumes bootstrap index 9 at the first bounded page-view window", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName(
      "bootstrap-page-view-resume-test",
    );
    const parameters = {
      p_source_system: "boom_domains",
      p_start: "2026-06-22 00:00:00",
      p_end: "2026-06-24 00:00:00",
    };
    mockCopySubmission(
      "backfill_jitsu_page_view_versions",
      "bounded-page-view-job",
      parameters,
    );

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedCopyPublication(state, "generation_page_view_resume", "bootstrap");
      state.storage.sql.exec(
        "UPDATE coordinator_state SET publication_index = 9 WHERE id = 1",
      );

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.activeCopyJobId).toBe("bounded-page-view-job");
      expect(status.activeCopyParameters).toEqual(parameters);
    });
  });

  it("resumes failed bootstrap index 11 at the Stripe payment snapshot", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName(
      "bootstrap-stripe-payment-resume-test",
    );
    mockCopySubmission(
      "snapshot_all_stripe_payments",
      "stripe-payment-job",
    );

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedCopyPublication(state, "generation_stripe_payment_resume", "bootstrap");
      state.storage.sql.exec(
        "UPDATE coordinator_state SET publication_index = 11 WHERE id = 1",
      );

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.activeCopyJobId).toBe("stripe-payment-job");
      expect(status.activeCopyParameters).toEqual({});
    });
  });

  it("freezes one cursor and deterministic batch before Worker computation", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("prepare-identity-batch-test");
    const generationId = "generation_prepare_test";
    const scheduledAt = "2026-08-26T12:34:00.000Z";
    const expectedVersion = new Date(scheduledAt).valueOf();
    const expectedBatchId = `${generationId}_identity_${expectedVersion}_engine1_limit1000`;

    mockCursor(1, "1970-01-01 00:00:00.000000", zeroHash);

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedIdentityPhase(state, generationId, "compute", scheduledAt);

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.identityPhase).toBe("compute");
      expect(status.identityBatch).toEqual({
        version: expectedVersion,
        id: expectedBatchId,
        cursorIngestedAt: "1970-01-01 00:00:00.000000",
        cursorEventId: zeroHash,
      });
      expect(status.activeCopyJobId).toBeNull();
    });
  });

  it("runs an empty identity batch without a compaction Copy", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("worker-empty-batch-test");
    const generationId = "generation_worker_empty";
    const scheduledAt = "2026-08-26T12:34:00.000Z";
    const batchVersion = new Date(scheduledAt).valueOf();
    const batchId = `${generationId}_identity_${batchVersion}_engine1_limit1000`;
    const cursorAt = "1970-01-01 00:00:00.000000";

    mockCursor(1, cursorAt, zeroHash);
    mockCursor(1, cursorAt, zeroHash);
    mockEmptyWorkerBatch({ batchId, batchVersion, cursorAt });

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedIdentityPhase(state, generationId, "compute", scheduledAt);

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("validate");

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.publicationSection).toBe("post_identity");
      expect(status.identityPhase).toBe("enqueue");
      expect(state.storage.sql.exec<{ value: number }>(
        "SELECT COUNT(*) AS value FROM publication_jobs",
      ).one().value).toBe(0);
    });
  });

  it("advances without rewriting a prepared batch that is already active", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("already-activated-worker-batch-test");
    const generationId = "generation_already_activated";
    const batchVersion = 1787878380001;
    const batchId = `${generationId}_identity_${batchVersion}`;
    const cursorAt = "2026-08-28 03:46:26.150572";
    const cursorEventId = "7".repeat(64);

    mockCursor(batchVersion, cursorAt, cursorEventId, batchId);

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedIdentityPhase(
        state,
        generationId,
        "compute",
        "2026-08-26T12:34:00.000Z",
      );
      state.storage.sql.exec(
        `
          UPDATE coordinator_state
          SET
            identity_batch_version = ?,
            identity_batch_id = ?,
            identity_cursor_ingested_at = '1970-01-01 00:00:00.000000',
            identity_cursor_event_id = ?
          WHERE id = 1
        `,
        batchVersion,
        batchId,
        zeroHash,
      );

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.identityPhase).toBe("compute");
      expect(status.identityBatch).toBeNull();
      expect(status.lastError).toBeNull();
    });
  });

  it.skip("legacy Copy compaction is retired", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("activate-identity-batch-test");
    const generationId = "generation_activation_test";
    const scheduledAt = "2026-08-26T12:34:00.000Z";
    const batchVersion = new Date(scheduledAt).valueOf();
    const batchId = `${generationId}_identity_${batchVersion}`;
    const cursorAt = "1970-01-01 00:00:00.000000";
    const nextCursorAt = "2026-08-26 12:34:59.123456";
    const nextCursorEventId = "c".repeat(64);
    const nextBatchVersion = batchVersion + 1;
    const nextBatchId = `${generationId}_identity_${nextBatchVersion}`;

    mockCursor(1, cursorAt, zeroHash);
    mockPreparationSubmission({
      batchId,
      batchVersion,
      cursorAt,
      cursorEventId: zeroHash,
      jobId: "prepare-job-1",
    });
    mockCopyJob("prepare-job-1");
    mockChangesSubmission({ batchId, batchVersion, jobId: "changes-job-1" });
    mockCopyJob("changes-job-1");
    mockProfilesSubmission({ batchId, batchVersion, jobId: "profiles-job-1" });
    mockCopyJob("profiles-job-1");
    mockTouchedProfilesSubmission({
      batchId,
      batchVersion,
      jobId: "touched-profiles-job-1",
    });
    mockCopyJob("touched-profiles-job-1");
    mockScopedSubmission({ batchId, batchVersion, jobId: "scope-job-1" });
    mockCopyJob("scope-job-1");
    mockFactKeysSubmission({ batchId, batchVersion, jobId: "fact-keys-job-1" });
    mockCopyJob("fact-keys-job-1");
    for (let factShard = 0; factShard < 8; factShard += 1) {
      const jobId = `facts-job-1-${factShard}`;
      mockFactsSubmission({ batchId, batchVersion, factShard, jobId });
      mockCopyJob(jobId);
    }
    mockComponentsSubmission({ batchId, batchVersion, jobId: "components-job-1" });
    mockCopyJob("components-job-1");
    mockCompactionSubmission({ batchId, batchVersion, jobId: "compact-job-1" });
    mockCopyJob("compact-job-1");
    mockManifest({ batchId, batchVersion, inputEventCount: 3 });
    mockActivation();
    mockCursor(batchVersion, nextCursorAt, nextCursorEventId);
    mockPreparationSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      cursorAt: nextCursorAt,
      cursorEventId: nextCursorEventId,
      jobId: "prepare-job-2",
    });
    mockCopyJob("prepare-job-2");
    mockChangesSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      jobId: "changes-job-2",
    });
    mockCopyJob("changes-job-2");
    mockProfilesSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      jobId: "profiles-job-2",
    });
    mockCopyJob("profiles-job-2");
    mockTouchedProfilesSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      jobId: "touched-profiles-job-2",
    });
    mockCopyJob("touched-profiles-job-2");
    mockScopedSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      jobId: "scope-job-2",
    });
    mockCopyJob("scope-job-2");
    mockFactKeysSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      jobId: "fact-keys-job-2",
    });
    mockCopyJob("fact-keys-job-2");
    for (let factShard = 0; factShard < 8; factShard += 1) {
      const jobId = `facts-job-2-${factShard}`;
      mockFactsSubmission({
        batchId: nextBatchId,
        batchVersion: nextBatchVersion,
        factShard,
        jobId,
      });
      mockCopyJob(jobId);
    }
    mockComponentsSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      jobId: "components-job-2",
    });
    mockCopyJob("components-job-2");
    mockCompactionSubmission({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      jobId: "compact-job-2",
    });
    mockCopyJob("compact-job-2");
    mockManifest({
      batchId: nextBatchId,
      batchVersion: nextBatchVersion,
      inputEventCount: 0,
    });

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedIdentityPhase(state, generationId, "prepare", scheduledAt);

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).activeCopyJobId).toBe("prepare-job-1");

      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("changes");

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("profiles");

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("touched_profiles");

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("scope");

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("fact_keys");

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("facts_0");

      for (let factShard = 0; factShard < 8; factShard += 1) {
        await instance.alarm();
        await instance.alarm();

        const nextPhase = factShard === 7
          ? "components"
          : `facts_${factShard + 1}`;
        expect((await instance.status()).identityPhase).toBe(nextPhase);
      }

      expect((await instance.status()).identityPhase).toBe("components");

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("compact");

      await instance.alarm();
      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("validate");

      await instance.alarm();
      expect((await instance.status()).identityPhase).toBe("activate");

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.publicationSection).toBe("identity");
      expect(status.identityPhase).toBe("compute");
      expect(status.identityBatch).toBeNull();

      for (let index = 0; index < 34; index += 1) await instance.alarm();
      await state.storage.deleteAlarm();

      const drainedStatus = await instance.status();
      expect(drainedStatus.publicationSection).toBe("post_identity");
      expect(drainedStatus.identityPhase).toBe("enqueue");
      expect(state.storage.sql.exec<{ pipe_name: string }>(
        "SELECT pipe_name FROM publication_jobs",
      ).toArray()).toEqual([
        { pipe_name: "prepare_identity_compaction" },
        { pipe_name: "classify_identity_compaction_changes" },
        { pipe_name: "seed_identity_compaction_profiles" },
        { pipe_name: "freeze_identity_compaction_touched_profiles" },
        { pipe_name: "scope_identity_compaction" },
        { pipe_name: "select_identity_compaction_fact_keys" },
        ...Array.from({ length: 8 }, () => ({
          pipe_name: "expand_identity_compaction_facts",
        })),
        { pipe_name: "build_identity_compaction_components" },
        { pipe_name: "compact_identity_state" },
        { pipe_name: "prepare_identity_compaction" },
        { pipe_name: "classify_identity_compaction_changes" },
        { pipe_name: "seed_identity_compaction_profiles" },
        { pipe_name: "freeze_identity_compaction_touched_profiles" },
        { pipe_name: "scope_identity_compaction" },
        { pipe_name: "select_identity_compaction_fact_keys" },
        ...Array.from({ length: 8 }, () => ({
          pipe_name: "expand_identity_compaction_facts",
        })),
        { pipe_name: "build_identity_compaction_components" },
        { pipe_name: "compact_identity_state" },
      ]);
    });
  });

  it("does not publish a batch before manifest validation", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("invalid-manifest-test");
    const generationId = "generation_invalid_manifest";
    const scheduledAt = "2026-08-26T12:34:00.000Z";
    const batchVersion = new Date(scheduledAt).valueOf();
    const batchId = `${generationId}_identity_${batchVersion}`;

    mockManifest({
      batchId,
      batchVersion,
      inputEventCount: 3,
      isValid: false,
    });

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedPreparedIdentityPhase(
        state,
        generationId,
        "validate",
        scheduledAt,
        batchVersion,
        batchId,
      );

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.phase).toBe("failed");
      expect(status.identityPhase).toBe("validate");
      expect(status.lastError).toContain("failed validation");
    });
  });

  it("skips activation cleanly when the bounded batch has no events", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("empty-identity-batch-test");
    const generationId = "generation_empty_identity";
    const scheduledAt = "2026-08-26T12:34:00.000Z";
    const batchVersion = new Date(scheduledAt).valueOf();
    const batchId = `${generationId}_identity_${batchVersion}`;

    mockManifest({ batchId, batchVersion, inputEventCount: 0 });

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedPreparedIdentityPhase(
        state,
        generationId,
        "validate",
        scheduledAt,
        batchVersion,
        batchId,
      );

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.phase).toBe("copying");
      expect(status.publicationSection).toBe("post_identity");
      expect(status.identityPhase).toBe("enqueue");
    });
  });

  it("reads all eight recurring identity producers without Copy jobs", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("identity-enqueue-batches-test");
    const cutoff = "2026-08-26 12:14:00";

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedIdentityEnqueue(state, "generation_identity_batches", cutoff);

      for (const batch of IDENTITY_ENQUEUE_BATCHES) {
        const sourceIngestedFrom = batch.usesGenerationOverlapCutoff
          ? cutoff
          : "1970-01-01 00:00:00";
        const sourceIngestedTo = batch.producerId === "source_identity:activecampaign"
          ? "2026-08-26 12:24:00"
          : undefined;
        mockEmptySourcePage(batch.producerId, sourceIngestedFrom, sourceIngestedTo);
        await instance.alarm();
      }

      await instance.alarm();
      await state.storage.deleteAlarm();

      const status = await instance.status();
      expect(status.identityPhase).toBe("compute");
      expect(status.identityEnqueue).toBeNull();
      expect(state.storage.sql.exec<{ value: number }>(
        "SELECT COUNT(*) AS value FROM publication_jobs",
      ).one().value).toBe(0);
    });
  });

  it("splits a wide ActiveCampaign enqueue into bounded source windows", async () => {
    const stub = env.PUBLICATION_COORDINATOR.getByName("activecampaign-window-test");
    const generationId = "generation_activecampaign_windows";
    const cutoff = "2026-08-26 12:00:00";

    await runInDurableObject(stub, async (instance: PublicationCoordinator, state) => {
      seedIdentityEnqueue(state, generationId, cutoff, "recurring", 5);
      state.storage.sql.exec(
        "UPDATE raw_runs SET scheduled_at = '2026-08-28T12:25:00.000Z' WHERE generation_id = ?",
        generationId,
      );
      mockEmptySourcePage(
        "source_identity:activecampaign",
        "2026-08-26 12:00:00",
        "2026-08-27 12:00:00",
      );
      mockEmptySourcePage(
        "source_identity:activecampaign",
        "2026-08-27 12:00:00",
        "2026-08-28 12:00:00",
      );
      mockEmptySourcePage(
        "source_identity:activecampaign",
        "2026-08-28 12:00:00",
        "2026-08-28 12:25:00",
      );

      await instance.alarm();
      await instance.alarm();
      await instance.alarm();
      await state.storage.deleteAlarm();

      expect((await instance.status()).identityEnqueue).toEqual({
        completedBatches: 6,
        totalBatches: 8,
        currentProducerId: "source_identity:stripe",
        sourceIngestedFrom: cutoff,
      });
    });
  });
});

function mockCursor(
  activeBatchVersion: number,
  checkpointIngestedAt: string,
  checkpointEventId: string,
  activeBatchId = "seed-batch",
): void {
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "GET",
      path: "/v0/pipes/current_identity_compaction_cursor.json?p_tenant_id=boom",
    })
    .reply(200, {
      data: [{
        active_batch_version: activeBatchVersion,
        active_batch_id: activeBatchId,
        checkpoint_ingested_at: checkpointIngestedAt,
        checkpoint_event_id: checkpointEventId,
      }],
    });
}

function mockJourneyBackfillProfilePage(
  afterProfileId: string,
  profileIds: string[],
): void {
  const query = new URLSearchParams({
    p_after_profile_id: afterProfileId,
    p_batch_limit: "500",
    p_tenant_id: "boom",
  });
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "GET",
      path: `/v0/pipes/reporting_journey_profile_page.json?${query.toString()}`,
    })
    .reply(200, { data: profileIds.map((profileId) => ({ profile_id: profileId })) });
}

function mockJourneyBuild(
  profileIds: string[],
  rows: Record<string, unknown>[],
): void {
  const query = new URLSearchParams();
  for (const profileId of profileIds) query.append("p_profile_ids", profileId);
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "GET",
      path: `/v0/pipes/reporting_profile_journey_window_build.json?${query.toString()}`,
    })
    .reply(200, { data: rows });
}

function mockJourneyAppend(dataSourceName: string): void {
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: `/v0/events?name=${dataSourceName}&wait=true`,
    })
    .reply(200, { successful_rows: 1 });
}

function mockCopySubmission(
  pipeName: string,
  jobId: string,
  parameters: Readonly<Record<string, string>> = {},
): void {
  const query = new URLSearchParams(
    Object.entries(parameters).sort(([left], [right]) => left.localeCompare(right)),
  );
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: `/v0/pipes/${pipeName}/copy${suffix}`,
    })
    .reply(200, { job: { job_id: jobId } });
}

function mockPreparationSubmission(input: {
  batchId: string;
  batchVersion: number;
  cursorAt: string;
  cursorEventId: string;
  jobId: string;
}): void {
  const query = new URLSearchParams({
    p_batch_id: input.batchId,
    p_batch_limit: "1000",
    p_batch_version: String(input.batchVersion),
    p_cursor_event_id: input.cursorEventId,
    p_cursor_ingested_at: input.cursorAt,
    p_tenant_id: "boom",
  });
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: `/v0/pipes/prepare_identity_compaction/copy?${query.toString()}`,
    })
    .reply(200, { job: { job_id: input.jobId } });
}

function mockScopedSubmission(input: {
  batchId: string;
  batchVersion: number;
  jobId: string;
}): void {
  mockBatchCopySubmission("scope_identity_compaction", input);
}

function mockChangesSubmission(input: {
  batchId: string;
  batchVersion: number;
  jobId: string;
}): void {
  mockBatchCopySubmission("classify_identity_compaction_changes", input);
}

function mockProfilesSubmission(input: {
  batchId: string;
  batchVersion: number;
  jobId: string;
}): void {
  mockBatchCopySubmission("seed_identity_compaction_profiles", input);
}

function mockTouchedProfilesSubmission(input: {
  batchId: string;
  batchVersion: number;
  jobId: string;
}): void {
  mockBatchCopySubmission("freeze_identity_compaction_touched_profiles", input);
}

function mockFactsSubmission(input: {
  batchId: string;
  batchVersion: number;
  factShard: number;
  jobId: string;
}): void {
  const query = new URLSearchParams({
    p_batch_id: input.batchId,
    p_batch_version: String(input.batchVersion),
    p_fact_shard: String(input.factShard),
    p_fact_shard_count: "8",
    p_tenant_id: "boom",
  });
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: `/v0/pipes/expand_identity_compaction_facts/copy?${query.toString()}`,
    })
    .reply(200, { job: { job_id: input.jobId } });
}

function mockFactKeysSubmission(input: {
  batchId: string;
  batchVersion: number;
  jobId: string;
}): void {
  mockBatchCopySubmission("select_identity_compaction_fact_keys", input);
}

function mockComponentsSubmission(input: {
  batchId: string;
  batchVersion: number;
  jobId: string;
}): void {
  mockBatchCopySubmission("build_identity_compaction_components", input);
}

function mockCompactionSubmission(input: {
  batchId: string;
  batchVersion: number;
  jobId: string;
}): void {
  mockBatchCopySubmission("compact_identity_state", input);
}

function mockBatchCopySubmission(
  pipeName:
    | "classify_identity_compaction_changes"
    | "seed_identity_compaction_profiles"
    | "freeze_identity_compaction_touched_profiles"
    | "scope_identity_compaction"
    | "select_identity_compaction_fact_keys"
    | "build_identity_compaction_components"
    | "compact_identity_state",
  input: { batchId: string; batchVersion: number; jobId: string },
): void {
  const query = new URLSearchParams({
    p_batch_id: input.batchId,
    p_batch_version: String(input.batchVersion),
    p_tenant_id: "boom",
  });
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: `/v0/pipes/${pipeName}/copy?${query.toString()}`,
    })
    .reply(200, { job: { job_id: input.jobId } });
}

function mockManifest(input: {
  batchId: string;
  batchVersion: number;
  inputEventCount: number;
  isValid?: boolean;
}): void {
  const query = new URLSearchParams({
    p_batch_id: input.batchId,
    p_batch_version: String(input.batchVersion),
    p_tenant_id: "boom",
  });
  const outputRowCount = input.inputEventCount === 0 ? 0 : 43;
  const outputHash = outputRowCount === 0
    ? "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    : "d".repeat(64);
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "GET",
      path: `/v0/pipes/identity_compaction_manifest.json?${query.toString()}`,
    })
    .reply(200, {
      data: [{
        tenant_id: "boom",
        batch_version: input.batchVersion,
        batch_id: input.batchId,
        input_event_count: input.inputEventCount,
        input_hash: "b".repeat(64),
        checkpoint_ingested_at: "2026-08-26 12:34:59.123456",
        checkpoint_event_id: "c".repeat(64),
        expected_output_row_count: outputRowCount,
        expected_output_hash: outputHash,
        actual_output_row_count: outputRowCount,
        actual_output_hash: outputHash,
        is_valid: input.isValid === false ? 0 : 1,
      }],
    });
}

function mockEmptyWorkerBatch(input: {
  batchId: string;
  batchVersion: number;
  cursorAt: string;
}): void {
  const pendingQuery = new URLSearchParams([
    ["p_batch_limit", "1000"],
    ["p_cursor_event_id", zeroHash],
    ["p_cursor_ingested_at", input.cursorAt],
    ["p_tenant_id", "boom"],
  ]);
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "GET",
      path: `/v0/pipes/identity_worker_pending_facts.json?${pendingQuery.toString()}`,
    })
    .reply(200, { data: [] });

  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: "/v0/events?name=identity_state_delta_versions&wait=true",
    })
    .reply(200, { successful_rows: 1 });

  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: "/v0/events?name=reporting_journey_identity_queue&wait=true",
    })
    .reply(200, { successful_rows: 1 });

  const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  const manifestQuery = new URLSearchParams({
    p_batch_id: input.batchId,
    p_batch_version: String(input.batchVersion),
    p_tenant_id: "boom",
  });
  const manifest = {
    data: [{
      tenant_id: "boom",
      batch_version: input.batchVersion,
      batch_id: input.batchId,
      input_event_count: 0,
      input_hash: emptyHash,
      checkpoint_ingested_at: input.cursorAt,
      checkpoint_event_id: zeroHash,
      expected_output_row_count: 0,
      expected_output_hash: emptyHash,
      actual_output_row_count: 0,
      actual_output_hash: emptyHash,
      is_valid: 1,
    }],
  };
  for (let index = 0; index < 2; index += 1) {
    fetchMock.get(tinybirdOrigin)
      .intercept({
        method: "GET",
        path: `/v0/pipes/identity_compaction_manifest.json?${manifestQuery.toString()}`,
      })
      .reply(200, manifest);
  }
}

function mockActivation(): void {
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: "/v0/events?name=identity_state_delta_versions&wait=true",
    })
    .reply(200, { successful_rows: 1 });
}

function mockEmptySourcePage(
  producerId: string,
  sourceIngestedFrom: string,
  sourceIngestedTo?: string,
): void {
  const query = new URLSearchParams();
  query.set("p_after_fact_key", "");
  query.set("p_batch_limit", "1000");
  query.set("p_identity_producer", producerId);
  query.set("p_source_ingested_from", sourceIngestedFrom);
  if (sourceIngestedTo) query.set("p_source_ingested_to", sourceIngestedTo);
  fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "GET",
      path: `/v0/pipes/identity_worker_source_facts.json?${query.toString()}`,
    })
    .reply(200, { data: [] });
}

function mockCopyJob(jobId: string): void {
  fetchMock.get(tinybirdOrigin)
    .intercept({ method: "GET", path: `/v0/jobs/${jobId}` })
    .reply(200, { status: "done" });
}

function seedIdentityPhase(
  state: DurableObjectState,
  generationId: string,
  identityPhase:
    | "compute"
    | "prepare"
    | "changes"
    | "profiles"
    | "touched_profiles"
    | "scope"
    | "fact_keys"
    | "facts_0"
    | "facts_1"
    | "facts_2"
    | "facts_3"
    | "facts_4"
    | "facts_5"
    | "facts_6"
    | "facts_7"
    | "components"
    | "compact"
    | "validate"
    | "activate",
  scheduledAt: string,
): void {
  state.storage.sql.exec(
    `
      UPDATE coordinator_state
      SET
        phase = 'copying',
        bootstrap_status = 'complete',
        active_raw_generation = ?,
        publication_kind = 'recurring',
        publication_section = 'identity',
        identity_phase = ?
      WHERE id = 1
    `,
    generationId,
    identityPhase,
  );
  seedPublication(state, generationId);
  seedCompletedRawRun(state, generationId, scheduledAt);
}

function seedPreparedIdentityPhase(
  state: DurableObjectState,
  generationId: string,
  identityPhase: "validate" | "activate",
  scheduledAt: string,
  batchVersion: number,
  batchId: string,
): void {
  seedIdentityPhase(state, generationId, identityPhase, scheduledAt);
  state.storage.sql.exec(
    `
      UPDATE coordinator_state
      SET
        identity_batch_version = ?,
        identity_batch_id = ?,
        identity_cursor_ingested_at = '1970-01-01 00:00:00.000000',
        identity_cursor_event_id = ?
      WHERE id = 1
    `,
    batchVersion,
    batchId,
    zeroHash,
  );
}

function seedIdentityEnqueue(
  state: DurableObjectState,
  generationId: string,
  cutoff: string,
  kind: "bootstrap" | "recurring" = "recurring",
  enqueueIndex = 0,
): void {
  state.storage.sql.exec(
    `
      UPDATE coordinator_state
      SET
        phase = 'copying',
        bootstrap_status = ?,
        active_raw_generation = ?,
        publication_kind = ?,
        publication_section = 'identity',
        identity_phase = 'enqueue',
        identity_enqueue_index = ?,
        identity_enqueue_after_fact_key = '',
        identity_enqueue_ingested_at = '2026-08-28 00:00:00.000000',
        identity_overlap_cutoff = ?
      WHERE id = 1
    `,
    kind === "bootstrap" ? "requested" : "complete",
    generationId,
    kind,
    enqueueIndex,
    cutoff,
  );
  seedPublication(state, generationId, kind);
  if (kind === "recurring") {
    const cutoffDate = new Date(`${cutoff.replace(" ", "T")}Z`);
    const scheduledAt = new Date(cutoffDate.valueOf() + 10 * 60_000).toISOString();
    seedCompletedRawRun(state, generationId, scheduledAt);
  }
}

function seedAwaitingPublication(
  state: DurableObjectState,
  generationId: string,
  scheduledAt: string,
): void {
  state.storage.sql.exec(
    `
      UPDATE coordinator_state
      SET
        phase = 'awaiting_bootstrap',
        active_raw_generation = ?,
        publication_kind = NULL,
        publication_section = NULL
      WHERE id = 1
    `,
    generationId,
  );
  seedCompletedRawRun(state, generationId, scheduledAt);
}

function seedPublication(
  state: DurableObjectState,
  generationId: string,
  kind: "bootstrap" | "recurring" = "recurring",
): void {
  state.storage.sql.exec(
    `
      INSERT INTO publication_generations (
        generation_id,
        kind,
        status,
        started_at_ms,
        completed_at_ms,
        last_error
      ) VALUES (?, ?, 'running', ?, NULL, NULL)
    `,
    generationId,
    kind,
    Date.now(),
  );
}

function seedCopyPublication(
  state: DurableObjectState,
  generationId: string,
  kind: "bootstrap" | "recurring",
): void {
  state.storage.sql.exec(
    `
      UPDATE coordinator_state
      SET
        phase = 'copying',
        bootstrap_status = ?,
        active_raw_generation = ?,
        publication_kind = ?,
        publication_section = 'pre_identity',
        publication_index = 0
      WHERE id = 1
    `,
    kind === "bootstrap" ? "requested" : "complete",
    generationId,
    kind,
  );
  seedPublication(state, generationId, kind);
}

function seedCompletedRawRun(
  state: DurableObjectState,
  generationId: string,
  scheduledAt: string,
): void {
  state.storage.sql.exec(
    `
      INSERT INTO raw_runs (
        request_key,
        scheduled_at,
        slot,
        generation_id,
        status,
        attempt_count,
        last_error,
        created_at_ms,
        updated_at_ms
      ) VALUES (?, ?, 0, ?, 'completed', 1, NULL, ?, ?)
    `,
    `${generationId}_raw`,
    scheduledAt,
    generationId,
    Date.now(),
    Date.now(),
  );
}
