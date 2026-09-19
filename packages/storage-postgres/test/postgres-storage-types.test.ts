import type * as PostgresStorageClient from "@effect-agent/storage-postgres/postgres-storage-client";
import type { PostgresStorageConfig } from "@effect-agent/storage-postgres/postgres-storage-config";
import type {
  PostgresStorageError,
  PostgresStorageInitializationError,
} from "@effect-agent/storage-postgres/postgres-storage-error";
import type { PostgresStorageFailpoint } from "@effect-agent/storage-postgres/postgres-storage-failpoint";
import type * as PostgresSubmissionLedger from "@effect-agent/storage-postgres/postgres-submission-ledger";
import type * as PostgresThreadStore from "@effect-agent/storage-postgres/postgres-thread-store";
import { expect, it } from "@effect/vitest";
import type { Crypto, Layer } from "effect";
import type * as SqlClientService from "effect/unstable/sql/SqlClient";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

/** The dialect is provided inside each store's Layer, so it never reaches a composition root. */
type ThreadStoreRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof PostgresThreadStore.layerWithServices>,
    PostgresStorageConfig | PostgresStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type ThreadStoreErrorProof = Assert<
  Equal<
    Layer.Error<typeof PostgresThreadStore.layerWithServices>,
    PostgresStorageInitializationError
  >
>;
type LedgerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof PostgresSubmissionLedger.layerWithServices>,
    PostgresStorageConfig | PostgresStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type LedgerErrorProof = Assert<
  Equal<
    Layer.Error<typeof PostgresSubmissionLedger.layerWithServices>,
    PostgresStorageInitializationError
  >
>;

type ConvenienceLayerProof = Assert<
  Equal<Layer.Services<ReturnType<typeof PostgresThreadStore.layer>>, never>
>;

type ClientErrorProof = Assert<
  Equal<Layer.Error<ReturnType<typeof PostgresStorageClient.layer>>, PostgresStorageError>
>;

it("keeps configuration, failpoint and client authority in the named Layer inputs", () => {
  const proofs: ReadonlyArray<true> = [
    true satisfies ThreadStoreRequirementsProof,
    true satisfies ThreadStoreErrorProof,
    true satisfies LedgerRequirementsProof,
    true satisfies LedgerErrorProof,
    true satisfies ConvenienceLayerProof,
    true satisfies ClientErrorProof,
  ];

  expect(proofs).toHaveLength(6);
});
