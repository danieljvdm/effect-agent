import {
  possessionChildAdmissionAuthorizerLayer,
  possessionOperationAuthorizerLayer,
} from "@effect-agent/session";
import { Layer } from "effect";

/**
 * Explicit possession-based authorization for trusted local durability tests.
 * Tests exercising denial or tenant policy provide their own authorizers.
 */
export const TrustedLocalDurableAuthorizationLayer = Layer.merge(
  possessionOperationAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
);
