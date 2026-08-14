<script setup lang="ts">
type Status = "available" | "next" | "planned" | "proposed" | "invariant";

withDefaults(
  defineProps<{
    status: Status;
    label?: string;
  }>(),
  { label: undefined },
);

const labels: Record<Status, string> = {
  available: "Available",
  next: "Next",
  planned: "Planned",
  proposed: "Proposed",
  invariant: "Invariant",
};
</script>

<template>
  <span class="status-badge" :class="`status-badge--${status}`">
    <span class="status-badge__dot" aria-hidden="true" />
    {{ label ?? labels[status] }}
  </span>
</template>
