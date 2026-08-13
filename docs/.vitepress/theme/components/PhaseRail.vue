<script setup lang="ts">
const phases = [
  { id: "P0", title: "Design proof", status: "available" },
  { id: "P1", title: "Ephemeral core", status: "available" },
  { id: "P2", title: "Local runtime", status: "available" },
  { id: "P3", title: "Persistence", status: "available" },
  { id: "P4", title: "Node durability", status: "next" },
  { id: "P5", title: "Durable tools", status: "planned" },
  { id: "P6", title: "Cloudflare", status: "planned" },
  { id: "P7", title: "Hardening", status: "planned" },
] as const;
</script>

<template>
  <ol class="phase-rail" aria-label="Effect Agent implementation phases">
    <li
      v-for="phase in phases"
      :key="phase.id"
      :class="`phase-rail__item phase-rail__item--${phase.status}`"
    >
      <span class="phase-rail__marker" aria-hidden="true" />
      <span class="phase-rail__id">{{ phase.id }}</span>
      <strong>{{ phase.title }}</strong>
      <StatusBadge :status="phase.status" />
    </li>
  </ol>
</template>
