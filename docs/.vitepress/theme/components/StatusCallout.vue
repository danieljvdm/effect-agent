<script setup lang="ts">
withDefaults(
  defineProps<{
    status: "available" | "next" | "planned" | "proposed";
    phase?: string;
    title?: string;
  }>(),
  { phase: undefined, title: undefined },
);
</script>

<template>
  <aside class="status-callout" :class="`status-callout--${status}`">
    <div class="status-callout__heading">
      <StatusBadge :status="status" />
      <span v-if="phase" class="status-callout__phase">{{ phase }}</span>
    </div>
    <strong v-if="title" class="status-callout__title">{{ title }}</strong>
    <div class="status-callout__body"><slot /></div>
  </aside>
</template>
