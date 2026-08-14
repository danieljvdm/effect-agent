<script setup lang="ts">
defineProps<{
  success?: string;
  failure?: string;
  requirements?: string;
}>();
</script>

<template>
  <div class="contract-panel" aria-label="Effect runtime contract">
    <div class="contract-panel__signature">
      <span>Effect</span><span class="contract-panel__angle">&lt;</span>
      <span class="contract-panel__type">A</span><span>, </span>
      <span class="contract-panel__error">E</span><span>, </span>
      <span class="contract-panel__requirements">R</span>
      <span class="contract-panel__angle">&gt;</span>
    </div>
    <dl class="contract-panel__legend">
      <div>
        <dt>A</dt>
        <dd>{{ success ?? "decoded result" }}</dd>
      </div>
      <div>
        <dt>E</dt>
        <dd>{{ failure ?? "expected failures" }}</dd>
      </div>
      <div>
        <dt>R</dt>
        <dd>{{ requirements ?? "required Layers" }}</dd>
      </div>
    </dl>
  </div>
</template>
