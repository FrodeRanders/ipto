<script>
  import { snyggifyTime } from '$lib/api.js';

  export let units = [];
  export let selectedId;
  export let onSelect = () => {};
</script>

<div class="results">
  <div class="header">
    <h3>Results</h3>
    <span>{units.length} units</span>
  </div>
  <div class="list">
    {#each units as unit}
      <div class="row" class:selected={unit.id === selectedId} on:click={() => onSelect(unit)}>
        <div>
          <div class="name">{unit.name}</div>
          <div class="meta">{unit.id} · Tenant {unit.tenantId} · {unit.status}</div>
        </div>
        <div class="date">{snyggifyTime(unit.created)}</div>
      </div>
    {/each}
  </div>
</div>

<style>
  .results {
    display: flex;
    flex-direction: column;
    gap: 0.8rem;
    padding: 1rem;
    border-radius: 1.2rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    background: rgba(255, 255, 255, 0.04);
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  h3 {
    margin: 0;
    font-family: 'Space Grotesk', sans-serif;
  }

  span {
    color: var(--text-muted);
  }

  .list {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
    max-height: var(--results-max-height, 60vh);
    overflow-y: auto;
    padding-right: 0.2rem;
    scrollbar-gutter: stable;
  }

  .row {
    display: flex;
    justify-content: space-between;
    gap: 0.8rem;
    padding: 0.8rem 1rem;
    border-radius: 0.8rem;
    border: 1px solid transparent;
    cursor: pointer;
    background: rgba(255, 255, 255, 0.02);
  }

  .row.selected {
    border-color: rgba(255, 190, 92, 0.4);
    background: rgba(255, 190, 92, 0.1);
  }

  .name {
    font-weight: 600;
  }

  .meta {
    color: var(--text-muted);
    font-size: 0.85rem;
  }

  .date {
    color: var(--text-muted);
    font-size: 0.85rem;
  }
</style>
