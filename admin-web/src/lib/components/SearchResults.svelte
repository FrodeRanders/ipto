<script>
  import { snyggifyTime } from '$lib/api.js';

  export let units = [];
  export let selectedId;
  export let onSelect = () => {};
  export let page = 1;
  export let pageSize = 30;
  export let hasNext = false;
  export let canPage = true;
  export let onPrev = () => {};
  export let onNext = () => {};
  export let onPageSizeChange = () => {};
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
  <div class="footer">
    <div class="pager">
      <button type="button" on:click={onPrev} disabled={!canPage || page <= 1}>Prev</button>
      <span>Page {page}</span>
      <button type="button" on:click={onNext} disabled={!canPage || !hasNext}>Next</button>
    </div>
    <div class="page-size">
      <label for="page-size">Page size</label>
      <select
        id="page-size"
        bind:value={pageSize}
        on:change={(event) => onPageSizeChange(Number(event.target.value))}
        disabled={!canPage}
      >
        <option value={20}>20</option>
        <option value={30}>30</option>
        <option value={50}>50</option>
        <option value={100}>100</option>
      </select>
    </div>
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

  .footer {
    margin-top: 0.4rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.8rem;
    flex-wrap: wrap;
  }

  .pager {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    color: var(--text-muted);
  }

  .pager button {
    border: none;
    border-radius: 999px;
    padding: 0.35rem 0.8rem;
    background: rgba(255, 255, 255, 0.08);
    color: var(--text);
    cursor: pointer;
  }

  .pager button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .page-size {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    color: var(--text-muted);
    font-size: 0.85rem;
  }

  .page-size select {
    background: rgba(8, 10, 18, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-radius: 0.6rem;
    padding: 0.35rem 0.5rem;
  }
</style>
