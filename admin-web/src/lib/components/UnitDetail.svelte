<script>
  import { snyggifyTime } from '$lib/api.js';
  import AttributeItem from './AttributeItem.svelte';

  export let unit;

  const coreKeys = ['dcterms:title', 'dcterms:date'];
</script>

{#if unit}
  <div class="detail">
    <div class="header">
      <div>
        <div class="kicker">Unit</div>
        <h2>{unit.name}</h2>
        <div class="meta">{unit.tenantId}&nbsp;&bull;&nbsp;{unit.unitId}&nbsp;#&nbsp;{unit.version}</div>
      </div>
      <div class="status">{unit.status}</div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="label">Created</div>
        <div class="value">{snyggifyTime(unit.created)}</div>
        <div class="label">Modified</div>
        <div class="value">{snyggifyTime(unit.modified)}</div>
      </div>
      <div class="card">
        <div class="label">Key attributes</div>
        {#each coreKeys as key}
          <div class="value">
            {key}:
            {#if unit.attributes[key]?.value !== undefined}
              {unit.attributes[key].value}
            {:else}
              {unit.attributes[key]}
            {/if}
          </div>
        {/each}
      </div>
    </div>

    <div class="attributes">
      <h3>Attributes</h3>
      {#each Object.entries(unit.attributes) as [key, value]}
        <AttributeItem name={key} data={value} />
      {/each}
    </div>
  </div>
{:else}
  <div class="empty">
    <h3>Select a unit</h3>
    <p>The unit detail will appear here.</p>
  </div>
{/if}

<style>
  .detail {
    display: flex;
    flex-direction: column;
    gap: 1.4rem;
    padding: 1.4rem;
    border-radius: 1.2rem;
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.08);
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }

  .kicker {
    font-size: 0.75rem;
    letter-spacing: 0.12rem;
    text-transform: uppercase;
    color: var(--text-muted);
  }

  h2 {
    margin: 0.3rem 0;
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.8rem;
  }

  .meta {
    color: var(--text-muted);
  }

  .status {
    background: rgba(255, 190, 92, 0.15);
    color: var(--accent);
    padding: 0.3rem 0.7rem;
    border-radius: 999px;
    font-size: 0.8rem;
  }

  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 1rem;
  }

  .card {
    background: rgba(12, 15, 25, 0.8);
    border-radius: 0.9rem;
    padding: 1rem;
    border: 1px solid rgba(255, 255, 255, 0.06);
  }

  .label {
    font-size: 0.75rem;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    margin-top: 0.6rem;
  }

  .value {
    font-size: 0.95rem;
  }

  .attributes {
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .attributes h3 {
    margin: 0;
    font-family: 'Space Grotesk', sans-serif;
  }

  .empty {
    border: 1px dashed rgba(255, 255, 255, 0.2);
    border-radius: 1rem;
    padding: 2rem;
    text-align: center;
  }

  .empty p {
    color: var(--text-muted);
  }
</style>
