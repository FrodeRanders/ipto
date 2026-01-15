<script>
  import { createEventDispatcher } from 'svelte';

  export let templates = [];
  export let searches = [];
  export let where = '';

  const dispatch = createEventDispatcher();

  const runSearch = () => {
    dispatch('run', { where });
  };
</script>

<div class="filters">
  <div class="field">
    <label>Template</label>
    <select>
      {#each templates as template}
        <option>{template.name}</option>
      {/each}
    </select>
  </div>
  <div class="field">
    <label>Saved searches</label>
    <select>
      {#each searches as search}
        <option>{search.name}</option>
      {/each}
    </select>
  </div>
  <div class="field wide">
    <label>Search expression</label>
    <textarea rows="3" bind:value={where} placeholder='dcterms:title = "Report" AND status = EFFECTIVE'></textarea>
  </div>
  <div class="field wide">
    <label>Attribute filters</label>
    <div class="chips">
      <span>dcterms:date >= 2024-01-01</span>
      <span>dcterms:title contains "Case"</span>
      <button type="button">Add filter</button>
    </div>
  </div>
  <div class="actions">
    <button type="button" class="primary" on:click={runSearch}>Run search</button>
    <button type="button">Reset</button>
  </div>
</div>

<style>
  .filters {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    padding: 1.2rem;
    border-radius: 1.2rem;
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.08);
  }

  .field {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }

  label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    color: var(--text-muted);
  }

  select {
    background: rgba(8, 10, 18, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-radius: 0.6rem;
    padding: 0.5rem 0.6rem;
  }

  textarea {
    background: rgba(8, 10, 18, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-radius: 0.8rem;
    padding: 0.6rem 0.7rem;
    resize: vertical;
    min-height: 70px;
  }

  .wide {
    grid-column: 1 / -1;
  }

  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.6rem;
    align-items: center;
  }

  .chips span {
    background: rgba(255, 255, 255, 0.08);
    padding: 0.4rem 0.7rem;
    border-radius: 999px;
    font-size: 0.85rem;
  }

  .chips button {
    border: 1px dashed rgba(255, 255, 255, 0.3);
    background: transparent;
    color: var(--text-muted);
    padding: 0.4rem 0.7rem;
    border-radius: 999px;
    cursor: pointer;
  }

  .actions {
    display: flex;
    align-items: center;
    gap: 0.8rem;
  }

  .actions button {
    border: none;
    border-radius: 999px;
    padding: 0.5rem 1rem;
    background: rgba(255, 255, 255, 0.1);
    color: var(--text);
    cursor: pointer;
  }

  .primary {
    background: var(--accent);
    color: #0c0f19;
    font-weight: 600;
  }
</style>
