<script>
  import { createEventDispatcher } from 'svelte';

  export let templates = [];
  export let searches = [];
  export let where = '';
  export let selectedSearchId = '';
  export let selectedTemplateId = '';
  export let searchableAttributes = [];
  export let templateAttributes = [];
  export let orderBy = 'unitId';
  export let orderDirection = 'asc';

  const dispatch = createEventDispatcher();
  let queryInput;

  const unitFields = [
    { name: 'unitid', type: 'LONG' },
    { name: 'unitver', type: 'INTEGER' },
    { name: 'unitname', type: 'STRING' },
    { name: 'status', type: 'STATUS' },
    { name: 'created', type: 'TIME' },
    { name: 'modified', type: 'TIME' },
    { name: 'corrid', type: 'STRING' }
  ];

  const dateFilters = [
    { id: 'created-today', label: 'Created today' },
    { id: 'modified-today', label: 'Modified today' },
    { id: 'created-after', label: 'Created after…' },
    { id: 'created-before', label: 'Created before…' }
  ];

  const builderFilters = [
    { id: 'effective', label: 'Effective' },
    { id: 'title-as', label: 'Title as…' },
    { id: 'unitname-contains', label: 'Unit name contains…' },
    { id: 'description-contains', label: 'Description contains…' }
  ];

  const selectSavedSearch = (event) => {
    const id = event.target.value;
    selectedSearchId = id;
    const selected = searches.find((search) => String(search.id) === String(id));
    dispatch('select', { id, search: selected || null });
  };

  const selectTemplate = (event) => {
    const id = event.target.value;
    selectedTemplateId = id;
    dispatch('template-change', { id });
  };

  const notifyOrderChange = () => {
    dispatch('order-change', { orderBy, orderDirection });
  };

  const runSearch = () => {
    dispatch('run', { where });
  };

  const saveSearch = () => {
    dispatch('save', { where });
  };

  const renameSearch = () => {
    if (!selectedSearchId) return;
    dispatch('rename', { id: selectedSearchId });
  };

  const deleteSearch = () => {
    if (!selectedSearchId) return;
    dispatch('delete', { id: selectedSearchId });
  };

  const addFilter = (id) => {
    dispatch('add-filter', { id });
  };

  let pendingDateFilter = '';
  let datePicker;

  const openDatePicker = (id) => {
    pendingDateFilter = id;
    setTimeout(() => {
      if (datePicker) {
        datePicker.focus();
        datePicker.click();
        if (typeof datePicker.showPicker === 'function') {
          datePicker.showPicker();
        }
      }
    }, 0);
  };

  const onDatePicked = (event) => {
    const value = event.target.value;
    if (!value || !pendingDateFilter) return;
    dispatch('add-filter-date', { id: pendingDateFilter, value });
    event.target.value = '';
    pendingDateFilter = '';
    if (event.target && typeof event.target.blur === 'function') {
      event.target.blur();
    }
  };

  const addSavedSearch = (search) => {
    if (!search) return;
    dispatch('add-saved', { search });
  };

  const resetSearch = () => {
    selectedSearchId = '';
    dispatch('reset');
  };

  let infoPanel;

  const closeInfoPanel = () => {
    if (infoPanel) {
      infoPanel.open = false;
    }
  };

  const insertField = (field, type) => {
    dispatch('insert-field', { field, type, input: queryInput });
    closeInfoPanel();
  };

  const insertOperator = (operator) => {
    dispatch('insert-operator', { operator, input: queryInput });
    closeInfoPanel();
  };

  const storageKey = 'ipto.searchFilters.open';
  let filtersOpen = false;

  if (typeof localStorage !== 'undefined') {
    const stored = localStorage.getItem(storageKey);
    if (stored !== null) {
      filtersOpen = stored === 'true';
    }
  }

  const onFiltersToggle = (event) => {
    filtersOpen = event.currentTarget?.open ?? false;
    if (typeof localStorage !== 'undefined') {
      localStorage.setItem(storageKey, String(filtersOpen));
    }
  };
</script>

<div class="filters">
  <div class="field">
    <label>Template</label>
    <select bind:value={selectedTemplateId} on:change={selectTemplate}>
      <option value="">No template</option>
      {#each templates as template}
        <option value={String(template._id ?? template.id)}>
          {template.displayName || template.name || template._name || `Template ${template._id ?? template.id}`}
        </option>
      {/each}
    </select>
  </div>
  <div class="field">
    <label>Saved searches</label>
    <select bind:value={selectedSearchId} on:change={selectSavedSearch}>
      <option value="">Select saved search</option>
      {#each searches as search}
        <option value={search.id}>{search.name}</option>
      {/each}
    </select>
  </div>
  <div class="field">
    <label>Sort by</label>
    <select bind:value={orderBy} on:change={notifyOrderChange}>
      <option value="unitId">Unit id</option>
      <option value="created">Created</option>
      <option value="modified">Modified</option>
    </select>
  </div>
  <div class="field">
    <label>Direction</label>
    <select bind:value={orderDirection} on:change={notifyOrderChange}>
      <option value="asc">Ascending</option>
      <option value="desc">Descending</option>
    </select>
  </div>
  <div class="field wide">
    <div class="label-row">
      <label>Search expression</label>
      <details class="info" bind:this={infoPanel}>
        <summary aria-label="Search expression help">?</summary>
        <div class="info-panel">
          <div class="info-title">Searchable unit fields</div>
          <div class="info-list">
            {#each unitFields as field}
              <button type="button" class="info-chip" on:click={() => insertField(field.name, field.type)}>
                {field.name}
              </button>
            {/each}
          </div>
          <div class="info-title">Operators</div>
          <div class="info-list">
            {#each ['=', '!=', '>', '>=', '<', '<=', 'LIKE'] as op}
              <button type="button" class="info-chip" on:click={() => insertOperator(op)}>
                {op}
              </button>
            {/each}
          </div>
          <div class="info-title">Searchable attributes</div>
          {#if searchableAttributes.length}
            <div class="info-list">
              {#each searchableAttributes as attr}
                <button type="button" class="info-chip" on:click={() => insertField(attr.name, attr.type)}>
                  {attr.name}
                </button>
              {/each}
            </div>
          {:else}
            <div class="info-empty">No searchable attributes available.</div>
          {/if}
        </div>
      </details>
    </div>
    <textarea
      rows="3"
      bind:this={queryInput}
      bind:value={where}
      placeholder="Provide a search query"
    ></textarea>
    <div class="actions actions-inline">
      <button type="button" class="primary" on:click={runSearch} title="Run the current query">Run search</button>
      <button type="button" on:click={saveSearch} title="Save the current query as a saved search">Save search</button>
      <button
        type="button"
        class="ghost"
        disabled={!selectedSearchId}
        on:click={renameSearch}
        title="Rename the selected saved search"
      >
        Rename
      </button>
      <button
        type="button"
        class="ghost"
        disabled={!selectedSearchId}
        on:click={deleteSearch}
        title="Delete the selected saved search"
      >
        Delete
      </button>
      <button type="button" on:click={resetSearch} title="Clear the query and results">Reset</button>
    </div>
  </div>
  <details class="field wide filters" bind:open={filtersOpen} on:toggle={onFiltersToggle}>
    <summary class="filters-summary">
      <span class="filters-caret" aria-hidden="true">{filtersOpen ? '▼' : '▶'}</span>
      <span class="filters-title">Attribute filters</span>
      <span class="filters-hint" title="Query builder helpers for common filters.">?</span>
    </summary>
    <div class="filters-body">
      <div class="chips">
        <div class="chip-group">
          <div class="chip-label">Date filters</div>
          <div class="chip-row">
            {#each dateFilters as filter}
              <button
                type="button"
                class="chip builder date"
                on:click={() =>
                  filter.id === 'created-before' || filter.id === 'created-after'
                    ? openDatePicker(filter.id)
                    : addFilter(filter.id)
                }
              >
                {filter.label}
              </button>
            {/each}
            {#if pendingDateFilter}
              <input
                class="date-picker-inline"
                type="date"
                bind:this={datePicker}
                on:change={onDatePicked}
                aria-label={pendingDateFilter === 'created-before' ? 'Created before date' : 'Created after date'}
              />
            {/if}
          </div>
        </div>
        <div class="chip-group">
          <div class="chip-label">Quick filters</div>
          <div class="chip-row">
            {#each builderFilters as filter}
              <button
                type="button"
                class="chip builder"
                on:click={() => addFilter(filter.id)}
              >
                {filter.label}
              </button>
            {/each}
          </div>
        </div>
        {#if templateAttributes.length}
          <div class="chip-group">
            <div class="chip-label">Template attributes</div>
            <div class="chip-row">
              {#each templateAttributes as attr}
                <button
                  type="button"
                  class="chip saved"
                  class:disabled={!attr.searchable}
                  on:click={() => attr.searchable && insertField(attr.name, attr.type)}
                  title={attr.searchable ? 'Insert into query' : 'Not searchable'}
                  disabled={!attr.searchable}
                >
                  {attr.name}
                </button>
              {/each}
            </div>
          </div>
        {/if}
      </div>
      {#if searches.length}
        <div class="saved-searches">
          <div class="chip-label">Saved searches</div>
          <div class="chip-row">
            {#each searches as search}
              <button type="button" class="chip saved" on:click={() => addSavedSearch(search)}>
                {search.name}
              </button>
            {/each}
          </div>
        </div>
      {:else}
        <div class="empty">No saved searches yet.</div>
      {/if}
    </div>
  </details>
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

  .label-row {
    display: flex;
    align-items: center;
    gap: 0.6rem;
  }

  .info {
    position: relative;
  }

  .info summary {
    list-style: none;
    width: 1.2rem;
    height: 1.2rem;
    border-radius: 999px;
    border: 1px solid rgba(255, 255, 255, 0.3);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 0.75rem;
    color: var(--text-muted);
    cursor: pointer;
  }

  .info summary::-webkit-details-marker {
    display: none;
  }

  .info[open] summary {
    color: var(--text);
    border-color: rgba(255, 255, 255, 0.5);
  }

  .info-panel {
    position: absolute;
    top: 1.6rem;
    left: 0;
    z-index: 5;
    width: min(320px, 70vw);
    background: rgba(12, 15, 25, 0.98);
    border: 1px solid rgba(255, 255, 255, 0.14);
    border-radius: 0.8rem;
    padding: 0.8rem;
    display: grid;
    gap: 0.6rem;
    box-shadow: 0 18px 30px rgba(0, 0, 0, 0.35);
  }

  .info-title {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    color: var(--text-muted);
  }

  .info-list {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }

  .info-chip {
    border: none;
    background: rgba(255, 255, 255, 0.08);
    color: var(--text);
    font-size: 0.75rem;
    padding: 0.2rem 0.5rem;
    border-radius: 999px;
    cursor: pointer;
  }

  .info-chip:hover {
    background: rgba(255, 255, 255, 0.16);
  }

  .info-empty {
    color: var(--text-muted);
    font-size: 0.8rem;
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
    flex-direction: column;
    gap: 0.8rem;
  }

  .chips span {
    background: rgba(255, 255, 255, 0.08);
    padding: 0.4rem 0.7rem;
    border-radius: 999px;
    font-size: 0.85rem;
  }

  .chip {
    font-size: 0.85rem;
    padding: 0.4rem 0.7rem;
    border-radius: 999px;
  }

  .chip.builder {
    border: 1px solid rgba(255, 255, 255, 0.2);
    background: rgba(255, 255, 255, 0.08);
    color: var(--text);
    cursor: pointer;
  }

  .chip.builder.date {
    border-color: rgba(159, 208, 255, 0.4);
    background: rgba(122, 180, 255, 0.12);
  }

  .chip.saved {
    border: 1px dashed rgba(255, 255, 255, 0.25);
    background: transparent;
    color: var(--text-muted);
  }

  .chip.saved.disabled {
    opacity: 0.5;
    cursor: not-allowed;
    border-color: rgba(255, 255, 255, 0.15);
  }

  .saved-searches {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    margin-top: 0.6rem;
  }

  .filters {
    border: 1px dashed rgba(255, 255, 255, 0.18);
    border-radius: 0.9rem;
    padding: 0.8rem 1rem;
    background: rgba(255, 255, 255, 0.02);
  }

  .filters-summary {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    cursor: pointer;
    list-style: none;
  }

  .filters-summary::-webkit-details-marker {
    display: none;
  }

  .filters-title {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    color: var(--text-muted);
  }

  .filters-caret {
    border: none;
    background: rgba(255, 255, 255, 0.08);
    color: var(--text-muted);
    border-radius: 0.4rem;
    width: 1.4rem;
    height: 1.4rem;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex: 0 0 auto;
  }

  .filters-hint {
    width: 1.1rem;
    height: 1.1rem;
    border-radius: 999px;
    border: 1px solid rgba(255, 255, 255, 0.3);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 0.7rem;
    color: var(--text-muted);
  }

  .filters-body {
    margin-top: 0.8rem;
    display: grid;
    gap: 0.8rem;
  }

  .chip-group {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .chip-label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    color: rgba(255, 255, 255, 0.38);
  }

  .chip-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.6rem;
    align-items: center;
  }

  .date-picker-inline {
    border-radius: 999px;
    padding: 0.35rem 0.6rem;
    border: 1px solid rgba(255, 255, 255, 0.2);
    background: rgba(8, 10, 18, 0.8);
    color: var(--text);
    font-size: 0.85rem;
  }

  .helper,
  .empty {
    margin-top: 0.4rem;
    color: var(--text-muted);
    font-size: 0.85rem;
  }

  .actions {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    flex-wrap: wrap;
  }

  .actions-inline {
    margin-top: 0.6rem;
    justify-content: flex-start;
  }

  .actions button {
    border: none;
    border-radius: 999px;
    padding: 0.5rem 1rem;
    background: rgba(255, 255, 255, 0.1);
    color: var(--text);
    cursor: pointer;
  }

  .actions button:disabled {
    cursor: not-allowed;
    opacity: 0.5;
  }

  .primary {
    background: var(--accent);
    color: #0c0f19;
    font-weight: 600;
  }

  .ghost {
    background: transparent;
    border: 1px solid rgba(255, 255, 255, 0.2);
  }
</style>
