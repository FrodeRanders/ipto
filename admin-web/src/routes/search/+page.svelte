<script>
  import { onMount } from 'svelte';
  import { fetchTenants, fetchTemplates, fetchSearches, fetchAttributeMetadata, searchUnits, fetchUnitById } from '$lib/api.js';
  import SectionTitle from '$lib/components/SectionTitle.svelte';
  import SearchFilters from '$lib/components/SearchFilters.svelte';
  import SearchResults from '$lib/components/SearchResults.svelte';
  import UnitDetail from '$lib/components/UnitDetail.svelte';
  import { tenantId } from '$lib/stores/tenant.js';

  let units = [];
  let selected = null;
  let error = '';
  let queryHint = '';
  let tenantError = '';
  let tenants = [];
  let templates = [];
  let searches = [];
  let savedSearches = [];
  let selectedSearchId = '';
  let searchableAttributes = [];
  let templateAttributes = [];
  let templateAttributeNames = [];
  let searchableAttributeMap = new Map();
  let selectedTemplateId = '';
  let unitError = '';
  let where = '';
  let searchBusy = false;
  let pageSize = 30;
  let offset = 0;
  let orderBy = 'unitId';
  let orderDirection = 'asc';
  let lastTenantId = null;

  const savedSearchKey = 'ipto.savedSearches';

  $: queryHint = !searchBusy && !(where || '').trim() ? 'Enter a search query to run.' : '';

  const isNumericId = (value) => /^\d+$/.test(String(value));

  const loadSavedSearches = () => {
    if (typeof localStorage === 'undefined') return [];
    try {
      const raw = localStorage.getItem(savedSearchKey);
      const parsed = raw ? JSON.parse(raw) : [];
      return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
      return [];
    }
  };

  const persistSavedSearches = (items) => {
    if (typeof localStorage === 'undefined') return;
    localStorage.setItem(savedSearchKey, JSON.stringify(items));
  };

  const selectUnit = async (unit) => {
    unitError = '';
    selected = unit;
    if (unit && unit.tenantId != null && isNumericId(unit.id)) {
      try {
        const fresh = await fetchUnitById(unit.tenantId, unit.id);
        selected = fresh;
      } catch (err) {
        unitError = err?.message || 'Failed to refresh unit details.';
      }
    }
  };

  const runSearch = async (expression = where, nextOffset = offset, nextPageSize = pageSize) => {
    if ($tenantId === null) {
      return;
    }
    const query = (expression || '').trim();
    offset = Math.max(0, nextOffset);
    pageSize = Math.max(1, nextPageSize);
    if (!query) {
      units = [];
      selected = null;
      return;
    }
    try {
      searchBusy = true;
      error = '';
      units = await searchUnits({
        tenantId: $tenantId,
        where: query,
        offset,
        size: pageSize,
        orderBy,
        orderDirection
      });
      selected = units[0] || null;
    } catch (err) {
      error = err?.message || 'Failed to load units.';
    } finally {
      searchBusy = false;
    }
  };

  const applySavedSearch = (search) => {
    if (!search) return;
    where = search.where || '';
    offset = 0;
  };

  const updateTemplateAttributes = (id) => {
    selectedTemplateId = String(id ?? '');
    const selectedTemplate = templates.find(
      (template) => String(template._id ?? template.id) === String(id)
    );
    if (!selectedTemplate) {
      templateAttributes = [];
      templateAttributeNames = [];
      return;
    }
    templateAttributeNames = selectedTemplate._attributes || [];
    templateAttributes = templateAttributeNames
      .map((attrName) => ({
        name: attrName,
        type: searchableAttributeMap.get(attrName),
        searchable: searchableAttributeMap.has(attrName)
      }));
  };

  const saveCurrentSearch = () => {
    const trimmed = where.trim();
    if (!trimmed) {
      return;
    }
    const name = prompt('Name this search', trimmed.slice(0, 40));
    if (!name) {
      return;
    }
    const next = [
      ...savedSearches,
      {
        id: `local-${Date.now()}`,
        name: name.trim(),
        where: trimmed,
        createdAt: new Date().toISOString()
      }
    ];
    savedSearches = next;
    searches = next;
    selectedSearchId = '';
    persistSavedSearches(next);
  };

  const escapeQueryValue = (value) =>
    String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"');

  const appendClause = (clause) => {
    const trimmed = clause.trim();
    if (!trimmed) return;
    where = where.trim() ? `${where.trim()} AND ${trimmed}` : trimmed;
  };

  const addFilterComponent = ({ id }) => {
    switch (id) {
      case 'effective':
        appendClause('status = EFFECTIVE');
        break;
      case 'created-today': {
        const today = new Date().toISOString().slice(0, 10);
        appendClause(`(created >= "${today}T00:00:00Z" AND created <= "${today}T23:59:59Z")`);
        break;
      }
      case 'modified-today': {
        const today = new Date().toISOString().slice(0, 10);
        appendClause(`(modified >= "${today}T00:00:00Z" AND modified <= "${today}T23:59:59Z")`);
        break;
      }
      case 'title-as': {
        const input = prompt('Title equals', '');
        if (!input) break;
        appendClause(`dcterms:title = "${escapeQueryValue(input)}"`);
        break;
      }
      case 'unitname-contains': {
        const input = prompt('Unit name contains', '');
        if (!input) break;
        appendClause(`unitname = "*${escapeQueryValue(input)}*"`);
        break;
      }
      case 'description-contains': {
        const input = prompt('Description contains', '');
        if (!input) break;
        appendClause(`dcterms:description = "*${escapeQueryValue(input)}*"`);
        break;
      }
      default:
        break;
    }
  };

  const addDateFilterComponent = ({ id, value }) => {
    if (!value) {
      return;
    }
    switch (id) {
      case 'created-before':
        appendClause(`created <= "${value}T23:59:59Z"`);
        break;
      case 'created-after':
        appendClause(`created >= "${value}T00:00:00Z"`);
        break;
      default:
        break;
    }
  };

  const addSavedSearchClause = ({ search }) => {
    if (!search || !search.where) {
      return;
    }
    const clause = search.where.trim();
    if (!clause) {
      return;
    }
    appendClause(`(${clause})`);
  };

  const goPrevPage = () => {
    if (offset <= 0 || searchBusy) return;
    runSearch(where, Math.max(0, offset - pageSize), pageSize);
  };

  const goNextPage = () => {
    if (searchBusy || units.length < pageSize) return;
    runSearch(where, offset + pageSize, pageSize);
  };

  const changePageSize = (size) => {
    if (Number.isNaN(size) || size <= 0) return;
    offset = 0;
    pageSize = size;
    runSearch(where, 0, size);
  };

  const updateOrder = ({ orderBy: nextOrderBy, orderDirection: nextOrderDirection }) => {
    orderBy = nextOrderBy;
    orderDirection = nextOrderDirection;
    offset = 0;
    runSearch(where, 0, pageSize);
  };

  const insertTokenAtCursor = (input, token) => {
    let start = where.length;
    let end = where.length;
    if (input && typeof input.selectionStart === 'number') {
      start = input.selectionStart;
      end = input.selectionEnd ?? start;
    }
    const before = where.slice(0, start);
    const after = where.slice(end);
    where = `${before}${token}${after}`;
    return { start, length: token.length };
  };

  const focusAt = (input, pos) => {
    if (input && typeof input.setSelectionRange === 'function') {
      setTimeout(() => {
        input.focus();
        input.setSelectionRange(pos, pos);
      }, 0);
    }
  };

  const selectRange = (input, start, end) => {
    if (input && typeof input.setSelectionRange === 'function') {
      setTimeout(() => {
        input.focus();
        input.setSelectionRange(start, end);
      }, 0);
    }
  };

  const buildFieldToken = (field, type) => {
    if (type === 'STRING' || type === 'TIME') {
      return { token: `${field} = ""`, cursorOffset: -1 };
    }
    if (type === 'STATUS') {
      return { token: `${field} = EFFECTIVE`, select: 'EFFECTIVE' };
    }
    if (type === 'BOOLEAN') {
      return { token: `${field} = true`, select: 'true' };
    }
    return { token: `${field} = ` };
  };

  const insertFieldToken = ({ field, type, input }) => {
    const { token, cursorOffset, select } = buildFieldToken(field, type);
    const { start, length } = insertTokenAtCursor(input, token);
    if (cursorOffset !== undefined) {
      focusAt(input, start + length + cursorOffset);
      return;
    }
    if (select) {
      const selectStart = start + token.indexOf(select);
      selectRange(input, selectStart, selectStart + select.length);
      return;
    }
    focusAt(input, start + length);
  };

  const insertOperatorToken = ({ operator, input }) => {
    const { start, length } = insertTokenAtCursor(input, `${operator} `);
    focusAt(input, start + length);
  };

  const resetSearchForm = () => {
    where = '';
    selectedSearchId = '';
    units = [];
    selected = null;
    offset = 0;
  };

  const renameSavedSearch = ({ id }) => {
    const target = savedSearches.find((search) => String(search.id) === String(id));
    if (!target) {
      return;
    }
    const nextName = prompt('Rename this search', target.name);
    if (!nextName) {
      return;
    }
    const updated = savedSearches.map((search) =>
      String(search.id) === String(id) ? { ...search, name: nextName.trim() } : search
    );
    savedSearches = updated;
    searches = updated;
    persistSavedSearches(updated);
  };

  const deleteSavedSearch = ({ id }) => {
    const target = savedSearches.find((search) => String(search.id) === String(id));
    if (!target) {
      return;
    }
    const confirmed = confirm(`Delete saved search "${target.name}"?`);
    if (!confirmed) {
      return;
    }
    const updated = savedSearches.filter((search) => String(search.id) !== String(id));
    savedSearches = updated;
    searches = updated;
    selectedSearchId = '';
    persistSavedSearches(updated);
  };

  onMount(async () => {
    savedSearches = loadSavedSearches();
    searches = savedSearches;

    const [tenantResult, templateResult, searchResult] = await Promise.allSettled([
      fetchTenants(),
      fetchTemplates(),
      fetchSearches()
    ]);

    if (tenantResult.status === 'fulfilled') {
      tenants = tenantResult.value;
      if ($tenantId === null || !tenants.some((tenant) => tenant._tenant_id === $tenantId)) {
        tenantId.set(tenants[0]?._tenant_id ?? null);
      }
    } else {
      tenantError = tenantResult.reason?.message || 'Failed to load tenants.';
    }

    if (templateResult.status === 'fulfilled') {
      templates = templateResult.value.map((template) => ({
        ...template,
        id: template._id ?? template.id,
        name: template._name || template.name,
        displayName: template._name || template.name || `Template ${template._id ?? template.id}`
      }));
      if (selectedTemplateId) {
        updateTemplateAttributes(selectedTemplateId);
      } else {
        templateAttributes = [];
        templateAttributeNames = [];
      }
    } else {
      error = templateResult.reason?.message || 'Failed to load templates.';
    }

    if (searchResult.status === 'fulfilled') {
      if (!savedSearches.length) {
        searches = searchResult.value;
      }
    } else if (!error) {
      error = searchResult.reason?.message || 'Failed to load searches.';
    }

    const attributesResult = await fetchAttributeMetadata().then(
      (value) => ({ status: 'fulfilled', value }),
      (reason) => ({ status: 'rejected', reason })
    );
    if (attributesResult.status === 'fulfilled') {
      const searchable = attributesResult.value
        .filter((attr) => attr._searchable)
        .map((attr) => ({
          name: attr._alias || attr._name,
          type: attr._type,
          original: attr._name,
          alias: attr._alias
        }))
        .filter((attr) => attr.name);

      searchableAttributes = searchable
        .map(({ name, type }) => ({ name, type }))
        .sort((a, b) => a.name.localeCompare(b.name, 'en', { sensitivity: 'base' }))
        .slice(0, 24);

      const searchableByAttr = new Map();
      searchable.forEach((attr) => {
        searchableByAttr.set(attr.original, attr.type);
        if (attr.alias) {
          searchableByAttr.set(attr.alias, attr.type);
        }
      });
      searchableAttributeMap = searchableByAttr;
      templateAttributes = templateAttributeNames
        .map((attrName) => ({
          name: attrName,
          type: searchableByAttr.get(attrName)
        }))
        .filter((attr) => attr.type);
    }
  });

  $: if (tenants.length && $tenantId !== null && $tenantId !== lastTenantId) {
    lastTenantId = $tenantId;
    offset = 0;
    runSearch(where, 0, pageSize);
  }
</script>

<section>
  <SectionTitle
    title="Unit search"
    hint="Search filters stay on top. Results and details share the workspace below."
  />
  <div class="tenant-bar">
    <div class="tenant-label">Tenants</div>
    {#if tenantError}
      <div class="tenant-error">{tenantError}</div>
    {:else if tenants.length === 0}
      <div class="tenant-empty">No tenants returned.</div>
    {:else}
      <div class="tenant-chips">
        {#each tenants as tenant}
          <span
            class:selected={tenant._tenant_id === $tenantId}
            role="button"
            tabindex="0"
            on:click={() => tenantId.set(tenant._tenant_id)}
            on:keydown={(event) => {
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                tenantId.set(tenant._tenant_id);
              }
            }}
          >
            {tenant._name} · {tenant._tenant_id}
          </span>
        {/each}
      </div>
      <div class="tenant-select">
        <label for="tenant">Active</label>
        <select id="tenant" bind:value={$tenantId}>
          {#each tenants as tenant}
            <option value={tenant._tenant_id}>{tenant._name}</option>
          {/each}
        </select>
      </div>
    {/if}
  </div>
  <SearchFilters
    {templates}
    {searches}
    {searchableAttributes}
    {templateAttributes}
    bind:selectedTemplateId
    bind:where
    bind:selectedSearchId
    bind:orderBy
    bind:orderDirection
    on:run={(event) => runSearch(event.detail.where)}
    on:select={(event) => applySavedSearch(event.detail.search)}
    on:save={saveCurrentSearch}
    on:add-filter={(event) => addFilterComponent(event.detail)}
    on:add-filter-date={(event) => addDateFilterComponent(event.detail)}
    on:add-saved={(event) => addSavedSearchClause(event.detail)}
    on:insert-field={(event) => insertFieldToken(event.detail)}
    on:insert-operator={(event) => insertOperatorToken(event.detail)}
    on:template-change={(event) => updateTemplateAttributes(event.detail.id)}
    on:order-change={(event) => updateOrder(event.detail)}
    on:rename={(event) => renameSavedSearch(event.detail)}
    on:delete={(event) => deleteSavedSearch(event.detail)}
    on:reset={resetSearchForm}
  />
  {#if searchBusy}
    <div class="tenant-empty">Searching…</div>
  {/if}
  {#if queryHint}
    <div class="hint">{queryHint}</div>
  {/if}
  {#if error}
    <div class="error">{error}</div>
  {/if}
  {#if unitError}
    <div class="error">{unitError}</div>
  {/if}
</section>

<div class="layout">
  <SearchResults
    units={units}
    selectedId={selected?.id}
    onSelect={selectUnit}
    page={Math.floor(offset / pageSize) + 1}
    pageSize={pageSize}
    hasNext={units.length === pageSize}
    canPage={!!where.trim()}
    onPrev={goPrevPage}
    onNext={goNextPage}
    onPageSizeChange={changePageSize}
  />
  <UnitDetail unit={selected} />
</div>

<style>
  .layout {
    display: grid;
    grid-template-columns: minmax(260px, 1fr) minmax(320px, 1.4fr);
    gap: 1.6rem;
    --results-max-height: clamp(300px, calc(100vh - 300px), 820px);
  }

  .tenant-bar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.8rem;
    margin: 1rem 0 1.2rem;
    padding: 0.8rem 1rem;
    border-radius: 1rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    background: rgba(255, 255, 255, 0.04);
  }

  .tenant-label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.12rem;
    color: var(--text-muted);
  }

  .tenant-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.6rem;
  }

  .tenant-chips span {
    background: rgba(255, 255, 255, 0.12);
    padding: 0.4rem 0.8rem;
    border-radius: 999px;
    font-size: 0.85rem;
    cursor: pointer;
    transition: background 160ms ease, border-color 160ms ease, color 160ms ease;
    border: 1px solid transparent;
  }

  .tenant-chips span.selected {
    background: rgba(255, 190, 92, 0.18);
    border-color: rgba(255, 190, 92, 0.5);
    color: #ffe4b0;
  }

  .tenant-select {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin-left: auto;
  }

  .tenant-select label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.12rem;
    color: var(--text-muted);
  }

  .tenant-select select {
    background: rgba(8, 10, 18, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-radius: 0.6rem;
    padding: 0.45rem 0.6rem;
    min-width: 160px;
  }

  .tenant-error {
    color: #ffbea8;
  }

  .tenant-empty {
    color: var(--text-muted);
  }

  .error {
    margin-top: 1rem;
    padding: 0.8rem 1rem;
    border-radius: 0.8rem;
    background: rgba(255, 120, 92, 0.12);
    border: 1px solid rgba(255, 120, 92, 0.4);
    color: #ffbea8;
  }

  .hint {
    margin-top: 1rem;
    padding: 0.7rem 1rem;
    border-radius: 0.8rem;
    border: 1px dashed rgba(255, 255, 255, 0.18);
    background: rgba(255, 255, 255, 0.04);
    color: var(--text-muted);
  }

  @media (max-width: 900px) {
    .layout {
      grid-template-columns: 1fr;
    }
  }
</style>
