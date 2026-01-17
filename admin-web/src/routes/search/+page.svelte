<script>
  import { onMount } from 'svelte';
  import { fetchTenants, fetchTemplates, fetchSearches, searchUnits, fetchUnitById } from '$lib/api.js';
  import SectionTitle from '$lib/components/SectionTitle.svelte';
  import SearchFilters from '$lib/components/SearchFilters.svelte';
  import SearchResults from '$lib/components/SearchResults.svelte';
  import UnitDetail from '$lib/components/UnitDetail.svelte';
  import { tenantId } from '$lib/stores/tenant.js';

  let units = [];
  let selected = null;
  let error = '';
  let tenantError = '';
  let tenants = [];
  let templates = [];
  let searches = [];
  let unitError = '';
  let where = '';
  let searchBusy = false;
  let lastTenantId = null;

  const isNumericId = (value) => /^\d+$/.test(String(value));

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

  const runSearch = async (expression = where) => {
    if ($tenantId === null) {
      return;
    }
    try {
      searchBusy = true;
      error = '';
      units = await searchUnits({
        tenantId: $tenantId,
        where: expression,
        offset: 0,
        size: 30
      });
      selected = units[0] || null;
    } catch (err) {
      error = err?.message || 'Failed to load units.';
    } finally {
      searchBusy = false;
    }
  };

  onMount(async () => {
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
      templates = templateResult.value;
    } else {
      error = templateResult.reason?.message || 'Failed to load templates.';
    }

    if (searchResult.status === 'fulfilled') {
      searches = searchResult.value;
    } else if (!error) {
      error = searchResult.reason?.message || 'Failed to load searches.';
    }
  });

  $: if (tenants.length && $tenantId !== null && $tenantId !== lastTenantId) {
    lastTenantId = $tenantId;
    runSearch(where);
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
          <span>{tenant._name} · {tenant._tenant_id}</span>
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
  <SearchFilters {templates} {searches} bind:where on:run={(event) => runSearch(event.detail.where)} />
  {#if searchBusy}
    <div class="tenant-empty">Searching…</div>
  {/if}
  {#if error}
    <div class="error">{error}</div>
  {/if}
  {#if unitError}
    <div class="error">{unitError}</div>
  {/if}
</section>

<div class="layout">
  <SearchResults units={units} selectedId={selected?.id} onSelect={selectUnit} />
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

  @media (max-width: 900px) {
    .layout {
      grid-template-columns: 1fr;
    }
  }
</style>
