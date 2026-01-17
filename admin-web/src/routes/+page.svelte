<script>
  import { onMount } from 'svelte';
  import { fetchTenants, fetchAttributeMetadata, fetchRecords, fetchTemplates } from '$lib/api.js';
  import StatCard from '$lib/components/StatCard.svelte';
  import SectionTitle from '$lib/components/SectionTitle.svelte';
  import { tenantId } from '$lib/stores/tenant.js';

  let tenants = [];
  let attributes = [];
  let records = [];
  let templates = [];
  let tenantError = '';
  let catalogError = '';

  onMount(async () => {
    const [tenantsResult, attributesResult, recordsResult, templatesResult] = await Promise.allSettled([
      fetchTenants(),
      fetchAttributeMetadata(),
      fetchRecords(),
      fetchTemplates()
    ]);

    if (tenantsResult.status === 'fulfilled') {
      tenants = tenantsResult.value;
      if ($tenantId === null || !tenants.some((tenant) => tenant._tenant_id === $tenantId)) {
        tenantId.set(tenants[0]?._tenant_id ?? null);
      }
    } else {
      tenantError = tenantsResult.reason?.message ?? 'Failed to load tenants';
    }

    if (attributesResult.status === 'fulfilled') {
      attributes = attributesResult.value;
    } else {
      catalogError = attributesResult.reason?.message ?? 'Failed to load attributes';
    }

    if (recordsResult.status === 'fulfilled') {
      records = recordsResult.value;
    } else if (!catalogError) {
      catalogError = recordsResult.reason?.message ?? 'Failed to load records';
    }

    if (templatesResult.status === 'fulfilled') {
      templates = templatesResult.value;
    } else if (!catalogError) {
      catalogError = templatesResult.reason?.message ?? 'Failed to load templates';
    }
  });
</script>

<section>
  <SectionTitle title="IPTO Admin" hint="High-level system status and navigation shortcuts." />
  <div class="stats">
    <StatCard
      label="Tenants"
      value={tenantError ? '—' : tenants.length}
      detail={tenantError ? tenantError : 'Active spaces'}
    />
    <StatCard
      label="Attributes"
      value={catalogError ? '—' : attributes.length}
      detail={catalogError ? catalogError : 'Defined fields'}
    />
    <StatCard
      label="Records"
      value={catalogError ? '—' : records.length}
      detail={catalogError ? catalogError : 'Structured bundles'}
    />
    <StatCard
      label="Templates"
      value={catalogError ? '—' : templates.length}
      detail={catalogError ? catalogError : 'Search bundles'}
    />
  </div>
</section>

<section class="panel">
  <SectionTitle title="Tenants" hint="Active tenant spaces and descriptions." />
  {#if tenantError}
    <div class="error">{tenantError}</div>
  {:else if tenants.length === 0}
    <div class="empty">No tenants were returned.</div>
  {:else}
    <div class="tenant-grid">
      {#each tenants as tenant}
        <div
          class="tenant-card"
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
          <div class="tenant-name">{tenant._name}</div>
          <div class="tenant-meta">ID {tenant._tenant_id}</div>
          {#if tenant.description}
            <div class="tenant-desc">{tenant._description}</div>
          {/if}
        </div>
      {/each}
    </div>
  {/if}
</section>

<section class="panel">
  <SectionTitle title="Operational focus" hint="Split by admin tasks, browsing, and search." />
  <div class="grid">
    <div class="card">
      <h3>Administration</h3>
      <p>Define attributes, record shapes, and unit templates for search.</p>
      <a href="/admin">Go to schema admin →</a>
    </div>
    <div class="card">
      <h3>Tree browsing</h3>
      <p>Navigate parent-child relations in a tenant scoped tree.</p>
      <a href="/trees">Browse unit trees →</a>
    </div>
    <div class="card">
      <h3>Search</h3>
      <p>Use templates or ad-hoc attribute filters to locate units.</p>
      <a href="/search">Run searches →</a>
    </div>
  </div>
</section>

<style>
  .stats {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem;
  }

  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 1rem;
  }

  .card {
    background: rgba(255, 255, 255, 0.04);
    border-radius: 1rem;
    padding: 1.2rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }

  .card h3 {
    margin: 0;
    font-family: 'Space Grotesk', sans-serif;
  }

  .card p {
    color: var(--text-muted);
    margin: 0;
  }

  .card a {
    margin-top: auto;
    text-decoration: none;
    color: var(--accent);
  }

  .tenant-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 1rem;
  }

  .tenant-card {
    border-radius: 1rem;
    padding: 1rem;
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.08);
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    transition: border-color 160ms ease, background 160ms ease, transform 160ms ease;
  }

  .tenant-card.selected {
    border-color: rgba(255, 190, 92, 0.5);
    background: rgba(255, 190, 92, 0.12);
    transform: translateY(-2px);
  }

  .tenant-name {
    font-weight: 600;
    font-family: 'Space Grotesk', sans-serif;
  }

  .tenant-meta {
    color: var(--text-muted);
    font-size: 0.85rem;
  }

  .tenant-desc {
    color: var(--text-muted);
    font-size: 0.9rem;
  }

  .empty,
  .error {
    padding: 0.9rem 1rem;
    border-radius: 0.9rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    background: rgba(255, 255, 255, 0.04);
    color: var(--text-muted);
  }

  .error {
    border-color: rgba(255, 120, 92, 0.4);
    background: rgba(255, 120, 92, 0.12);
    color: #ffbea8;
  }
</style>
