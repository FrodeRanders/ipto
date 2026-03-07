<script>
  import { onMount } from 'svelte';
  import SectionTitle from '$lib/components/SectionTitle.svelte';
  import TreeView from '$lib/components/TreeView.svelte';
  import UnitDetail from '$lib/components/UnitDetail.svelte';
  import { fetchTreeRoots, fetchTreeChildren, fetchUnitById } from '$lib/api.js';
  import { tenantId } from '$lib/stores/tenant.js';

  let tenants = [];
  let roots = [];
  let rootUnits = new Map();
  let selected = null;
  let loading = true;
  let loadError = '';
  let unitError = '';
  let lastTenantId = null;

  const toUnit = (root) => ({
    id: String(root._unit_id ?? 0),
    tenantId: root._tenant_id,
    name: root._unit_name || root._tenant_name,
    status: root._status ? `STATUS_${root._status}` : 'EFFECTIVE',
    created: root._created ?? 'n/a',
    modified: root._modified ?? 'n/a',
    attributes: {}
  });

  const toNode = (root) => ({
    id: String(root._unit_id ?? 0),
    name: root._unit_name || root._tenant_name || `Unit ${root._unit_id ?? 0}`,
    tenantId: root._tenant_id,
    children: [],
    childrenLoaded: false,
    isLoading: false
  });

  const selectNode = async (node) => {
    unitError = '';
    const unitId = String(node.id);
    const fallback = rootUnits.get(unitId);
    try {
      await loadChildren(node);
      const unit = await fetchUnitById(node.tenantId, unitId);
      selected = unit;
    } catch (error) {
      unitError = error?.message ?? 'Failed to load unit details';
      selected = fallback || selected;
    }
  };

  $: activeTenant = tenants.find((tenant) => tenant.id === $tenantId) ?? tenants[0];
  $: activeRoot = roots.find((root) => root.tenantId === $tenantId) ?? roots[0];
  $: if (activeRoot && !activeRoot.childrenLoaded && !loading) {
    loadChildren(activeRoot);
  }
  $: if ($tenantId !== null && $tenantId !== lastTenantId) {
    lastTenantId = $tenantId;
    selected = null;
  }

  const updateTreeNodes = (nodes, predicate, updater) => {
    let changed = false;
    const nextNodes = nodes.map((node) => {
      if (predicate(node)) {
        changed = true;
        return updater(node);
      }
      if (node.children && node.children.length) {
        const nextChildren = updateTreeNodes(node.children, predicate, updater);
        if (nextChildren !== node.children) {
          changed = true;
          return { ...node, children: nextChildren };
        }
      }
      return node;
    });
    return changed ? nextNodes : nodes;
  };

  const loadChildren = async (node) => {
    if (!node || node.childrenLoaded || node.isLoading) return;
    roots = updateTreeNodes(
      roots,
      (candidate) => candidate.id === node.id && candidate.tenantId === node.tenantId,
      (candidate) => ({ ...candidate, isLoading: true })
    );
    try {
      const children = await fetchTreeChildren(node.tenantId, node.id);
      const nextChildren = children.map((unit) => ({
        id: unit.id,
        name: unit.name,
        tenantId: unit.tenantId,
        children: [],
        childrenLoaded: false,
        isLoading: false
      }));
      roots = updateTreeNodes(
        roots,
        (candidate) => candidate.id === node.id && candidate.tenantId === node.tenantId,
        (candidate) => ({
          ...candidate,
          children: nextChildren,
          childrenLoaded: true,
          isLoading: false
        })
      );
      children.forEach((unit) => {
        if (!rootUnits.has(unit.id)) {
          rootUnits.set(unit.id, unit);
        }
      });
    } catch (error) {
      unitError = error?.message ?? 'Failed to load tree children';
      roots = updateTreeNodes(
        roots,
        (candidate) => candidate.id === node.id && candidate.tenantId === node.tenantId,
        (candidate) => ({ ...candidate, childrenLoaded: true, isLoading: false })
      );
    } finally {
      roots = updateTreeNodes(
        roots,
        (candidate) => candidate.id === node.id && candidate.tenantId === node.tenantId,
        (candidate) => ({ ...candidate, isLoading: false })
      );
    }
  };

  onMount(async () => {
    try {
      const payload = await fetchTreeRoots();
      roots = payload.map(toNode);
      tenants = payload.map((root) => ({
        id: root._tenant_id,
        name: root._tenant_name,
        description: root._tenant_description,
        units: root._unit_count ?? 0
      }));
      rootUnits = new Map(payload.map((root) => [String(root._unit_id ?? 0), toUnit(root)]));
      if ($tenantId === null || !tenants.some((tenant) => tenant.id === $tenantId)) {
        tenantId.set(tenants[0]?.id ?? null);
      }
      selected = null;
      if (roots.length > 0) {
        await loadChildren(roots[0]);
      }
    } catch (error) {
      loadError = error?.message ?? 'Failed to load tree roots';
    } finally {
      loading = false;
    }
  });
</script>

<section>
  <SectionTitle
    title="Unit Trees"
    hint="Browse parent-child relationships per tenant. Tree navigation and unit detail stay side-by-side."
  />
</section>

<div class="toolbar">
  {#if loading}
    <div class="info">
      <div class="label">Loading</div>
      <div class="value">Fetching tenant tree roots...</div>
    </div>
  {:else if loadError}
    <div class="info error">
      <div class="label">Error</div>
      <div class="value">{loadError}</div>
    </div>
  {:else if tenants.length === 0}
    <div class="info error">
      <div class="label">Missing</div>
      <div class="value">No tenants were returned.</div>
    </div>
  {:else}
    <div class="field">
      <label>Tenant</label>
      <select bind:value={$tenantId}>
        {#each tenants as tenant}
          <option value={tenant.id}>{tenant.name}</option>
        {/each}
      </select>
    </div>
    <div class="info">
      <div class="label">Scope</div>
    <div class="value">Tenant {activeTenant.name}&nbsp;&bull;&nbsp;{activeTenant.units} units</div>
    {#if activeTenant.description}
      <div class="note">{activeTenant.description}</div>
    {/if}
    </div>
  {/if}
</div>

<div class="layout">
  <div class="tree panel">
    <div class="tree-header">
      <h3>{activeRoot?.name ?? 'Unit tree'}</h3>
      <span>{activeRoot?.children?.length ?? 0} top-level nodes</span>
    </div>
    <div class="tree-scroll">
      {#if activeRoot}
        <TreeView node={activeRoot} selectedId={selected?.id} onSelect={selectNode} onToggle={loadChildren} />
      {:else}
        <div class="empty-tree">No roots loaded yet.</div>
      {/if}
    </div>
  </div>
  <div class="detail">
    {#if unitError}
      <div class="detail-error">{unitError}</div>
    {/if}
    <UnitDetail unit={selected} />
  </div>
</div>

<style>
  .toolbar {
    display: flex;
    flex-wrap: wrap;
    gap: 1.2rem;
    align-items: center;
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
    min-width: 200px;
  }

  .info {
    padding: 0.8rem 1rem;
    border-radius: 0.9rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    background: rgba(255, 255, 255, 0.04);
  }

  .info.error {
    border-color: rgba(255, 124, 92, 0.4);
    color: #ffb2a3;
  }

  .label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    color: var(--text-muted);
  }

  .value {
    font-weight: 600;
  }

  .note {
    font-size: 0.85rem;
    color: var(--text-muted);
    margin-top: 0.3rem;
  }

  .layout {
    display: grid;
    grid-template-columns: minmax(240px, 1fr) minmax(320px, 2fr);
    gap: 1.6rem;
  }

  .tree-scroll {
    max-height: clamp(300px, calc(100vh - 260px), 820px);
    overflow-y: auto;
    padding-right: 0.2rem;
    scrollbar-gutter: stable;
  }

  .tree-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
  }

  .tree-header h3 {
    margin: 0;
    font-family: 'Space Grotesk', sans-serif;
  }

  .tree-header span {
    color: var(--text-muted);
  }

  .empty-tree {
    color: var(--text-muted);
    font-size: 0.95rem;
    padding: 1rem 0.6rem;
  }

  .detail-error {
    margin-bottom: 0.9rem;
    padding: 0.7rem 0.9rem;
    border-radius: 0.8rem;
    border: 1px solid rgba(255, 120, 92, 0.4);
    background: rgba(255, 120, 92, 0.12);
    color: #ffbea8;
  }

  @media (max-width: 900px) {
    .layout {
      grid-template-columns: 1fr;
    }
  }
</style>
