<script>
  import { onMount } from 'svelte';
  import { snyggifyTime, fetchTenants, fetchAttributeMetadata, fetchRecords, fetchTemplates } from '$lib/api.js';
  import SectionTitle from '$lib/components/SectionTitle.svelte';
  import AdminList from '$lib/components/AdminList.svelte';

  let tenants = [];
  let attributes = [];
  let records = [];
  let templates = [];
  let tenantError = '';
  let catalogError = '';

  const baseName = (value) => {
    if (!value) return '';
    const text = String(value);
    const parts = text.split(':');
    return parts[parts.length - 1];
  };

  const sortByBaseName = (items, selector) =>
    [...items].sort((a, b) => baseName(selector(a)).localeCompare(baseName(selector(b)), 'en', { sensitivity: 'base' }));

  const buildRecordFieldMap = (items) => {
    const map = new Map();
    items.forEach((record) => {
      const name = record?._name;
      const fields = record?._fields || [];
      if (!name || !fields.length) return;
      map.set(name, fields);
      map.set(baseName(name), fields);
    });
    return map;
  };

  const resolveRecordFields = (recordMap, attribute) => {
    if (!recordMap || !attribute) return null;
    return recordMap.get(attribute._name) || recordMap.get(baseName(attribute._name));
  };

  $: recordFieldMap = buildRecordFieldMap(records);

  onMount(async () => {
    const [tenantsResult, attributesResult, recordsResult, templatesResult] = await Promise.allSettled([
      fetchTenants(),
      fetchAttributeMetadata(),
      fetchRecords(),
      fetchTemplates()
    ]);

    if (tenantsResult.status === 'fulfilled') {
      tenants = tenantsResult.value;
    } else {
      tenantError = tenantsResult.reason?.message ?? 'Failed to load tenants';
    }

    if (attributesResult.status === 'fulfilled') {
      attributes = sortByBaseName(attributesResult.value, (item) => item._alias || item._name);
    } else {
      catalogError = attributesResult.reason?.message ?? 'Failed to load attributes';
    }

    if (recordsResult.status === 'fulfilled') {
      records = sortByBaseName(recordsResult.value, (item) => item._name);
    } else if (!catalogError) {
      catalogError = recordsResult.reason?.message ?? 'Failed to load records';
    }

    if (templatesResult.status === 'fulfilled') {
      templates = sortByBaseName(templatesResult.value, (item) => item._name);
    } else if (!catalogError) {
      catalogError = templatesResult.reason?.message ?? 'Failed to load templates';
    }
  });
</script>

<section>
  <SectionTitle title="Schema administration" hint="Attributes, records, and templates are managed separately from browsing and search." />
</section>

<div class="stack">
  {#if tenantError}
    <div class="error">{tenantError}</div>
  {:else if tenants.length === 0}
    <div class="error">No tenants were returned.</div>
  {:else}
    <AdminList
      title="Tenants"
      items={tenants.map((tenant) => ({
        name: tenant._name,
        description: tenant._description || `Tenant ${tenant._tenant_id}`,
        id: `ID ${tenant._tenant_id}`,
        created: snyggifyTime(tenant._created)
      }))}
      fields={['id', 'created']}
      actionLabel="Add tenant"
    />
  {/if}

  {#if catalogError}
    <div class="error">{catalogError}</div>
  {:else}
    <AdminList
      title="Attributes"
      items={attributes.map((item) => {
        const recordFields = item._type === 'RECORD' ? resolveRecordFields(recordFieldMap, item) : null;
        const fieldCount = recordFields ? recordFields.length : 0;
        const alias = item._alias || item._name;
        const qualifiedName = item._qual_name || item._name;
        return {
          name: alias,
          subname: alias !== qualifiedName ? qualifiedName : null,
          prefix: item._namespace_alias ? item._namespace_alias : null,
          description: `${item._type} · ${item._cardinality}${fieldCount ? ` · ${fieldCount} fields` : ''}`,
          searchable: item._searchable ? 'searchable' : 'internal',
          structure: recordFields || []
        };
      })}
      fields={['searchable']}
      actionLabel="Add attribute"
    />

    <AdminList
      title="Records"
      items={records.map((item) => ({
        name: item._name,
        description: (item._fields || []).join(' · '),
        fields: `${(item._fields || []).length} fields`
      }))}
      fields={['fields']}
      actionLabel="Add record"
    />

    <AdminList
      title="Templates"
      items={templates.map((item) => ({
        name: item._name,
        description: (item._attributes || []).join(' · '),
        fields: `${(item._attributes || []).length} attributes`
      }))}
      fields={['fields']}
      actionLabel="Add template"
    />
  {/if}
</div>

<style>
  .stack {
    display: grid;
    gap: 1.4rem;
  }

  .error {
    padding: 0.9rem 1rem;
    border-radius: 0.9rem;
    border: 1px solid rgba(255, 120, 92, 0.4);
    background: rgba(255, 120, 92, 0.12);
    color: #ffbea8;
  }
</style>
