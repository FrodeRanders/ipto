<script>
  import { onMount, tick } from 'svelte';
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { toStore } from 'svelte/store';
  import SectionTitle from '#lib/components/SectionTitle.svelte';
  import { fetchAttributeMetadata, fetchRecordTemplate, updateRecordTemplate } from '#lib/api.js';

  const routeId = toStore(() => page.params.id);

  let recordId;
  let recordName = '';
  let recordFields = [];
  let attributes = [];
  let selectedAttrId = '';
  let fieldAlias = '';
  let error = '';
  let loading = false;
  let recordErrors = {};
  let dragIndex = null;
  let dragOverIndex = null;
  let dragOverPosition = 'before';

  const normalizeValue = (value) => String(value ?? '').trim();
  const baseName = (value) => {
    if (!value) return '';
    const text = String(value);
    const parts = text.split(':');
    return parts[parts.length - 1];
  };
  const normalizeKey = (value) => normalizeValue(value).toLowerCase();
  const reservedFieldNames = new Set([
    'unitid',
    'unitver',
    'unitname',
    'status',
    'created',
    'modified',
    'corrid'
  ]);

  $: recordId = Number($routeId);

  const addField = () => {
    recordErrors = {};
    if (!selectedAttrId) {
      recordErrors.fields = 'Select an attribute to add.';
      return;
    }
    const id = Number(selectedAttrId);
    const target = attributes.find((attr) => attr._id === id);
    if (!target) {
      recordErrors.fields = 'Selected attribute was not found.';
      return;
    }
    if (recordFields.some((field) => field.attrId === id)) {
      recordErrors.fields = 'That attribute is already added.';
      return;
    }
    recordFields = [
      ...recordFields,
      {
        attrId: id,
        name: target._alias || target._name,
        alias: normalizeValue(fieldAlias) || baseName(target._alias || target._name)
      }
    ];
    selectedAttrId = '';
    fieldAlias = '';
  };

  const removeField = (attrId) => {
    recordFields = recordFields.filter((field) => field.attrId !== attrId);
  };

  const updateFieldAlias = (index, value) => {
    recordFields = recordFields.map((field, idx) =>
      idx === index ? { ...field, alias: normalizeValue(value) } : field
    );
  };

  const effectiveAlias = (field) => normalizeValue(field.alias) || baseName(field.name);

  const getFieldAliasIssue = () => {
    if (!recordFields.length) {
      return '';
    }
    const seen = new Set();
    for (const field of recordFields) {
      const alias = normalizeKey(effectiveAlias(field));
      if (!alias) {
        return 'Each field needs an alias or a usable name.';
      }
      if (reservedFieldNames.has(alias)) {
        return `Alias "${alias}" is reserved.`;
      }
      if (seen.has(alias)) {
        return `Alias "${alias}" is duplicated.`;
      }
      seen.add(alias);
    }
    return '';
  };

  $: fieldAliasIssue = getFieldAliasIssue();

  const validateRecordFields = () => {
    const errors = {};
    if (!recordFields.length) {
      errors.fields = 'Add at least one field to the record.';
      return errors;
    }
    const seen = new Set();
    for (const field of recordFields) {
      const alias = normalizeKey(effectiveAlias(field));
      if (!alias) {
        errors.fields = 'Each field needs an alias or a usable name.';
        break;
      }
      if (reservedFieldNames.has(alias)) {
        errors.fields = `Alias "${alias}" is reserved.`;
        break;
      }
      if (seen.has(alias)) {
        errors.fields = `Alias "${alias}" is duplicated.`;
        break;
      }
      seen.add(alias);
    }
    return errors;
  };

  const handleDragStart = (index) => {
    dragIndex = index;
  };

  const handleDragOver = (event, index) => {
    event.preventDefault();
    const target = event.currentTarget;
    if (!target) return;
    const rect = target.getBoundingClientRect();
    const midpoint = rect.top + rect.height / 2;
    dragOverPosition = event.clientY >= midpoint ? 'after' : 'before';
    dragOverIndex = index;
  };

  const handleDragEnter = (index) => {
    dragOverIndex = index;
  };

  const handleDrop = (index) => {
    if (dragIndex === null || dragIndex === index) {
      dragIndex = null;
      dragOverIndex = null;
      dragOverPosition = 'before';
      return;
    }
    const next = [...recordFields];
    const [moved] = next.splice(dragIndex, 1);
    let insertIndex = dragOverPosition === 'after' ? index + 1 : index;
    if (dragIndex < insertIndex) {
      insertIndex -= 1;
    }
    next.splice(insertIndex, 0, moved);
    recordFields = next;
    dragIndex = null;
    dragOverIndex = null;
    dragOverPosition = 'before';
  };

  const handleDragEnd = () => {
    dragIndex = null;
    dragOverIndex = null;
    dragOverPosition = 'before';
  };

  const moveFieldByIndex = (from, to) => {
    if (to < 0 || to >= recordFields.length || from === to) {
      return;
    }
    const next = [...recordFields];
    const [moved] = next.splice(from, 1);
    next.splice(to, 0, moved);
    recordFields = next;
  };

  const handleRowKeydown = async (event, index) => {
    if (!(event.altKey || event.ctrlKey)) {
      return;
    }
    const handle = event.currentTarget;
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      moveFieldByIndex(index, index - 1);
      await tick();
      handle.focus();
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      moveFieldByIndex(index, index + 1);
      await tick();
      handle.focus();
    }
  };

  const handleSave = async () => {
    error = '';
    recordErrors = validateRecordFields();

    const name = normalizeValue(recordName);
    if (!name) {
      recordErrors.recordName = 'Record type name is required.';
    }

    if (Object.keys(recordErrors).length) {
      return;
    }

    loading = true;
    try {
      await updateRecordTemplate({
        recordId,
        name,
        fields: recordFields.map((field) => ({
          attrId: field.attrId,
          alias: effectiveAlias(field)
        }))
      });
      await goto('/admin');
    } catch (err) {
      error = err?.message || 'Failed to update record template.';
    } finally {
      loading = false;
    }
  };

  onMount(async () => {
    error = '';
    if (!recordId || Number.isNaN(recordId)) {
      error = 'Record id is missing.';
      return;
    }

    try {
      const [attrs, record] = await Promise.all([
        fetchAttributeMetadata(),
        fetchRecordTemplate(recordId)
      ]);
      attributes = [...attrs].sort((a, b) => (a._alias || a._name).localeCompare(b._alias || b._name, 'en', { sensitivity: 'base' }));
      recordName = record._name || '';
      recordFields = (record._fields || []).map((field) => ({
        attrId: field.attrId,
        name: field.name,
        alias: field.alias || ''
      }));
    } catch (err) {
      error = err?.message || 'Failed to load record template.';
    }
  });
</script>

<section>
  <SectionTitle
    title="Edit record fields"
    hint="Update the record type name and reorder or adjust its fields."
  />
</section>

<div class="panel">
  {#if error}
    <div class="error">{error}</div>
  {/if}

  <label>
    Record type name
    <input
      type="text"
      placeholder="RecordType"
      bind:value={recordName}
      aria-invalid={recordErrors.recordName ? 'true' : 'false'}
    />
    {#if recordErrors.recordName}
      <span class="field-error">{recordErrors.recordName}</span>
    {/if}
  </label>

  <div class="record-controls">
      <select aria-label="Attribute to add" bind:value={selectedAttrId}>
        <option value="">Select attribute</option>
        {#each attributes as attr}
          {#if attr._id !== recordId}
            <option value={attr._id}>{attr._alias || attr._name}</option>
          {/if}
        {/each}
      </select>
      <input
        type="text"
        placeholder="Field alias (optional)"
        aria-label="Field alias (optional)"
        bind:value={fieldAlias}
      />
      <button type="button" on:click={addField}>Add field</button>
    </div>
    {#if recordErrors.fields}
      <div class="field-error">{recordErrors.fields}</div>
    {:else if fieldAliasIssue}
      <div class="field-error">{fieldAliasIssue}</div>
    {/if}
    <div class="field-list" role="list" aria-label="Record fields">
      {#if recordFields.length === 0}
        <div class="muted">No fields added yet.</div>
      {:else}
        {#each recordFields as field, index (field.attrId)}
          <div
            class="field-row"
            role="listitem"
            class:drag-over={index === dragOverIndex}
            class:drag-over-before={index === dragOverIndex && dragOverPosition === 'before'}
            class:drag-over-after={index === dragOverIndex && dragOverPosition === 'after'}
            on:dragover={(event) => handleDragOver(event, index)}
            on:dragenter={() => handleDragEnter(index)}
            on:drop={() => handleDrop(index)}
          >
            <button
              type="button"
              class="drag-handle"
              draggable="true"
              on:dragstart={() => handleDragStart(index)}
              on:dragend={handleDragEnd}
              aria-label={`Reorder ${field.name}`}
              on:keydown={(event) => handleRowKeydown(event, index)}
              title="Drag or use Alt+Up/Down to reorder"
            >
              ::
            </button>
            <div class="field-name">{field.name}</div>
            <input
              type="text"
              value={field.alias}
              placeholder="Alias"
              aria-label={`Alias for ${field.name}`}
              on:input={(event) => updateFieldAlias(index, event.target.value)}
            />
            <div class="field-actions">
              <button type="button" class="ghost" disabled={index === 0}
                aria-label={`Move ${field.name} up`} on:click={() => moveFieldByIndex(index, index - 1)}>↑</button>
              <button type="button" class="ghost" disabled={index === recordFields.length - 1}
                aria-label={`Move ${field.name} down`} on:click={() => moveFieldByIndex(index, index + 1)}>↓</button>
              <button type="button" class="ghost" on:click={() => removeField(field.attrId)}>Remove</button>
            </div>
          </div>
        {/each}
      {/if}
    </div>

  <div class="actions">
    <button type="button" class="ghost" on:click={() => goto('/admin')}>Cancel</button>
    <button type="button" class="primary" on:click={handleSave} disabled={loading}>
      {loading ? 'Saving…' : 'Save changes'}
    </button>
  </div>
</div>

<style>
  .panel {
    background: rgba(8, 10, 18, 0.75);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 1.2rem;
    padding: 1.4rem;
    display: grid;
    gap: 1.2rem;
  }

  label {
    display: grid;
    gap: 0.4rem;
    font-size: 0.85rem;
    color: var(--text-muted);
  }

  input,
  select {
    background: rgba(8, 10, 18, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-radius: 0.6rem;
    padding: 0.5rem 0.6rem;
    font-size: 0.95rem;
  }

  .record-controls {
    display: grid;
    grid-template-columns: 2fr 1.5fr auto;
    gap: 0.8rem;
    align-items: center;
  }

  .field-list {
    display: grid;
    gap: 0.6rem;
  }

  .field-row {
    display: grid;
    grid-template-columns: auto 1fr 1.5fr auto;
    gap: 0.8rem;
    align-items: center;
    padding: 0.6rem 0.8rem;
    border-radius: 0.8rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    background: rgba(255, 255, 255, 0.03);
    position: relative;
  }

  .field-row.drag-over {
    border-color: rgba(255, 190, 92, 0.6);
    box-shadow: inset 0 0 0 1px rgba(255, 190, 92, 0.45);
    background: rgba(255, 190, 92, 0.08);
  }

  .field-name {
    font-weight: 600;
    color: var(--text);
  }

  .field-actions {
    display: flex;
    gap: 0.4rem;
    flex-wrap: wrap;
  }

  .drag-handle {
    border: 0;
    font: inherit;
    font-weight: 600;
    color: var(--text-muted);
    cursor: grab;
    user-select: none;
    padding: 0.2rem 0.4rem;
    border-radius: 0.4rem;
    background: rgba(255, 255, 255, 0.08);
  }

  .drag-handle:active {
    cursor: grabbing;
  }

  .field-row:focus-within {
    outline: 2px solid rgba(255, 190, 92, 0.7);
    outline-offset: 2px;
  }

  .field-row.drag-over-before::before,
  .field-row.drag-over-after::after {
    content: '';
    height: 2px;
    background: rgba(255, 190, 92, 0.85);
    border-radius: 999px;
    position: absolute;
    left: 0.6rem;
    right: 0.6rem;
  }

  .field-row.drag-over-before::before {
    top: 0;
  }

  .field-row.drag-over-after::after {
    bottom: 0;
  }

  .actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.6rem;
  }

  .actions button,
  .record-controls button {
    border: none;
    border-radius: 999px;
    padding: 0.5rem 1rem;
    background: rgba(255, 255, 255, 0.1);
    color: var(--text);
    cursor: pointer;
  }

  .actions button:disabled {
    cursor: not-allowed;
    opacity: 0.6;
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

  .error {
    padding: 0.8rem 1rem;
    border-radius: 0.9rem;
    border: 1px solid rgba(255, 120, 92, 0.4);
    background: rgba(255, 120, 92, 0.12);
    color: #ffbea8;
  }

  .field-error {
    color: #ffbea8;
    font-size: 0.8rem;
  }

  .muted {
    color: var(--text-muted);
    font-size: 0.9rem;
  }

  @media (max-width: 900px) {
    .record-controls {
      grid-template-columns: 1fr;
    }

    .field-row {
      grid-template-columns: 1fr;
      justify-items: start;
    }

    .actions {
      justify-content: flex-start;
    }
  }
</style>
