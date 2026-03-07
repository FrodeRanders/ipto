<script>
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import SectionTitle from '$lib/components/SectionTitle.svelte';
  import { createAttribute, createRecordTemplate, fetchAttributeMetadata, updateAttributeDescriptions } from '$lib/api.js';

  const attributeTypes = [
    'STRING',
    'TIME',
    'INTEGER',
    'LONG',
    'DOUBLE',
    'BOOLEAN',
    'DATA',
    'RECORD'
  ];

  let step = 1;
  let creating = false;
  let error = '';
  let attributesError = '';
  let attributes = [];
  let createdAttribute = null;
  let formErrors = {};
  let recordErrors = {};
  let recordFields = [];
  let selectedAttrId = '';
  let fieldAlias = '';
  let form = {
    name: '',
    qualifiedName: '',
    alias: '',
    type: 'STRING',
    isArray: false,
    recordName: ''
  };
  let descriptionItems = [
    { lang: 'SE', alias: '', description: '' }
  ];
  let descriptionError = '';

  const ISO_LANG_RE = /^[A-Za-z]{2}$/;
  const ISO_LANG_OPTIONS = [
    'SE',
    'EN',
    'NO',
    'DA',
    'DE',
    'FR',
    'ES',
    'IT',
    'NL',
    'FI',
    'IS',
    'PL',
    'PT'
  ];
  let dragIndex = null;
  let dragOverIndex = null;
  let dragOverPosition = 'before';

  const baseName = (value) => {
    if (!value) return '';
    const text = String(value);
    const parts = text.split(':');
    return parts[parts.length - 1];
  };

  const normalizeValue = (value) => String(value ?? '').trim();
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

  const collectExistingAttributeKeys = (items) => {
    const keys = new Set();
    items.forEach((item) => {
      [item?._name, item?._alias, item?._qual_name].forEach((entry) => {
        const key = normalizeKey(entry);
        if (key) {
          keys.add(key);
        }
      });
    });
    return keys;
  };

  const buildFormErrors = () => {
    const errors = {};
    const existingKeys = collectExistingAttributeKeys(attributes);
    const name = normalizeValue(form.name);
    const qualifiedName = normalizeValue(form.qualifiedName);
    const alias = normalizeValue(form.alias);

    if (!name) {
      errors.name = 'Attribute name is required.';
    } else if (existingKeys.has(normalizeKey(name))) {
      errors.name = 'Attribute name already exists.';
    }

    if (!qualifiedName) {
      errors.qualifiedName = 'Qualified name is required.';
    } else if (existingKeys.has(normalizeKey(qualifiedName))) {
      errors.qualifiedName = 'Qualified name already exists.';
    }

    if (alias) {
      if (existingKeys.has(normalizeKey(alias))) {
        errors.alias = 'Alias already exists.';
      } else if (normalizeKey(alias) === normalizeKey(name) || normalizeKey(alias) === normalizeKey(qualifiedName)) {
        errors.alias = 'Alias must be distinct from name and qualified name.';
      }
    }

    if (name && qualifiedName && normalizeKey(name) === normalizeKey(qualifiedName)) {
      errors.qualifiedName = 'Qualified name must be distinct from attribute name.';
    }

    if (!attributeTypes.includes(form.type)) {
      errors.type = 'Select a valid attribute type.';
    }

    return errors;
  };

  const handleCreate = async () => {
    error = '';
    descriptionError = '';
    if (form.type === 'RECORD' && !normalizeValue(form.recordName)) {
      form.recordName = baseName(form.name);
    }
    formErrors = buildFormErrors();
    if (Object.keys(formErrors).length) {
      return;
    }

    creating = true;
    try {
      const payload = {
        alias: normalizeValue(form.alias) || null,
        attributeName: normalizeValue(form.name),
        qualifiedName: normalizeValue(form.qualifiedName),
        type: form.type,
        isArray: form.isArray
      };
      createdAttribute = await createAttribute(payload);
      const cleanedDescriptions = descriptionItems
        .map((item) => ({
          lang: normalizeValue(item.lang) || 'SE',
          alias: normalizeValue(item.alias) || normalizeValue(form.name),
          description: normalizeValue(item.description)
        }))
        .filter((item) => item.description);
      if (cleanedDescriptions.length) {
        const seen = new Set();
        for (const entry of cleanedDescriptions) {
          const lang = entry.lang.toUpperCase();
          if (!ISO_LANG_RE.test(lang)) {
            descriptionError = `Language "${entry.lang}" must be a two-letter ISO code.`;
            return;
          }
          if (seen.has(lang)) {
            descriptionError = `Language "${entry.lang}" is duplicated.`;
            return;
          }
          seen.add(lang);
        }
      }
      if (cleanedDescriptions.length) {
        await updateAttributeDescriptions(createdAttribute._id, cleanedDescriptions);
      }
      if (form.type === 'RECORD') {
        step = 2;
        if (!form.recordName) {
          form.recordName = baseName(form.name);
        }
      } else {
        await goto('/admin');
      }
    } catch (err) {
      error = err?.message || 'Failed to create attribute.';
    } finally {
      creating = false;
    }
  };

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

  const handleRowKeydown = (event, index) => {
    if (!(event.altKey || event.ctrlKey)) {
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      moveFieldByIndex(index, index - 1);
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      moveFieldByIndex(index, index + 1);
    }
  };

  const handleRecordSubmit = async () => {
    error = '';
    recordErrors = validateRecordFields();

    if (Object.keys(recordErrors).length) {
      return;
    }

    creating = true;
    try {
      const recordName = normalizeValue(form.recordName);
      if (!recordName) {
        recordErrors = { recordName: 'Record type name is required.' };
        creating = false;
        return;
      }
      await createRecordTemplate({
        recordId: createdAttribute._id,
        name: recordName,
        fields: recordFields.map((field) => ({
          attrId: field.attrId,
          alias: effectiveAlias(field)
        }))
      });
      await goto('/admin');
    } catch (err) {
      error = err?.message || 'Failed to create record template.';
    } finally {
      creating = false;
    }
  };

  onMount(async () => {
    try {
      const data = await fetchAttributeMetadata();
      attributes = [...data].sort((a, b) => (a._alias || a._name).localeCompare(b._alias || b._name, 'en', { sensitivity: 'base' }));
    } catch (err) {
      attributesError = err?.message || 'Failed to load attributes.';
    }
  });

  const addDescriptionItem = () => {
    descriptionItems = [...descriptionItems, { lang: 'SE', alias: '', description: '' }];
  };

  const removeDescriptionItem = (index) => {
    if (descriptionItems.length === 1) {
      descriptionItems = [{ lang: 'SE', alias: '', description: '' }];
      return;
    }
    descriptionItems = descriptionItems.filter((_, idx) => idx !== index);
  };

  const updateDescriptionItem = (index, key, value) => {
    descriptionItems = descriptionItems.map((item, idx) =>
      idx === index ? { ...item, [key]: value } : item
    );
  };
</script>

<section>
  <SectionTitle
    title="Add attribute"
    hint="Create a new attribute, then optionally configure record fields."
  />
</section>

<div class="wizard">
  <div class="steps">
    <div class:active={step === 1}>1 · Attribute details</div>
    <div class:active={step === 2}>2 · Record fields</div>
  </div>

  {#if error}
    <div class="error">{error}</div>
  {/if}

  {#if step === 1}
    <div class="panel">
      <div class="field-grid">
        <label>
          Attribute name
          <input
            type="text"
            placeholder="prefix:localName"
            bind:value={form.name}
            aria-invalid={formErrors.name ? 'true' : 'false'}
          />
          {#if formErrors.name}
            <span class="field-error">{formErrors.name}</span>
          {/if}
        </label>
        <label>
          Qualified name
          <input
            type="text"
            placeholder="https://example.com/ns#localName"
            bind:value={form.qualifiedName}
            aria-invalid={formErrors.qualifiedName ? 'true' : 'false'}
          />
          {#if formErrors.qualifiedName}
            <span class="field-error">{formErrors.qualifiedName}</span>
          {/if}
        </label>
        <label>
          Alias (optional)
          <input
            type="text"
            placeholder="short-name"
            bind:value={form.alias}
            aria-invalid={formErrors.alias ? 'true' : 'false'}
          />
          {#if formErrors.alias}
            <span class="field-error">{formErrors.alias}</span>
          {/if}
        </label>
        <label>
          Type
          <select bind:value={form.type} aria-invalid={formErrors.type ? 'true' : 'false'}>
            {#each attributeTypes as type}
              <option value={type}>{type}</option>
            {/each}
          </select>
          {#if formErrors.type}
            <span class="field-error">{formErrors.type}</span>
          {/if}
        </label>
        <label class="checkbox">
          <input type="checkbox" bind:checked={form.isArray} />
          Cardinality: vector (unchecked = scalar)
        </label>
        <label class="record-name" class:disabled={form.type !== 'RECORD'}>
          Record type name
          <input
            type="text"
            placeholder="RecordType"
            bind:value={form.recordName}
            disabled={form.type !== 'RECORD'}
            aria-invalid={formErrors.recordName ? 'true' : 'false'}
          />
          {#if formErrors.recordName}
            <span class="field-error">{formErrors.recordName}</span>
          {/if}
        </label>
      </div>
      <div class="description-block">
        <div class="description-header">
          <div>
            <h4>Attribute descriptions</h4>
            <p>Optional multilingual labels and descriptions.</p>
          </div>
          <button type="button" class="ghost" on:click={addDescriptionItem}>Add language</button>
        </div>
        {#if descriptionError}
          <div class="field-error">{descriptionError}</div>
        {/if}
        {#each descriptionItems as item, index}
          <div class="description-row">
            <input
              type="text"
              placeholder="Lang (e.g. SE)"
              list="lang-options"
              value={item.lang}
              on:input={(event) => updateDescriptionItem(index, 'lang', event.target.value)}
            />
            <input
              type="text"
              placeholder="Alias (defaults to name)"
              value={item.alias}
              on:input={(event) => updateDescriptionItem(index, 'alias', event.target.value)}
            />
            <textarea
              rows="2"
              placeholder="Description"
              on:input={(event) => updateDescriptionItem(index, 'description', event.target.value)}
            >{item.description}</textarea>
            <button type="button" class="ghost" on:click={() => removeDescriptionItem(index)}>Remove</button>
          </div>
        {/each}
        <datalist id="lang-options">
          {#each ISO_LANG_OPTIONS as lang}
            <option value={lang} />
          {/each}
        </datalist>
      </div>
      <div class="actions">
        <button type="button" class="ghost" on:click={() => goto('/admin')}>Cancel</button>
        <button type="button" class="primary" on:click={handleCreate} disabled={creating}>
          {creating ? 'Creating…' : 'Create attribute'}
        </button>
      </div>
    </div>
  {:else}
    <div class="panel">
      <div class="record-header">
        <div>
          <h3>Record fields for {form.recordName || createdAttribute?._name}</h3>
          <p>Select existing attributes to include in this record.</p>
        </div>
        <button type="button" class="ghost" on:click={() => goto('/admin')}>Cancel</button>
      </div>
      {#if attributesError}
        <div class="error">{attributesError}</div>
      {:else}
        <div class="record-controls">
          <select bind:value={selectedAttrId}>
            <option value="">Select attribute</option>
            {#each attributes as attr}
              {#if attr._id !== createdAttribute?._id}
                <option value={attr._id}>{attr._alias || attr._name}</option>
              {/if}
            {/each}
          </select>
          <input
            type="text"
            placeholder="Field alias (optional)"
            bind:value={fieldAlias}
          />
          <button type="button" on:click={addField}>Add field</button>
        </div>
    {#if recordErrors.fields}
      <div class="field-error">{recordErrors.fields}</div>
    {:else if fieldAliasIssue}
      <div class="field-error">{fieldAliasIssue}</div>
    {/if}
        <div class="field-list">
          {#if recordFields.length === 0}
            <div class="muted">No fields added yet.</div>
          {:else}
            {#each recordFields as field, index}
              <div
                class="field-row"
                class:drag-over={index === dragOverIndex}
                class:drag-over-before={index === dragOverIndex && dragOverPosition === 'before'}
                class:drag-over-after={index === dragOverIndex && dragOverPosition === 'after'}
                tabindex="0"
                on:keydown={(event) => handleRowKeydown(event, index)}
                on:dragover={(event) => handleDragOver(event, index)}
                on:dragenter={() => handleDragEnter(index)}
                on:drop={() => handleDrop(index)}
              >
                <span
                  class="drag-handle"
                  draggable="true"
                  on:dragstart={() => handleDragStart(index)}
                  on:dragend={handleDragEnd}
                  title="Drag to reorder"
                >
                  ::
                </span>
                <div class="field-name">{field.name}</div>
                <input
                  type="text"
                  value={field.alias}
                  placeholder="Alias"
                  on:input={(event) => updateFieldAlias(index, event.target.value)}
                />
                <div class="field-actions">
                  <button type="button" class="ghost" on:click={() => removeField(field.attrId)}>Remove</button>
                </div>
              </div>
            {/each}
          {/if}
        </div>
      {/if}
      <div class="actions">
        <button type="button" class="ghost" on:click={() => goto('/admin')}>Cancel</button>
        <button type="button" class="primary" on:click={handleRecordSubmit} disabled={creating}>
          {creating ? 'Saving…' : 'Save record fields'}
        </button>
      </div>
    </div>
  {/if}
</div>

<style>
  .wizard {
    display: grid;
    gap: 1.2rem;
  }

  .steps {
    display: flex;
    gap: 0.8rem;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.12rem;
    color: var(--text-muted);
  }

  .steps div {
    padding: 0.3rem 0.6rem;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.06);
  }

  .steps div.active {
    background: rgba(255, 190, 92, 0.2);
    color: #ffe4b0;
  }

  .panel {
    background: rgba(8, 10, 18, 0.75);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 1.2rem;
    padding: 1.4rem;
    display: grid;
    gap: 1.2rem;
  }

  .field-grid {
    display: grid;
    gap: 0.8rem;
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

  .checkbox {
    align-items: center;
    grid-template-columns: auto 1fr;
    gap: 0.6rem;
    color: var(--text);
  }

  .record-name.disabled {
    opacity: 0.6;
  }

  .description-block {
    display: grid;
    gap: 0.8rem;
    border-top: 1px solid rgba(255, 255, 255, 0.08);
    padding-top: 1rem;
  }

  .description-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
  }

  .description-header h4 {
    margin: 0 0 0.3rem;
  }

  .description-header p {
    margin: 0;
    color: var(--text-muted);
    font-size: 0.9rem;
  }

  .description-row {
    display: grid;
    grid-template-columns: 0.6fr 1fr 2fr auto;
    gap: 0.6rem;
    align-items: start;
  }

  .description-row textarea {
    background: rgba(8, 10, 18, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-radius: 0.6rem;
    padding: 0.5rem 0.6rem;
    font-size: 0.95rem;
    resize: vertical;
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

  .record-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
  }

  .record-header h3 {
    margin: 0 0 0.4rem;
  }

  .record-header p {
    margin: 0;
    color: var(--text-muted);
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
    border-color: rgba(255, 190, 92, 0.4);
    background: rgba(255, 190, 92, 0.08);
  }

  .field-row:focus-visible {
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

  .field-name {
    font-weight: 600;
    color: var(--text);
  }

  .drag-handle {
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

  .field-actions {
    display: flex;
    gap: 0.4rem;
    flex-wrap: wrap;
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

    .description-row {
      grid-template-columns: 1fr;
    }

    .actions {
      justify-content: flex-start;
    }
  }
</style>
