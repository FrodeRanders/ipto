<script>
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import SectionTitle from '$lib/components/SectionTitle.svelte';
  import { fetchAttributeMetadata, fetchAttributeDescriptions, updateAttributeDescriptions } from '$lib/api.js';

  let attrId;
  let attribute = null;
  let descriptionItems = [];
  let error = '';
  let loading = false;
  let validationError = '';

  const normalizeValue = (value) => String(value ?? '').trim();
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

  $: attrId = Number($page.params.id);

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

  const validateDescriptions = () => {
    const items = descriptionItems
      .map((item) => ({
        lang: normalizeValue(item.lang) || 'SE',
        alias: normalizeValue(item.alias) || attribute?._name || '',
        description: normalizeValue(item.description)
      }))
      .filter((item) => item.description);

    if (!items.length) {
      return { error: 'Provide at least one description.' };
    }

    const seen = new Set();
    for (const item of items) {
      const lang = item.lang.toUpperCase();
      if (!ISO_LANG_RE.test(lang)) {
        return { error: `Language "${item.lang}" must be a two-letter ISO code.` };
      }
      const key = lang.toLowerCase();
      if (seen.has(key)) {
        return { error: `Duplicate language entry: ${item.lang}` };
      }
      seen.add(key);
    }

    return { items };
  };

  const handleSave = async () => {
    error = '';
    validationError = '';

    const { items, error: validationMessage } = validateDescriptions();
    if (validationMessage) {
      validationError = validationMessage;
      return;
    }

    loading = true;
    try {
      await updateAttributeDescriptions(attrId, items);
      await goto('/admin');
    } catch (err) {
      error = err?.message || 'Failed to update descriptions.';
    } finally {
      loading = false;
    }
  };

  onMount(async () => {
    error = '';
    if (!attrId || Number.isNaN(attrId)) {
      error = 'Attribute id is missing.';
      return;
    }

    try {
      const [attrs, descriptions] = await Promise.all([
        fetchAttributeMetadata(),
        fetchAttributeDescriptions(attrId)
      ]);
      attribute = attrs.find((item) => item._id === attrId) || null;
      descriptionItems = descriptions.length
        ? descriptions.map((item) => ({
            lang: item.lang || 'SE',
            alias: item.alias || '',
            description: item.description || ''
          }))
        : [{ lang: 'SE', alias: '', description: '' }];
    } catch (err) {
      error = err?.message || 'Failed to load attribute details.';
    }
  });
</script>

<section>
  <SectionTitle
    title="Edit attribute descriptions"
    hint="Manage multilingual labels and descriptions for this attribute."
  />
</section>

<div class="panel">
  {#if error}
    <div class="error">{error}</div>
  {/if}

  {#if attribute}
    <div class="attribute-summary">
      <div class="summary-name">{attribute._alias || attribute._name}</div>
      <div class="summary-meta">{attribute._type} · {attribute._cardinality}</div>
      {#if attribute._qual_name}
        <div class="summary-sub">{attribute._qual_name}</div>
      {/if}
    </div>
  {/if}

  <div class="description-header">
    <div>
      <h4>Descriptions</h4>
      <p>Each language entry can override the alias and description.</p>
    </div>
    <button type="button" class="ghost" on:click={addDescriptionItem}>Add language</button>
  </div>

  {#if validationError}
    <div class="field-error">{validationError}</div>
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

  <div class="actions">
    <button type="button" class="ghost" on:click={() => goto('/admin')}>Cancel</button>
    <button type="button" class="primary" on:click={handleSave} disabled={loading}>
      {loading ? 'Saving…' : 'Save descriptions'}
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

  .attribute-summary {
    padding: 0.8rem 1rem;
    border-radius: 0.9rem;
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.08);
  }

  .summary-name {
    font-weight: 600;
    margin-bottom: 0.3rem;
  }

  .summary-meta,
  .summary-sub {
    color: var(--text-muted);
    font-size: 0.9rem;
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

  input,
  textarea {
    background: rgba(8, 10, 18, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-radius: 0.6rem;
    padding: 0.5rem 0.6rem;
    font-size: 0.95rem;
  }

  textarea {
    resize: vertical;
  }

  .actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.6rem;
  }

  .actions button,
  .description-header button {
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
    font-size: 0.85rem;
  }

  @media (max-width: 900px) {
    .description-row {
      grid-template-columns: 1fr;
    }

    .actions {
      justify-content: flex-start;
    }
  }
</style>
