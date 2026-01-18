<script>
  import DataPreview from './DataPreview.svelte';

  export let name;
  export let data;
  export let level = 0;

  const formatValue = (value) => {
    if (value === null || value === undefined) return '—';
    if (Array.isArray(value)) return value.join(', ');
    return value;
  };
</script>

<div class="attr" style={`--level:${level}`}>
  <div class="attr-name">
    <span class="attr-primary">{data?.alias || name}</span>
    {#if data?.alias && data.alias !== name}
      <span class="attr-alias">{name}</span>
    {/if}
    {#if data?.type}
      <span class="attr-type">{data.type}</span>
    {/if}
  </div>
  {#if data?.type === 'RECORD'}
    <div class="record">
      {#if data.attributes && data.attributes.length}
        {#each data.attributes as child}
          <svelte:self name={child.name || child.alias} data={child} level={level + 1} />
        {/each}
      {:else}
        <div class="attr-value">No nested attributes</div>
      {/if}
    </div>
  {:else if data?.value && typeof data.value === 'object' && data.value.mimetype}
    <DataPreview data={data.value} />
  {:else if data?.value && typeof data.value === 'object' && !Array.isArray(data.value)}
    <pre class="attr-json">{JSON.stringify(data.value, null, 2)}</pre>
  {:else}
    <div class="attr-value">{formatValue(data?.value)}</div>
  {/if}
</div>

<style>
  .attr {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
    padding: 0.9rem;
    border-radius: 0.8rem;
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.06);
    margin-left: calc(var(--level) * 0.6rem);
  }

  .attr-name {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }

  .attr-primary {
    font-weight: 600;
  }

  .attr-alias {
    font-size: 0.75rem;
    letter-spacing: 0.04rem;
    padding: 0.1rem 0.4rem;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.06);
    color: var(--text-muted);
  }

  .attr-type {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.08rem;
    padding: 0.12rem 0.5rem;
    border-radius: 999px;
    background: rgba(122, 180, 255, 0.18);
    color: #9fd0ff;
  }

  .attr-value {
    color: var(--text-muted);
  }

  .attr-json {
    background: #0a0d16;
    padding: 0.8rem;
    border-radius: 0.6rem;
    font-size: 0.85rem;
    color: #a7f0ff;
    overflow: auto;
  }

  .record {
    display: grid;
    gap: 0.6rem;
    border-left: 2px solid rgba(255, 255, 255, 0.08);
    padding-left: 0.8rem;
  }
</style>
