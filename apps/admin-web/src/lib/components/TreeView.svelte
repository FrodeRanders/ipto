<script>
  export let node;
  export let level = 0;
  export let selectedId;
  export let onSelect = () => {};
  export let onToggle = () => {};

  let isOpen = false;

  const toggle = () => {
    const nextOpen = !isOpen;
    isOpen = nextOpen;
    if (nextOpen) {
      onToggle(node);
    }
  };

  $: canExpand = !node.childrenLoaded || (node.children && node.children.length > 0);
</script>

<div class="node" style={`--level:${level}`}
  class:selected={node.id === selectedId}
  on:click={() => onSelect(node)}>
  <button type="button" class="caret" on:click|stopPropagation={toggle}>
    {#if canExpand}
      {isOpen ? '▼' : '▶'}
    {:else}
      •
    {/if}
  </button>
  <span class="label">{node.name}</span>
  {#if node.isLoading}
    <span class="loading">loading...</span>
  {/if}
</div>

{#if isOpen}
  {#if node.children && node.children.length}
    <div class="children">
      {#each node.children as child}
        <svelte:self node={child} level={level + 1} {selectedId} {onSelect} {onToggle} />
      {/each}
    </div>
  {/if}
{/if}

<style>
  .node {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.5rem 0.6rem 0.5rem calc(0.6rem + var(--level) * 1.2rem);
    border-radius: 0.8rem;
    cursor: pointer;
    transition: background 120ms ease;
  }

  .node:hover {
    background: rgba(255, 255, 255, 0.06);
  }

  .node.selected {
    background: rgba(255, 190, 92, 0.12);
    border: 1px solid rgba(255, 190, 92, 0.3);
  }

  .caret {
    border: none;
    background: rgba(255, 255, 255, 0.08);
    color: var(--text-muted);
    border-radius: 0.4rem;
    width: 1.4rem;
    height: 1.4rem;
    cursor: pointer;
  }

  .label {
    color: var(--text);
  }

  .loading {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08rem;
    color: var(--text-muted);
  }

  .children {
    margin-top: 0.2rem;
  }

  .children.empty {
    margin-left: calc(1.6rem + var(--level) * 1.2rem);
    color: var(--text-muted);
    font-size: 0.85rem;
    padding: 0.3rem 0.2rem;
  }
</style>
