<script>
  import { createEventDispatcher } from 'svelte';
  import { role, isAdmin } from '../stores/role.js';
  import Badge from './Badge.svelte';

  export let title = '';
  export let items = [];
  export let fields = [];
  export let actionLabel = 'Add';
  export let actionId = '';

  const dispatch = createEventDispatcher();

  const handleAction = () => {
    dispatch('action', { id: actionId, label: actionLabel, title });
  };

  const handleEdit = (item) => {
    dispatch('edit', { id: actionId, item });
  };
</script>

<div class="admin-section">
  <div class="header">
    <div>
      <h3>{title}</h3>
      <p>Visible to all users. Editing requires administrator role.</p>
    </div>
    {#if isAdmin($role)}
      <button class="primary" type="button" on:click={handleAction}>{actionLabel}</button>
    {/if}
  </div>

  <div class="list">
    {#each items as item}
      <div class="row">
        <div class="main">
          <div class="name">
            {#if item.prefix}
              <span class="prefix">{item.prefix}</span>
            {/if}
            <span>{item.name}</span>
            {#if item.subname}
              <span class="subname">{item.subname}</span>
            {/if}
          </div>
          {#if item.description}
            <div class="description">{item.description}</div>
          {/if}
          {#if item.structure && item.structure.length}
            <div class="structure">
              {#each item.structure as entry}
                <div class="structure-item">{entry}</div>
              {/each}
            </div>
          {/if}
        </div>
        <div class="meta">
          {#each fields as field}
            <Badge label={item[field]} tone="neutral" />
          {/each}
        </div>
        {#if isAdmin($role)}
          <div class="actions">
            <button type="button" on:click={() => handleEdit(item)}>Edit</button>
            <button type="button" class="ghost">Archive</button>
          </div>
        {/if}
      </div>
    {/each}
  </div>
</div>

<style>
  .admin-section {
    background: rgba(8, 10, 18, 0.75);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 1.2rem;
    padding: 1.4rem;
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
    margin-bottom: 1rem;
  }

  h3 {
    margin: 0 0 0.3rem;
    font-family: 'Space Grotesk', sans-serif;
  }

  p {
    margin: 0;
    font-size: 0.9rem;
    color: var(--text-muted);
  }

  .list {
    display: flex;
    flex-direction: column;
    gap: 0.8rem;
  }

  .row {
    display: grid;
    grid-template-columns: 2fr 1fr auto;
    gap: 1rem;
    align-items: center;
    padding: 0.8rem 1rem;
    border-radius: 0.9rem;
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid rgba(255, 255, 255, 0.05);
  }

  .name {
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .prefix {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08rem;
    padding: 0.1rem 0.4rem;
    border-radius: 999px;
    background: rgba(122, 180, 255, 0.18);
    color: #9fd0ff;
  }

  .description {
    color: var(--text-muted);
    font-size: 0.9rem;
  }

  .subname {
    font-size: 0.75rem;
    letter-spacing: 0.04rem;
    padding: 0.1rem 0.4rem;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.06);
    color: var(--text-muted);
    font-weight: 500;
  }

  .structure {
    display: grid;
    gap: 0.4rem;
    margin-top: 0.6rem;
    padding-left: 0.8rem;
    border-left: 2px solid rgba(255, 255, 255, 0.08);
  }

  .structure-item {
    color: var(--text-muted);
    font-size: 0.85rem;
  }

  .meta {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }

  .actions {
    display: flex;
    gap: 0.4rem;
  }

  button {
    border: none;
    border-radius: 999px;
    padding: 0.4rem 0.9rem;
    background: rgba(255, 255, 255, 0.1);
    color: var(--text);
    cursor: pointer;
    font-size: 0.85rem;
  }

  .primary {
    background: var(--accent);
    color: #0c0f19;
    font-weight: 600;
  }

  .ghost {
    background: transparent;
    border: 1px solid rgba(255, 255, 255, 0.1);
  }

  @media (max-width: 900px) {
    .row {
      grid-template-columns: 1fr;
      justify-items: start;
    }
  }
</style>
