<script>
  export let label = 'user';
  export let tone = 'neutral';
</script>

<span class={`badge ${tone}`}>{label}</span>

<style>
  .badge {
    padding: 0.3rem 0.7rem;
    border-radius: 999px;
    font-size: 0.75rem;
    letter-spacing: 0.08rem;
    text-transform: uppercase;
  }

  .neutral {
    background: rgba(255, 255, 255, 0.12);
    color: var(--text-muted);
  }

  .accent {
    background: rgba(255, 190, 92, 0.2);
    color: var(--accent);
    border: 1px solid rgba(255, 190, 92, 0.4);
  }
</style>
