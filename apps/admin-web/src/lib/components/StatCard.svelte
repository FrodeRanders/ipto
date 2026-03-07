<script>
  export let label = '';
  export let value = '';
  export let detail = '';
</script>

<div class="card">
  <div class="value">{value}</div>
  <div class="label">{label}</div>
  {#if detail}
    <div class="detail">{detail}</div>
  {/if}
</div>

<style>
  .card {
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 1rem;
    padding: 1.2rem;
  }

  .value {
    font-size: 2rem;
    font-family: 'Space Grotesk', sans-serif;
  }

  .label {
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.12rem;
    font-size: 0.75rem;
  }

  .detail {
    margin-top: 0.6rem;
    font-size: 0.9rem;
    color: var(--text-muted);
  }
</style>
