<script>
  export let title = '';
  export let hint = '';
</script>

<div class="section-title">
  <h2>{title}</h2>
  {#if hint}
    <p>{hint}</p>
  {/if}
</div>

<style>
  .section-title {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    margin-bottom: 1.2rem;
  }

  h2 {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.5rem;
    margin: 0;
  }

  p {
    margin: 0;
    color: var(--text-muted);
    font-size: 0.95rem;
  }
</style>
