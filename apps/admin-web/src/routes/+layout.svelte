<script>
  import Header from '$lib/components/Header.svelte';
</script>

<Header />
<main class="page">
  <slot />
</main>

<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=Manrope:wght@300;500;700&display=swap');

  :global(:root) {
    --bg: #0b0f1c;
    --bg-alt: #121829;
    --accent: #ffbe5c;
    --text: #f5f3ef;
    --text-muted: #a2a9ba;
    --line: rgba(255, 255, 255, 0.08);
  }

  :global(body) {
    margin: 0;
    font-family: 'Manrope', sans-serif;
    background: radial-gradient(circle at top left, #1b2440 0%, #0b0f1c 45%, #0b0f1c 100%);
    color: var(--text);
    min-height: 100vh;
  }

  :global(*) {
    box-sizing: border-box;
  }

  :global(a) {
    color: inherit;
  }

  .page {
    padding: 2.4rem;
    display: flex;
    flex-direction: column;
    gap: 2.4rem;
  }

  :global(.panel) {
    background: rgba(12, 15, 25, 0.75);
    border-radius: 1.2rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    padding: 1.6rem;
  }

  @media (max-width: 900px) {
    .page {
      padding: 1.4rem;
    }
  }
</style>
