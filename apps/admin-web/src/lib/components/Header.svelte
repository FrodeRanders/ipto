<script>
  import { page } from '$app/state';
  import SignIn from './SignIn.svelte';

  const tabs = [
    { label: 'Overview', href: '/' },
    { label: 'Administration', href: '/admin' },
    { label: 'Trees', href: '/trees' },
    { label: 'Search', href: '/search' }
  ];

  const contextByPath = {
    '/': 'System dashboard',
    '/admin': 'Schema administration',
    '/trees': 'Tenant unit trees',
    '/search': 'Unit search'
  };

  let currentPath = $derived(page.url.pathname);
  let context = $derived(contextByPath[currentPath] || 'Unit view');
</script>

<header class="header">
  <div class="brand">
    <img class="logo" src="/ipto-lockup.svg" alt="IPTO" />
    <div class="context">
      <div class="title">Admin Console</div>
      <div class="subtitle">{context}</div>
    </div>
  </div>

  <nav class="tabs">
    {#each tabs as tab}
      <a class:active={currentPath === tab.href} href={tab.href}>{tab.label}</a>
    {/each}
  </nav>

  <SignIn />
</header>

<style>
  .header {
    position: sticky;
    top: 0;
    z-index: 10;
    display: grid;
    grid-template-columns: minmax(220px, 1fr) auto minmax(220px, 1fr);
    align-items: center;
    gap: 1rem;
    padding: 1.2rem 2.4rem;
    border-bottom: 1px solid var(--line);
    background: rgba(12, 15, 25, 0.92);
    backdrop-filter: blur(14px);
  }

  .brand {
    display: flex;
    align-items: center;
    gap: 1rem;
  }

  .logo {
    height: 40px;
    width: auto;
    display: block;
  }

  .title {
    font-size: 0.95rem;
    text-transform: uppercase;
    letter-spacing: 0.15rem;
    color: var(--text-muted);
  }

  .subtitle {
    font-size: 0.95rem;
    color: var(--text);
  }

  .tabs {
    display: flex;
    justify-content: center;
    gap: 1.5rem;
    font-size: 0.95rem;
  }

  .tabs a {
    text-decoration: none;
    color: var(--text-muted);
    padding: 0.4rem 0.8rem;
    border-radius: 999px;
    border: 1px solid transparent;
    transition: all 150ms ease;
  }

  .tabs a.active {
    color: var(--text);
    border-color: rgba(255, 255, 255, 0.2);
    background: rgba(255, 255, 255, 0.08);
  }

  .tabs a:hover {
    color: var(--text);
  }

  @media (max-width: 900px) {
    .header {
      grid-template-columns: 1fr;
      gap: 1rem;
    }

    .tabs {
      flex-wrap: wrap;
    }

  }
</style>
