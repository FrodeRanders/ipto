<script>
  import { session, signIn, signOut } from '#lib/stores/session.js';
  import Badge from './Badge.svelte';

  let expanded = false;
  let username = '';
  let password = '';
  let error = '';
  let pending = false;

  const submit = async (event) => {
    event.preventDefault();
    pending = true;
    error = '';
    try {
      await signIn(username, password);
      expanded = false;
    } catch (failure) {
      error = failure.message;
    } finally {
      password = '';
      pending = false;
    }
  };
</script>

<div class="account">
  {#if $session}
    <span>{$session.username}</span>
    <Badge label={$session.role} tone={$session.role === 'administrator' ? 'accent' : 'neutral'} />
    <button type="button" onclick={signOut}>Sign out</button>
  {:else}
    <button type="button" aria-expanded={expanded} onclick={() => { expanded = !expanded; password = ''; error = ''; }}>Sign in</button>
    {#if expanded}
      <form onsubmit={submit} class="sign-in">
        <label>Username<input autocomplete="username" bind:value={username} required /></label>
        <label>Password<input type="password" autocomplete="current-password" bind:value={password} required /></label>
        {#if error}<p role="alert">{error}</p>{/if}
        <button type="submit" disabled={pending}>{pending ? 'Signing in…' : 'Sign in'}</button>
      </form>
    {/if}
  {/if}
</div>

<style>
  .account { position: relative; display: flex; gap: 0.8rem; align-items: center; justify-content: flex-end; }
  button { border: 1px solid var(--line); border-radius: 999px; padding: 0.45rem 0.9rem; background: var(--bg); color: var(--text); cursor: pointer; }
  button:disabled { opacity: 0.6; cursor: wait; }
  .sign-in { position: absolute; top: calc(100% + 0.8rem); right: 0; width: 280px; display: grid; gap: 0.8rem; padding: 1rem; background: var(--bg); border: 1px solid var(--line); border-radius: 1rem; box-shadow: 0 1rem 2rem #0008; }
  label { display: grid; gap: 0.3rem; }
  input { width: 100%; padding: 0.5rem; background: var(--bg-alt); border: 1px solid var(--line); border-radius: 0.4rem; color: var(--text); }
  p { margin: 0; color: #ff9f9f; }
</style>
