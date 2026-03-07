<script>
  export let data;

  $: mimetype = data?.mimetype || 'unknown';
  $: filename = data?.filename || 'untitled';

  let prettyJson = '';
  $: {
    if (data && data.value && mimetype === 'application/json') {
      prettyJson = JSON.stringify(data.value, null, 2);
    } else {
      prettyJson = '';
    }
  }
</script>

<div class="preview">
  <div class="meta">
    <div>
      <div class="label">Filename</div>
      <div class="value">{filename}</div>
    </div>
    <div>
      <div class="label">MIME</div>
      <div class="value">{mimetype}</div>
    </div>
  </div>

  {#if mimetype === 'application/json' && prettyJson}
    <pre class="json">{prettyJson}</pre>
  {:else}
    <div class="fallback">
      Safe preview is unavailable. Download or open with a trusted viewer.
    </div>
  {/if}
</div>

<style>
  .preview {
    border-radius: 0.9rem;
    border: 1px solid rgba(255, 255, 255, 0.08);
    padding: 1rem;
    background: rgba(255, 255, 255, 0.03);
  }

  .meta {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 1rem;
    margin-bottom: 0.8rem;
  }

  .label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1rem;
    color: var(--text-muted);
  }

  .value {
    font-weight: 600;
  }

  .json {
    background: #0a0d16;
    padding: 0.8rem;
    border-radius: 0.6rem;
    font-size: 0.85rem;
    color: #a7f0ff;
    overflow: auto;
  }

  .fallback {
    color: var(--text-muted);
    font-size: 0.9rem;
  }
</style>
