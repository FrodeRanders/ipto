import { writable } from 'svelte/store';

const STORAGE_KEY = 'ipto.tenantId';

const readStoredTenantId = () => {
  if (typeof window === 'undefined') return null;
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
};

export const tenantId = writable(readStoredTenantId());

if (typeof window !== 'undefined') {
  tenantId.subscribe((value) => {
    if (value === null || value === undefined) {
      window.localStorage.removeItem(STORAGE_KEY);
      return;
    }
    window.localStorage.setItem(STORAGE_KEY, String(value));
  });
}
