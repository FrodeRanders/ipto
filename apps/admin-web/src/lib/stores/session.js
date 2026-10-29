import { writable } from 'svelte/store';

export const session = writable(null);
let authorization = '';

// Credentials stay in memory for this tab and are never written to browser storage.
export const signIn = async (username, password) => {
  if (!username || username.includes(':')) throw new Error('Enter a valid username.');
  const bytes = new TextEncoder().encode(`${username}:${password}`);
  const header = `Basic ${btoa(String.fromCharCode(...bytes))}`;
  const response = await fetch('/api/session', {
    credentials: 'omit',
    headers: { Authorization: header }
  });
  if (!response.ok) throw new Error('Sign-in failed. Check your username and password.');
  const identity = await response.json();
  authorization = header;
  session.set(identity);
};

export const signOut = () => {
  authorization = '';
  session.set(null);
};

export const apiFetch = async (url, options = {}) => {
  const headers = new Headers(options.headers);
  if (authorization) headers.set('Authorization', authorization);
  const response = await fetch(url, { ...options, headers, credentials: 'omit' });
  if (response.status === 401) signOut();
  return response;
};
