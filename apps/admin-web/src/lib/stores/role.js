import { derived } from 'svelte/store';
import { session } from './session.js';

export const role = derived(session, (identity) => identity?.role || 'user');

export const isAdmin = (value) => value === 'administrator';
