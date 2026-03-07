import { writable } from 'svelte/store';

export const role = writable('user');

export const isAdmin = (value) => value === 'administrator';
