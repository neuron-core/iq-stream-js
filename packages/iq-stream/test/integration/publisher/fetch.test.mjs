// The fetch build is what Workers and edge runtimes get; Node's own fetch stands in for theirs here.
import * as build from '../../../dist/publisher/fetch.js';
import { scenarios } from './scenarios.mjs';

scenarios('fetch', build);
