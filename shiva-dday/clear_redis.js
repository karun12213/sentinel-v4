#!/usr/bin/env node
/**
 * Clear old Redis cache from OctaFX account
 */
const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: 'https://growing-crow-80382.upstash.io',
  token: 'gQAAAAAAATn-AAIncDJlNjdjM2M4OTQzOTg0OGRhYjE3MzRjNjNhM2U1ZDUzNnAyODAzODI'
});

(async () => {
  console.log('🗑️  Clearing old Redis cache...');
  const keys = await redis.keys('shiva:*');
  console.log('Keys to delete:', keys);
  for (const key of keys) {
    await redis.del(key);
    console.log('  Deleted:', key);
  }
  console.log('✅ All old data cleared!');
  
  // Check account info
  const { Redis: Redis2 } = require('@upstash/redis');
  const redis2 = new Redis2({
    url: 'https://growing-crow-80382.upstash.io',
    token: 'gQAAAAAAATn-AAIncDJlNjdjM2M4OTQzOTg0OGRhYjE3MzRjNjNhM2U1ZDUzNnAyODAzODI'
  });
  
  console.log('\nRemaining keys:', await redis2.keys('shiva:*'));
  process.exit(0);
})();
