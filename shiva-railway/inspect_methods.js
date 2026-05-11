const MetaApi = require('metaapi.cloud-sdk').default;
require('dotenv').config();

const TOKEN = process.env.METAAPI_TOKEN;
const ACCOUNT_ID = 'aa7256d0-f345-4e35-bcec-d2644bdf75c1';

async function check() {
  const api = new MetaApi(TOKEN);
  const account = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
  
  console.log('Account methods:', Object.getOwnPropertyNames(Object.getPrototypeOf(account)).filter(m => typeof account[m] === 'function'));
  
  const connection = account.getRPCConnection();
  await connection.connect();
  await connection.waitSynchronized();
  
  console.log('Connection methods:', Object.getOwnPropertyNames(Object.getPrototypeOf(connection)).filter(m => typeof connection[m] === 'function'));
  
  process.exit(0);
}

check().catch(e => {
  console.error(e);
  process.exit(1);
});
