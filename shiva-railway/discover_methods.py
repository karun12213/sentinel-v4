import asyncio
import os
from metaapi_cloud_sdk import MetaApi
from dotenv import load_dotenv

async def main():
    load_dotenv('.shiva_env')
    token = os.getenv('METAAPI_TOKEN')
    account_id = os.getenv('METAAPI_ACCOUNT_ID')
    
    api = MetaApi(token)
    try:
        account = await api.metatrader_account_api.get_account(account_id)
        
        print("Methods in account:")
        for method in dir(account):
            if not method.startswith('_'):
                print(f"  - {method}")
        
        connection = account.get_rpc_connection()
        print("\nMethods in connection:")
        for method in dir(connection):
            if not method.startswith('_'):
                print(f"  - {method}")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
