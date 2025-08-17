"""
Simple Application that adds items to a Kroger cart. Meant to be run on a schedule with regularly updated items.
"""

import requests
import json
import base64
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from typing import Dict, List, Optional

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.system import Secret
from prefect.variables import Variable

# Block Management Tasks
@task(name="Load Kroger Config Block", retries=2)
def load_kroger_config_block() -> Dict:
    """Load Kroger API configuration from Prefect JSON block"""
    logger = get_run_logger()
    
    try:
        config = Variable.get("kroger-config")
        logger.info("Configuration loaded from Prefect block successfully")
        return config
    except Exception as e:
        logger.error(f"Failed to load config block: {str(e)}")
        raise

@task(name="Load Encryption Key Block")
def load_encryption_key_block() -> bytes:
    """Load encryption key from Prefect Secret block"""
    logger = get_run_logger()
    
    try:
        key_block = Secret.load("kroger-encryption-key")
        # The key is stored as base64 encoded string in the secret
        key_b64 = key_block.get()
        key = base64.b64decode(key_b64)
        logger.info("Encryption key loaded from Prefect block successfully")
        return key
    except Exception as e:
        logger.error(f"Failed to load encryption key block: {str(e)}")
        raise

@task(name="Load Stored Tokens Block", retries=1)
def load_stored_tokens_block(encryption_key: bytes) -> Dict:
    """Load and decrypt stored authentication tokens from Prefect block"""
    logger = get_run_logger()
    
    try:
        tokens_block = Secret.load("kroger-tokens")
        encrypted_tokens_b64 = tokens_block.get()
        
        if not encrypted_tokens_b64:
            logger.warning("No stored tokens found in block")
            return {}
        
        # Decode from base64 and decrypt
        encrypted_tokens = base64.b64decode(encrypted_tokens_b64)
        f = Fernet(encryption_key)
        decrypted_tokens = f.decrypt(encrypted_tokens)
        tokens = json.loads(decrypted_tokens.decode())
        
        logger.info("Tokens loaded from Prefect block successfully")
        return tokens
        
    except Exception as e:
        logger.error(f"Failed to load tokens from block: {str(e)}")
        return {}

@task(name="Save Tokens to Block")
def save_tokens_to_block(tokens: Dict, encryption_key: bytes):
    """Encrypt and save tokens to Prefect Secret block"""
    logger = get_run_logger()
    
    try:
        # Add metadata
        tokens['obtained_at'] = datetime.now().isoformat()
        tokens['expires_at'] = (
            datetime.now() + timedelta(seconds=tokens.get('expires_in', 3600))
        ).isoformat()
        
        # Encrypt and encode
        f = Fernet(encryption_key)
        encrypted_tokens = f.encrypt(json.dumps(tokens).encode())
        encrypted_tokens_b64 = base64.b64encode(encrypted_tokens).decode()
        
        # Save to block (create or update)
        try:
            tokens_block = Secret.load("kroger-tokens")
            # Update existing block
            tokens_block.value = encrypted_tokens_b64
            tokens_block.save("kroger-tokens", overwrite=True)
        except Exception as e:
            logger.error(f"Failed to save tokens to block: {str(e)}")
            raise
        
        logger.info("Tokens saved to Prefect block successfully")
        
    except Exception as e:
        logger.error(f"Failed to save tokens to block: {str(e)}")
        raise

@task(name="Check Token Validity")
def is_token_expired(tokens: Dict) -> bool:
    """Check if access token is expired or about to expire"""
    logger = get_run_logger()
    
    if not tokens or 'expires_at' not in tokens:
        logger.info("No valid token expiry found")
        return True
    
    try:
        expiry_time = datetime.fromisoformat(tokens['expires_at'])
        buffer_time = timedelta(minutes=5)
        is_expired = datetime.now() >= (expiry_time - buffer_time)
        
        logger.info(f"Token expired: {is_expired}")
        return is_expired
        
    except Exception as e:
        logger.error(f"Error checking token validity: {str(e)}")
        return True

@task(name="Refresh Access Token", retries=2)
def refresh_access_token(config: Dict, tokens: Dict, client_secret: str, encryption_key: bytes) -> Dict:
    """Refresh access token using refresh token"""
    logger = get_run_logger()
    
    if not tokens.get('refresh_token'):
        raise Exception("No refresh token available. Please run OAuth setup and store tokens in blocks.")
    
    logger.info("Refreshing access token...")
    
    base_url = "https://api.kroger.com/v1"
    token_url = f"{base_url}/connect/oauth2/token"
    
    token_data = {
        'grant_type': 'refresh_token',
        'refresh_token': tokens['refresh_token'],
        'client_id': config['client_id'],
        'client_secret': client_secret
    }
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        response = requests.post(token_url, data=token_data, headers=headers)
        response.raise_for_status()
        
        new_tokens = response.json()
        
        # Preserve refresh token if not provided in response
        if 'refresh_token' not in new_tokens and 'refresh_token' in tokens:
            new_tokens['refresh_token'] = tokens['refresh_token']
        
        # Save to Prefect block
        save_tokens_to_block(new_tokens, encryption_key)
        
        logger.info("Access token refreshed and saved to block successfully")
        return new_tokens
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to refresh token: {str(e)}")
        raise

@task(name="Ensure Valid Token")
def ensure_valid_token(config: Dict, client_secret: str, encryption_key: bytes) -> str:
    """Ensure we have a valid access token, refresh if necessary"""
    tokens = load_stored_tokens_block(encryption_key)
    
    if is_token_expired(tokens):
        tokens = refresh_access_token(config, tokens, client_secret, encryption_key)
    
    return tokens['access_token']

@task(name="Get Client Secret")
def get_client_secret(client_secret: str = None) -> str:
    """Get client secret from parameter, environment, or Prefect Secret block"""
    logger = get_run_logger()
    
    # Try parameter first
    if client_secret:
        return client_secret
    
    # Try environment variable
    import os
    env_secret = os.getenv('KROGER_CLIENT_SECRET')
    if env_secret:
        logger.info("Using client secret from environment variable")
        return env_secret
    
    # Try Prefect Secret block
    try:
        secret_block = Secret.load("kroger-client-secret")
        logger.info("Using client secret from Prefect Secret block")
        return secret_block.get()
    except Exception as e:
        logger.error(f"Failed to load client secret from block: {str(e)}")
        raise
    
    raise ValueError(
        "Client secret not found. Please provide it as parameter, environment variable, "
        "or create a Prefect Secret block named 'kroger-client-secret'"
    )

# Core API Request Task
@task(name="Make Kroger API Request", retries=3)
def make_kroger_request(
    method: str,
    endpoint: str,
    access_token: str,
    json_data: Optional[Dict] = None,
    headers: Optional[Dict] = None
) -> Dict:
    """Make authenticated API request to Kroger"""
    logger = get_run_logger()
    
    base_url = "https://api.kroger.com/v1"
    url = f"{base_url}{endpoint}"
    
    request_headers = headers or {}
    request_headers.update({
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    })
    
    kwargs = {'headers': request_headers}
    if json_data:
        kwargs['json'] = json_data
        request_headers['Content-Type'] = 'application/json'
    
    try:
        response = requests.request(method, url, **kwargs)
        logger.info(f"{method} {endpoint} -> {response.status_code}")
        
        if response.status_code == 204:
            return {'success': True, 'status_code': 204}
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        raise

# Cart Operations Task
@task(name="Add Items to Cart")
def add_items_to_cart(access_token: str, items: List[Dict]) -> Dict:
    """Add items to shopping cart"""
    logger = get_run_logger()
    logger.info(f"Adding {len(items)} items to cart")
    
    cart_data = {
        'items': [
            {
                'upc': item['upc'],
                'quantity': item['quantity']
            }
            for item in items
        ]
    }
    
    result = make_kroger_request('PUT', '/cart/add', access_token, json_data=cart_data)
    
    if result.get('status_code') == 204:
        logger.info(f"Successfully added {len(items)} items to cart")
        return {'success': True, 'items_added': len(items)}
    
    return result

@task(name="Create Cart Report")
def create_cart_report(items: List[Dict], result: Dict) -> str:
    """Create a markdown report of cart activity"""
    
    report = f"""# Kroger Cart Activity Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Items Processed
"""
    
    for item in items:
        report += f"- **UPC**: {item['upc']} | **Quantity**: {item['quantity']}\n"
    
    report += f"\n## Result\n"
    if result.get('success'):
        report += f"✅ **Success**: Added {result.get('items_added', 0)} items to cart\n"
    else:
        report += f"❌ **Failed**: {result.get('message', 'Unknown error')}\n"
    
    return report

# Main Flow
@flow(
    name="Kroger Add to Cart Flow",
    description="Add items with known UPCs to Kroger cart using Prefect blocks",
    task_runner=ConcurrentTaskRunner()
)
def kroger_add_to_cart_flow(
    items: List[Dict],
) -> Dict:
    """
    Main flow to add items to Kroger cart using Prefect blocks for configuration
    
    Args:
        items: List of items to add. Each item should have:
               - upc: Product UPC code 
               - quantity: Quantity to add
        client_secret: Kroger API client secret (optional if stored in block)
        
    Example:
        items = [
            {'upc': '0001111042908', 'quantity': 1},
            {'upc': '0001111041660', 'quantity': 2}
        ]
    """
    logger = get_run_logger()
    logger.info(f"Starting Kroger add to cart flow for {len(items)} items")
    
    # Validate input
    if not items:
        raise ValueError("Items list cannot be empty")
    
    for item in items:
        if 'upc' not in item or 'quantity' not in item:
            raise ValueError("Each item must have 'upc' and 'quantity' fields")
    client_secret = Secret.load("kroger-client-secret").get()
    # Load configuration and secrets from Prefect blocks
    config = load_kroger_config_block()
    encryption_key = load_encryption_key_block()
    client_secret = get_client_secret(client_secret)
    
    # Ensure valid authentication
    access_token = ensure_valid_token(config, client_secret, encryption_key)
    
    # Add items to cart
    cart_result = add_items_to_cart(access_token, items)
    
    # Create report
    report_content = create_cart_report(items, cart_result)
    
    # Create Prefect artifact with the report
    create_markdown_artifact(
        key="kroger-cart-report",
        markdown=report_content,
        description=f"Cart activity report for {len(items)} items"
    )
    
    logger.info("Kroger add to cart flow completed successfully")
    
    return {
        'items': items,
        'result': cart_result,
        'report': report_content,
        'success': cart_result.get('success', False)
    }

if __name__ == "__main__":
    # Example: Add single batch of items
    items = [
        {'upc': '0001111041700', 'quantity': 1}, # Kroger 2% milk
        {'upc': '0065708206040', 'quantity': 1}, # Kroger sourdough loaf
        {'upc': '0007835555000', 'quantity': 1}, # Greek God's Plain Greek Yogurt
        {'upc': '0000000004011', 'quantity': 2}, # Bananas
    ]
    result = kroger_add_to_cart_flow(items=items)