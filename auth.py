"""
Authentication module for Bigeye MCP Server

Provides secure credential storage and authentication management.
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from cryptography.fernet import Fernet


class SecureStorage:
    """Secure credential storage with encryption"""
    
    def __init__(self):
        self.storage_path = Path.home() / '.bigeye-mcp' / 'credentials.enc'
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.key = self._get_or_create_key()
        self.cipher = Fernet(self.key)
    
    def _get_or_create_key(self) -> bytes:
        """Get or create encryption key"""
        key_path = Path.home() / '.bigeye-mcp' / '.key'
        if key_path.exists():
            return key_path.read_bytes()
        else:
            key = Fernet.generate_key()
            key_path.write_bytes(key)
            # Set restrictive permissions (Unix-like systems)
            try:
                os.chmod(key_path, 0o600)
            except:
                pass  # Windows doesn't support chmod
            return key
    
    def save_credentials(self, instance: str, workspace_id: int, api_key: str):
        """Save encrypted credentials"""
        creds = {}
        if self.storage_path.exists():
            try:
                encrypted = self.storage_path.read_bytes()
                decrypted = self.cipher.decrypt(encrypted)
                creds = json.loads(decrypted)
            except:
                pass  # Start fresh if decryption fails
        
        # Store by instance and workspace
        if instance not in creds:
            creds[instance] = {}
        creds[instance][str(workspace_id)] = {
            'api_key': api_key,
            'saved_at': datetime.now().isoformat()
        }
        
        encrypted = self.cipher.encrypt(json.dumps(creds).encode())
        self.storage_path.write_bytes(encrypted)
        try:
            os.chmod(self.storage_path, 0o600)
        except:
            pass  # Windows doesn't support chmod
    
    def get_credentials(self, instance: str, workspace_id: int) -> Optional[str]:
        """Retrieve API key for instance/workspace"""
        if not self.storage_path.exists():
            return None
        
        try:
            encrypted = self.storage_path.read_bytes()
            decrypted = self.cipher.decrypt(encrypted)
            creds = json.loads(decrypted)
            
            if instance in creds and str(workspace_id) in creds[instance]:
                return creds[instance][str(workspace_id)]['api_key']
        except:
            pass
        
        return None
    
    def list_saved_credentials(self) -> Dict[str, List[int]]:
        """List all saved instance/workspace combinations"""
        if not self.storage_path.exists():
            return {}
        
        try:
            encrypted = self.storage_path.read_bytes()
            decrypted = self.cipher.decrypt(encrypted)
            creds = json.loads(decrypted)
            
            result = {}
            for instance, workspaces in creds.items():
                result[instance] = [int(ws_id) for ws_id in workspaces.keys()]
            return result
        except:
            return {}
    
    def delete_credentials(self, instance: Optional[str] = None, workspace_id: Optional[int] = None):
        """Delete saved credentials"""
        if not self.storage_path.exists():
            return
        
        if instance is None and workspace_id is None:
            # Delete all credentials
            try:
                self.storage_path.unlink()
            except:
                pass
            return
        
        try:
            encrypted = self.storage_path.read_bytes()
            decrypted = self.cipher.decrypt(encrypted)
            creds = json.loads(decrypted)
            
            if instance and instance in creds:
                if workspace_id:
                    # Delete specific workspace
                    creds[instance].pop(str(workspace_id), None)
                    if not creds[instance]:
                        creds.pop(instance, None)
                else:
                    # Delete all workspaces for instance
                    creds.pop(instance, None)
            
            if creds:
                encrypted = self.cipher.encrypt(json.dumps(creds).encode())
                self.storage_path.write_bytes(encrypted)
                try:
                    os.chmod(self.storage_path, 0o600)
                except:
                    pass
            else:
                self.storage_path.unlink()
        except:
            pass


class BigeyeAuthClient:
    """Enhanced Bigeye client with authentication management"""
    
    def __init__(self, session=None):
        self.storage = SecureStorage()
        self.current_instance = None
        self.current_workspace_id = None
        self.api_key = None
        self.session = session
        self._workspaces_cache = {}
        self._cache_expiry = {}
    
    @property
    def is_authenticated(self) -> bool:
        """Check if client is authenticated"""
        return bool(self.api_key and self.current_instance and self.current_workspace_id)
    
    @property
    def api_base_url(self) -> Optional[str]:
        """Get API base URL"""
        if not self.current_instance:
            return None
        return f"{self.current_instance}/api/v1"
    
    async def test_authentication(self, instance: str, api_key: str) -> Dict[str, Any]:
        """Test if API key is valid"""
        if not self.session:
            import httpx
            async with httpx.AsyncClient() as client:
                return await self._test_auth_with_client(client, instance, api_key)
        else:
            return await self._test_auth_with_client(self.session, instance, api_key)
    
    async def _test_auth_with_client(self, client, instance: str, api_key: str) -> Dict[str, Any]:
        """Internal method to test auth with a given client"""
        headers = {
            'Authorization': f'apikey {api_key}',
            'Content-Type': 'application/json'
        }
        
        try:
            # First try the user endpoint
            response = await client.get(
                f"{instance}/api/v1/user",
                headers=headers,
                follow_redirects=False
            )
            
            if response.status_code == 200:
                try:
                    user_data = response.json()
                    # Handle case where response might be a string or invalid JSON
                    if isinstance(user_data, str):
                        return {
                            'valid': False,
                            'error': f'Invalid response format: {user_data[:100]}'
                        }
                    return {
                        'valid': True,
                        'user': user_data.get('email', 'Unknown'),
                        'instance': instance
                    }
                except Exception as e:
                    return {
                        'valid': False,
                        'error': f'Failed to parse response: {str(e)}'
                    }
            elif response.status_code == 404:
                # If user endpoint doesn't exist, try workspaces endpoint
                # This is a fallback for instances that don't have /api/v1/user
                response = await client.get(
                    f"{instance}/api/v1/workspaces",
                    headers=headers,
                    follow_redirects=False
                )
                
                if response.status_code == 200:
                    # If we can list workspaces, auth is valid
                    try:
                        # Verify response is valid JSON
                        workspaces = response.json()
                        if isinstance(workspaces, str):
                            return {
                                'valid': False,
                                'error': f'Invalid workspaces response format: {workspaces[:100]}'
                            }
                        return {
                            'valid': True,
                            'user': 'Authenticated User',  # Can't get email without user endpoint
                            'instance': instance
                        }
                    except Exception as e:
                        return {
                            'valid': False,
                            'error': f'Failed to parse workspaces response: {str(e)}'
                        }
                else:
                    return {
                        'valid': False,
                        'error': f'Authentication failed: {response.status_code}'
                    }
            else:
                return {
                    'valid': False,
                    'error': f'Authentication failed: {response.status_code}'
                }
        except Exception as e:
            import traceback
            print(f"[BIGEYE AUTH DEBUG] Connection error: {str(e)}")
            print(f"[BIGEYE AUTH DEBUG] Traceback: {traceback.format_exc()}")
            return {
                'valid': False,
                'error': f'Connection error: {str(e)}'
            }
    
    async def discover_workspaces(self, instance: str, api_key: str) -> List[Dict[str, Any]]:
        """Discover available workspaces"""
        # Check cache first
        cache_key = f"{instance}:{api_key[:8]}"
        if cache_key in self._workspaces_cache:
            if datetime.now() < self._cache_expiry.get(cache_key, datetime.min):
                return self._workspaces_cache[cache_key]
        
        if not self.session:
            import httpx
            async with httpx.AsyncClient() as client:
                workspaces = await self._discover_workspaces_with_client(client, instance, api_key)
        else:
            workspaces = await self._discover_workspaces_with_client(self.session, instance, api_key)
        
        # Cache for 5 minutes
        self._workspaces_cache[cache_key] = workspaces
        self._cache_expiry[cache_key] = datetime.now() + timedelta(minutes=5)
        return workspaces
    
    async def _discover_workspaces_with_client(self, client, instance: str, api_key: str) -> List[Dict[str, Any]]:
        """Internal method to discover workspaces with a given client"""
        headers = {
            'Authorization': f'apikey {api_key}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = await client.get(
                f"{instance}/api/v1/workspaces",
                headers=headers
            )
            if response.status_code == 200:
                try:
                    workspaces = response.json()
                    # Ensure we have a list
                    if not isinstance(workspaces, list):
                        print(f"[BIGEYE AUTH DEBUG] Unexpected workspaces format: {type(workspaces)}")
                        return []
                    return workspaces
                except Exception as e:
                    print(f"[BIGEYE AUTH DEBUG] Failed to parse workspaces: {str(e)}")
                    return []
            else:
                print(f"[BIGEYE AUTH DEBUG] Failed to get workspaces: {response.status_code}")
                return []
        except:
            return []
    
    def set_credentials(self, instance: str, workspace_id: int, api_key: str):
        """Set current credentials"""
        self.current_instance = instance.rstrip('/')
        self.current_workspace_id = workspace_id
        self.api_key = api_key
    
    def get_headers(self) -> Dict[str, str]:
        """Get headers for API requests"""
        if not self.api_key:
            return {}
        return {
            'Authorization': f'apikey {self.api_key}',
            'Content-Type': 'application/json'
        }