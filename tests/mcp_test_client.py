#!/usr/bin/env python3
"""
MCP Test Client for testing stdio communication with the Bigeye MCP Server.

This client sends JSON-RPC messages via stdio and validates responses.
"""

import json
import sys
import subprocess
import threading
import queue
import time
from typing import Dict, Any, Optional, List
import argparse
from pathlib import Path


class MCPTestClient:
    """Test client for MCP stdio communication."""
    
    def __init__(self, docker_image: str, env_file: Optional[str] = None, debug: bool = False):
        self.docker_image = docker_image
        self.env_file = env_file
        self.debug = debug
        self.process = None
        self.response_queue = queue.Queue()
        self.error_queue = queue.Queue()
        self._stop_reading = threading.Event()
        
    def start(self) -> bool:
        """Start the Docker container with MCP server."""
        cmd = [
            "docker", "run", "--rm", "-i",
            "--env", "BIGEYE_API_KEY=test_key",
            "--env", "BIGEYE_API_URL=https://app.bigeye.com",
            "--env", "BIGEYE_WORKSPACE_ID=12345"
        ]
        
        if self.env_file:
            cmd.extend(["--env-file", self.env_file])
            
        if self.debug:
            cmd.extend(["--env", "BIGEYE_DEBUG=true"])
            
        cmd.extend([self.docker_image, "python", "server.py"])
        
        try:
            self.process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=0  # Unbuffered
            )
            
            # Start reading threads
            self._start_readers()
            
            # Give server time to start
            time.sleep(2)
            
            return True
        except Exception as e:
            print(f"Failed to start container: {e}")
            return False
    
    def _start_readers(self):
        """Start threads to read stdout and stderr."""
        def read_stdout():
            while not self._stop_reading.is_set():
                try:
                    line = self.process.stdout.readline()
                    if line:
                        if self.debug:
                            print(f"[STDOUT] {line.strip()}")
                        try:
                            # Try to parse as JSON-RPC response
                            response = json.loads(line)
                            self.response_queue.put(response)
                        except json.JSONDecodeError:
                            # Not JSON, might be debug output
                            if self.debug:
                                print(f"[Non-JSON stdout] {line.strip()}")
                except Exception as e:
                    if self.debug:
                        print(f"[Read error] {e}")
                    break
        
        def read_stderr():
            while not self._stop_reading.is_set():
                try:
                    line = self.process.stderr.readline()
                    if line:
                        if self.debug:
                            print(f"[STDERR] {line.strip()}")
                        self.error_queue.put(line.strip())
                except Exception:
                    break
        
        threading.Thread(target=read_stdout, daemon=True).start()
        threading.Thread(target=read_stderr, daemon=True).start()
    
    def send_message(self, message: Dict[str, Any]) -> bool:
        """Send a JSON-RPC message to the server."""
        try:
            json_str = json.dumps(message)
            if self.debug:
                print(f"[SEND] {json_str}")
            self.process.stdin.write(json_str + "\n")
            self.process.stdin.flush()
            return True
        except Exception as e:
            print(f"Failed to send message: {e}")
            return False
    
    def get_response(self, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """Get a response from the server."""
        try:
            return self.response_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def get_errors(self) -> List[str]:
        """Get all error messages."""
        errors = []
        while not self.error_queue.empty():
            try:
                errors.append(self.error_queue.get_nowait())
            except queue.Empty:
                break
        return errors
    
    def stop(self):
        """Stop the Docker container."""
        self._stop_reading.set()
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()


def run_test_sequence(client: MCPTestClient, fixtures_file: str) -> Dict[str, bool]:
    """Run a sequence of MCP protocol tests."""
    # Load test fixtures
    with open(fixtures_file, 'r') as f:
        fixtures = json.load(f)
    
    results = {}
    
    # Test 1: Initialize handshake
    print("\n=== Test 1: Initialize Handshake ===")
    init_msg = fixtures["initialize"]["request"]
    if client.send_message(init_msg):
        response = client.get_response()
        if response and "result" in response:
            result = response["result"]
            expected_fields = fixtures["initialize"]["expected_response_fields"]
            has_all_fields = all(field in result for field in expected_fields)
            results["initialize"] = has_all_fields
            print(f"✓ Initialize: {'PASS' if has_all_fields else 'FAIL'}")
            if has_all_fields:
                print(f"  Server: {result.get('serverInfo', {}).get('name', 'Unknown')}")
                print(f"  Version: {result.get('serverInfo', {}).get('version', 'Unknown')}")
        else:
            results["initialize"] = False
            print("✗ Initialize: No response")
    else:
        results["initialize"] = False
        print("✗ Initialize: Failed to send")
    
    # Test 2: List tools
    print("\n=== Test 2: List Tools ===")
    list_tools_msg = fixtures["list_tools"]["request"]
    if client.send_message(list_tools_msg):
        response = client.get_response()
        if response and "result" in response:
            tools = response["result"].get("tools", [])
            tool_names = [tool["name"] for tool in tools]
            expected_tools = fixtures["list_tools"]["expected_tools"]
            has_all_tools = all(tool in tool_names for tool in expected_tools)
            results["list_tools"] = has_all_tools
            print(f"✓ List Tools: {'PASS' if has_all_tools else 'FAIL'}")
            print(f"  Found {len(tools)} tools")
        else:
            results["list_tools"] = False
            print("✗ List Tools: No response")
    
    # Test 3: List resources
    print("\n=== Test 3: List Resources ===")
    list_resources_msg = fixtures["list_resources"]["request"]
    if client.send_message(list_resources_msg):
        response = client.get_response()
        if response and "result" in response:
            resources = response["result"].get("resources", [])
            resource_uris = [res["uri"] for res in resources]
            expected_resources = fixtures["list_resources"]["expected_resources"]
            has_all_resources = all(res in resource_uris for res in expected_resources)
            results["list_resources"] = has_all_resources
            print(f"✓ List Resources: {'PASS' if has_all_resources else 'FAIL'}")
            print(f"  Found {len(resources)} resources")
        else:
            results["list_resources"] = False
            print("✗ List Resources: No response")
    
    # Test 4: Call check_health tool
    print("\n=== Test 4: Call check_health Tool ===")
    check_health_msg = fixtures["call_check_health"]["request"]
    if client.send_message(check_health_msg):
        response = client.get_response()
        if response and "result" in response:
            result = response["result"]
            is_valid = isinstance(result, dict) and "content" in result
            results["call_check_health"] = is_valid
            print(f"✓ Call check_health: {'PASS' if is_valid else 'FAIL'}")
            if is_valid and isinstance(result["content"], list) and len(result["content"]) > 0:
                content = result["content"][0]
                if content.get("type") == "text":
                    print(f"  Response: {content.get('text', '')[:100]}...")
        else:
            results["call_check_health"] = False
            print("✗ Call check_health: No response")
    
    # Test 5: Read config resource
    print("\n=== Test 5: Read Config Resource ===")
    read_config_msg = fixtures["read_config_resource"]["request"]
    if client.send_message(read_config_msg):
        response = client.get_response()
        if response and "result" in response:
            contents = response["result"].get("contents", [])
            if contents and isinstance(contents[0], dict):
                config_text = contents[0].get("text", "")
                try:
                    config = json.loads(config_text)
                    expected_fields = fixtures["read_config_resource"]["expected_fields"]
                    has_all_fields = all(field in config for field in expected_fields)
                    results["read_config_resource"] = has_all_fields
                    print(f"✓ Read Config: {'PASS' if has_all_fields else 'FAIL'}")
                except json.JSONDecodeError:
                    results["read_config_resource"] = False
                    print("✗ Read Config: Invalid JSON")
        else:
            results["read_config_resource"] = False
            print("✗ Read Config: No response")
    
    # Test 6: Error handling
    print("\n=== Test 6: Error Handling ===")
    invalid_msg = fixtures["invalid_method"]["request"]
    if client.send_message(invalid_msg):
        response = client.get_response()
        if response and "error" in response:
            results["error_handling"] = True
            print("✓ Error Handling: PASS")
            print(f"  Error: {response['error'].get('message', 'Unknown error')}")
        else:
            results["error_handling"] = False
            print("✗ Error Handling: No error response")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Test MCP stdio communication with Docker container")
    parser.add_argument("--image", default="bigeye-mcp-server:latest", help="Docker image to test")
    parser.add_argument("--env-file", help="Environment file to use")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--fixtures", default="tests/fixtures/mcp_messages.json", help="Test fixtures file")
    args = parser.parse_args()
    
    # Create test client
    client = MCPTestClient(args.image, args.env_file, args.debug)
    
    print(f"Starting MCP test client...")
    print(f"Docker image: {args.image}")
    
    # Start the container
    if not client.start():
        print("Failed to start container")
        sys.exit(1)
    
    try:
        # Run test sequence
        results = run_test_sequence(client, args.fixtures)
        
        # Print summary
        print("\n=== Test Summary ===")
        total_tests = len(results)
        passed_tests = sum(1 for result in results.values() if result)
        
        for test_name, passed in results.items():
            status = "PASS" if passed else "FAIL"
            symbol = "✓" if passed else "✗"
            print(f"{symbol} {test_name}: {status}")
        
        print(f"\nTotal: {passed_tests}/{total_tests} tests passed")
        
        # Check for errors
        errors = client.get_errors()
        if errors and args.debug:
            print("\n=== Stderr Output ===")
            for error in errors:
                print(error)
        
        # Exit with appropriate code
        sys.exit(0 if passed_tests == total_tests else 1)
        
    finally:
        # Stop the container
        print("\nStopping container...")
        client.stop()


if __name__ == "__main__":
    main()