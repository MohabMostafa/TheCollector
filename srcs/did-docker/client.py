import requests
import time
import sys
import argparse
import json

BASE_URL = "http://localhost:3003"

def check_health(wait_for_model=False, timeout=300):
    """
    Check if the server is healthy and the model is loaded.
    
    Args:
        wait_for_model: If True, wait until the model is fully loaded.
        timeout: Maximum seconds to wait for model loading.
    
    Returns:
        Boolean indicating server health.
    """
    start_time = time.time()
    while True:
        try:
            response = requests.get(f"{BASE_URL}/health", timeout=10)
            response_data = response.json()
            
            if response.status_code == 200:
                if wait_for_model and response_data.get("status") == "initializing":
                    elapsed = time.time() - start_time
                    if elapsed > timeout:
                        print(f"Timeout waiting for model initialization ({timeout} seconds)")
                        return False
                    
                    print(f"Model is still initializing. Waiting... ({elapsed:.1f}s)")
                    time.sleep(5)
                    continue
                
                model_loaded = response_data.get('model_loaded', False)
                print(f"Server is healthy! Model loaded: {model_loaded}")
                
                if wait_for_model and not model_loaded:
                    elapsed = time.time() - start_time
                    if elapsed > timeout:
                        print(f"Timeout waiting for model initialization ({timeout} seconds)")
                        return False
                    print(f"Waiting for model to load... ({elapsed:.1f}s)")
                    time.sleep(5)
                    continue
                
                return True
            else:
                print(f"Server health check failed with status code: {response.status_code}")
                if wait_for_model:
                    elapsed = time.time() - start_time
                    if elapsed > timeout:
                        print(f"Timeout waiting for server ({timeout} seconds)")
                        return False
                    print(f"Retrying in 5 seconds... ({elapsed:.1f}s)")
                    time.sleep(5)
                    continue
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to server: {e}")
            elapsed = time.time() - start_time
            if elapsed > timeout:
                print(f"Timeout waiting for server connection ({timeout} seconds)")
                return False
            
            print(f"Retrying connection in 5 seconds... ({elapsed:.1f}s)")
            time.sleep(5)

def start_processing():
    try:
        response = requests.post(f"{BASE_URL}/process", timeout=30)
        data = response.json()
        print(f"Process start response: {json.dumps(data, indent=2)}")
        return data
    except Exception as e:
        print(f"Error starting processing: {e}")
        return None

def check_status():
    try:
        response = requests.get(f"{BASE_URL}/status", timeout=30)
        return response.json()
    except Exception as e:
        print(f"Error checking status: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Dialect Detection Client")
    parser.add_argument("--wait", action="store_true", help="Wait for model initialization")
    parser.add_argument("--timeout", type=int, default=600, help="Timeout in seconds")
    parser.add_argument("--skip-process", action="store_true", help="Skip processing, just check status")
    args = parser.parse_args()
    
    print("Dialect Detection Client")
    
    if not check_health(wait_for_model=args.wait, timeout=args.timeout):
        print("Server is not healthy or timeout reached. Exiting.")
        sys.exit(1)
    
    if not args.skip_process:
        print("\nStarting dialect processing...")
        result = start_processing()
        if result and result.get("status") == "error":
            print(f"Error: {result.get('message')}")
            sys.exit(1)
    else:
        print("\nSkipping processing, checking status only...")
    
    print("\nChecking processing status...")
    while True:
        status = check_status()
        
        if status is None:
            print("Error checking status. Exiting.")
            break
            
        if status.get("status") == "processing":
            print("Processing is still running. Waiting 5 seconds...")
            time.sleep(5)
        else:
            print("\nProcessing completed!")
            print("Final status:")
            for key, value in status.items():
                if key == "results" and isinstance(value, list):
                    print(f"\nProcessed {len(value)} files:")
                    # Count as success if no "status" key is present or if it's explicitly "success"
                    success_count = sum(1 for r in value if r.get("status", "success") == "success")
                    error_count = sum(1 for r in value if r.get("status", "success") != "success")
                    print(f"  - Success: {success_count}")
                    print(f"  - Errors: {error_count}")
                else:
                    print(f"{key}: {value}")
            break
    
if __name__ == "__main__":
    main()
