import os
from supabase import create_client, Client
from dotenv import load_dotenv

def get_supabase_client() -> Client:
    """Initializes and returns the Supabase client using credentials from environment variables."""
    print("Attempting to load Supabase credentials from environment/dotenv...")
    load_dotenv() # Load variables from .env

    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in environment or .env file.")
        # Check if placeholders are still present
        if os.environ.get("SUPABASE_URL") == "YOUR_SUPABASE_PROJECT_URL" or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") == "YOUR_SUPABASE_SERVICE_ROLE_KEY":
             print("Error: Please replace placeholder values in your .env file with actual Supabase credentials.")

        raise ValueError("Supabase URL and Service Role Key must be set.")

    print("Supabase credentials loaded. Initializing client...")
    try:
        # Use the service_role key for operations that require bypassing RLS (like inserts)
        supabase: Client = create_client(url, key)
        print("Supabase client initialized successfully.")
        return supabase
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        return None

# Example usage (can be removed or kept for testing)
if __name__ == "__main__":
    print("--- Testing Supabase Client Initialization ---")
    try:
        supabase_client = get_supabase_client()
        if supabase_client:
            print("Client initialization test successful.")
            # You could add a test query here if needed, e.g.,
            # try:
            #     response = supabase_client.table("cot_reports").select("*").limit(1).execute()
            #     print("Test query successful:", response.data)
            # except Exception as e:
            #     print(f"Test query failed: {e}")
        else:
            print("Client initialization test failed.")
    except ValueError as ve:
        print(f"Initialization failed due to credential error: {ve}")
    print("--- Supabase Client Initialization Test Finished ---")
