"""
TerminusDB Connection Test Script
=================================
Tests connectivity, schema creation, data insertion, and querying.
"""

from terminusdb_client import WOQLClient, WOQLQuery, GraphType

def test_connection():
    # 1. Configuration (Matches your docker-compose environment variables)
    server_url = "http://localhost:6363"
    db_id = "test_graph_db"
    user = "admin"
    password = "root"  # Default password from your docker-compose.yml
    team = "admin"     # Default team is usually 'admin'

    print(f"🔌 Connecting to TerminusDB at {server_url}...")
    
    try:
        client = WOQLClient(server_url)
        client.connect(user=user, password=password, team=team)
        print("✅ Connection Successful!")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 2. Create Database
    print(f"\n🔨 Creating database '{db_id}'...")
    try:
        # Try to delete it first to ensure a clean slate. 
        # We ignore errors here in case it didn't exist yet.
        try:
            client.delete_database(db_id)
            print(f"   (Cleaned up old version of '{db_id}')")
        except Exception:
            pass # It didn't exist, which is fine

        # Now create it fresh
        client.create_database(db_id, label="Test Graph DB", description="A temporary DB for testing connection")
        print(f"✅ Database '{db_id}' created.")
        
    except Exception as e:
        print(f"❌ Database creation failed: {e}")
        return

    # 3. Create Schema (Simple Social Graph)
    print("\n📝 Defining Schema (Person, Knows)...")
    schema = {
        "@type": "Class",
        "@id": "Person",
        "name": "xsd:string",
        "knows": {
            "@type": "Set",
            "@class": "Person"
        }
    }
    
    try:
        client.insert_document(schema, graph_type=GraphType.SCHEMA)
        print("✅ Schema inserted.")
    except Exception as e:
        print(f"❌ Schema creation failed: {e}")
        return

    # 4. Insert Data (Alice knows Bob)
    print("\n💾 Inserting Data (Nodes & Edges)...")
    data = [
        {
            "@type": "Person",
            "@id": "Person/alice",
            "name": "Alice",
            "knows": ["Person/bob"] 
        },
        {
            "@type": "Person",
            "@id": "Person/bob",
            "name": "Bob"
        }
    ]

    try:
        client.insert_document(data)
        print("✅ Data inserted successfully.")
    except Exception as e:
        print(f"❌ Data insertion failed: {e}")
        return

    # 5. Query Data (Verify)
    print("\n🔎 Querying Graph...")
    try:
        # Find who Alice knows
        # Query: Triple("Person/alice", "knows", "v:Friend")
        import json
        result = client.query(
            WOQLQuery().triple("Person/alice", "knows", "v:Friend")
        )
        
        result = json.loads(result) if isinstance(result, str) else result
        bindings = result.get('bindings', [])
        
        if bindings:
            friend_id = bindings[0]['Friend']
            print(f"✅ Query Successful! Found that Alice knows: {friend_id}")
        else:
            print("⚠️ Query ran but returned no results.")
            
    except Exception as e:
        print(f"❌ Query failed: {e}")

if __name__ == "__main__":
    test_connection()