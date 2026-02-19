
import sys
import os
sys.path.insert(0, os.getcwd())

try:
    print("Importing domain.meta...")
    import domain.meta
    print("Imported domain.meta successfully.")
    
    from domain.meta import Asset, AssetProfile
    print(f"Asset: {Asset}")
    print(f"AssetProfile: {AssetProfile}")
    
    print("Checking relationships...")
    # Trigger mapper init
    from sqlmodel import create_engine, select, Session
    engine = create_engine("sqlite:///:memory:")
    try:
        with Session(engine) as session:
            session.exec(select(Asset))
        print("Mapper init successful.")
    except Exception as e:
        print(f"Mapper init failed: {e}")

except Exception as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
