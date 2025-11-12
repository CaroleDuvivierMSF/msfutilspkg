import json
import inspect
import importlib

# === CONFIG ===
UDF_FUNCTION = "msfutilspkg.utils.sync_dataframes_with_old_new"  # module.path.to.function
DISPLAY_NAME = "sync_dataframes_with_old_new"
DESCRIPTION = "Synchronize two DataFrames and detect record-level changes."
OUTPUT_FILE = "udf_payload.json"
# ==============

module_name, func_name = UDF_FUNCTION.rsplit(".", 1)
module = importlib.import_module(module_name)
func = getattr(module, func_name)

script_source = inspect.getsource(func)

payload = {
    "displayName": DISPLAY_NAME,
    "description": DESCRIPTION,
    "scriptContent": script_source,
    "language": "python",
    "functionType": "UserDataFunction"
}

with open(OUTPUT_FILE, "w") as f:
    json.dump(payload, f, indent=2)

print(f"✅ UDF payload written to {OUTPUT_FILE}")
