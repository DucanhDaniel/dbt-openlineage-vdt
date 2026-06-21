from flask import Flask, request, Response
import requests
import os
import threading
import logging
import queue

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://marquez:5000/api/v1/lineage")
OM_URL = os.environ.get("OM_URL", "http://103.102.131.30:8585/api/v1/openlineage")
OM_TOKEN = os.environ.get("OM_TOKEN")
import json
from threading import Lock

# Store the merged assertions scoped by runId
# format: { "runId::namespace::name": { "assertion_name": assertion_dict } }
assertion_cache = {}
cache_lock = Lock()

# Store the runId and job name of the main test task for each model
run_cache = {}

# Store runIds that have failed to prevent a later test from overriding a FAILED state with COMPLETE
failed_runs = set()

def merge_assertions(namespace, name, new_assertions, current_run_id):
    # Dùng runId làm một phần của key để cách ly hoàn toàn các lượt chạy song song
    key = f"{current_run_id}::{namespace}::{name}"
    with cache_lock:
        if key not in assertion_cache:
            assertion_cache[key] = {}
            
        for ass in new_assertions:
            ass_name = ass.get("assertion")
            if ass_name:
                assertion_cache[key][ass_name] = ass
                
        return list(assertion_cache[key].values())

def forward_async(data):
    # Process and merge Data Quality assertions
    try:
        event = json.loads(data)
        job_name = event.get('job', {}).get('name', 'unknown')
        
        # --- VDT FIX: Marquez runId and job_name override ---
        # Cache the main test run for each model
        if job_name.endswith('.test') and 'relationships_' not in job_name:
            model_name = job_name.split('.')[-2]
            with cache_lock:
                run_cache[model_name] = {
                    'runId': event.get('run', {}).get('runId'),
                    'job_name': job_name
                }
        # Override the relationship test run with the main test run
        elif 'relationships_' in job_name:
            with cache_lock:
                for model_name, cached_info in run_cache.items():
                    if f"relationships_{model_name}_" in job_name:
                        if 'run' in event and cached_info['runId']:
                            event['run']['runId'] = cached_info['runId']
                        if 'job' in event and cached_info['job_name']:
                            event['job']['name'] = cached_info['job_name']
                        logging.info(f"[DEBUG] Overrode relationship test {job_name} with {cached_info['job_name']}")
                        break
        # -----------------------------------------------------

        # --- VDT FIX: Prevent COMPLETE from overriding FAIL ---
        event_type = event.get('eventType')
        run_id = event.get('run', {}).get('runId')
        
        if event_type == 'FAIL' and run_id:
            with cache_lock:
                failed_runs.add(run_id)
                
        if event_type == 'COMPLETE' and run_id:
            with cache_lock:
                if run_id in failed_runs:
                    event['eventType'] = 'FAIL'
                    logging.info(f"[DEBUG] Changed COMPLETE to FAIL for runId {run_id} because a previous test failed")
        # -----------------------------------------------------
        
        # Dump incoming event for debugging
        with open(f"/app/event_{job_name}.json", "w") as f:
            json.dump(event, f, indent=2)

        for ds_type in ['inputs', 'outputs']:
            for ds in event.get(ds_type, []):
                namespace = ds.get('namespace', '')
                name = ds.get('name', '')
                
                # Check normal facets
                facets = ds.get('facets', {})
                if 'dataQualityAssertions' in facets:
                    new_assertions = facets['dataQualityAssertions'].get('assertions', [])
                    if new_assertions:
                        full_assertions = merge_assertions(namespace, name, new_assertions, run_id)
                        ds['facets']['dataQualityAssertions']['assertions'] = full_assertions
                        
                # Check inputFacets (often used for Data Quality in OpenLineage)
                input_facets = ds.get('inputFacets', {})
                if 'dataQualityAssertions' in input_facets:
                    new_assertions = input_facets['dataQualityAssertions'].get('assertions', [])
                    if new_assertions:
                        full_assertions = merge_assertions(namespace, name, new_assertions, run_id)
                        ds['inputFacets']['dataQualityAssertions']['assertions'] = full_assertions
                        
        modified_data = json.dumps(event).encode('utf-8')
        
        # Dump modified event for debugging
        with open(f"/app/modified_{job_name}.json", "w") as f:
            json.dump(json.loads(modified_data.decode('utf-8')), f, indent=2)
            
    except Exception as e:
        logging.error(f"Failed to merge facets: {e}")
        modified_data = data

    # Forward to Marquez
    try:
        requests.post(MARQUEZ_URL, data=modified_data, headers={'Content-Type': 'application/json'}, timeout=5)
        logging.info("Successfully sent OpenLineage event to Marquez")
    except Exception as e:
        logging.error(f"Marquez Error: {e}")
        
    # Forward to OpenMetadata
    if OM_URL:
        try:
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {OM_TOKEN}'
            }
            requests.post(OM_URL, data=modified_data, headers=headers, timeout=5)
            logging.info("Successfully sent OpenLineage event to OpenMetadata")
        except Exception as e:
            logging.error(f"OpenMetadata Error: {e}")

# Event queue for processing requests sequentially to prevent race conditions
event_queue = queue.Queue()

def worker():
    while True:
        data = event_queue.get()
        try:
            forward_async(data)
        except Exception as e:
            logging.error(f"Worker Error: {e}")
        finally:
            event_queue.task_done()

# Start a single worker thread to maintain event order
worker_thread = threading.Thread(target=worker, daemon=True)
worker_thread.start()

@app.route('/', defaults={'path': ''}, methods=['POST'])
@app.route('/<path:path>', methods=['POST'])
def proxy(path):
    data = request.get_data()
    # Put event in queue to maintain order and unblock Airflow immediately
    event_queue.put(data)
    return Response("OK", status=200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
