from flask import Flask, request, jsonify
import  requests
import random
import argparse
import concurrent.futures

app = Flask(__name__)

class HaystackCache:
    def __init__(self):
        self.cache = {}
    
    def get_photo(self, key):
        return self.cache.get(key, None)

    def add_photo(self, key, data):
        self.cache[key] = data
        print(f"Photo with key {key} added to cache")

    def remove_photo(self, key):
        if key in self.cache:
            del self.cache[key]
            print(f"Photo with key {key} removed from cache")
        else:
            print(f"Photo with key {key} not found in cache")

@app.route('/read', methods=['GET'])
def get_photo():
    key = request.args.get('key')
    if not key:
        return jsonify({"error": "key is required"}), 400
    
    photo = cache.get_photo(key)
    if photo:
        return jsonify({'data': photo}), 200
    else:
        machine_url = request.args.get('machine_url')
        if not machine_url:
            return jsonify({"error": "need machine url to access machine"}), 400
        
        try:
            response = requests.get(f"http://{machine_url}/get", params=request.args)
            data = response.json()
            response.raise_for_status()
            photo = data['data']

            # Should add only if the photo is right enabled store
            cache.add_photo(key, photo)

            return jsonify({'data': photo}), 200

        except requests.RequestException as e:
            return jsonify({"error": str(e)}), 500

@app.route('/remove', methods=['DELETE'])
def remove_photo():
    key = request.args.get('key')
    if not key:
        return jsonify({"error": "key is required"}), 400
    
    cache.remove_photo(key)
    
    machine_urls = request.args.get('machine_urls')
    machine_urls = machine_urls.split(',')
    physical_ids = request.args.get('physical_ids')
    physical_ids = physical_ids.split(',')
    logical_id = request.args.get('logical_id')

    if len(machine_urls)!=2 and len(physical_ids)!=2:
        return jsonify({"error": "all machine_url are required"}), 400


    def send_delete_request(url, physical_id):
        try:
            response = requests.delete(f"http://{url}/remove", params={"key": key, "physical_id": physical_id, 'logical_id': logical_id})
            response.raise_for_status()
            return {"url": url, "status": "success"}
        except requests.RequestException as e:
            return {"url": url, "status": "error", "error": str(e)}

    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for url, physical_id in zip(machine_urls, physical_ids):
            futures.append(executor.submit(send_delete_request, url, physical_id))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)

    errors = [result for result in results if result["status"] == "error"]
    if len(errors):
        return jsonify({"errors": errors}), 500
    else:
        return jsonify({"message": "Photo deleted successfully!"}), 200

    
if __name__ == '__main__':
    cache = HaystackCache()
    parser = argparse.ArgumentParser(description='Run the Flask web server.')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the web server on')
    args = parser.parse_args()
    
    app.run(port=args.port)



