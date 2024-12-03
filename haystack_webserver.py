from flask import Flask, request, jsonify
import requests
import random
import concurrent.futures

app = Flask(__name__)

DIRECTORY_SERVICE_URLS = [
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5003",
    "http://localhost:5004",
    "http://localhost:5005"
]

CACHE_SERVERS_URLS = {
    0: "http://localhost:6001",
    1: "http://localhost:6002",
    2: "http://localhost:6003"
}

MACHINE_URLS = {
    0: "192.168.67.83:7000",
    1: "192.168.67.2:7000"
}

current_directory_index = 0

def get_next_directory_url():
    global current_directory_index
    url = DIRECTORY_SERVICE_URLS[current_directory_index]
    current_directory_index = (current_directory_index + 1) % len(DIRECTORY_SERVICE_URLS)
    return url



@app.route('/read', methods=['GET'])
def read_request():
    photo_id = request.args.get('photo_id')

    directory_url = get_next_directory_url()

    try:
        response = requests.get(f"{directory_url}/read", params={"photo_id": photo_id})
        response.raise_for_status()
        directory_response = response.json()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    cache_id = directory_response.get('cache_id')
    logical_id = directory_response.get('logical_id')
    physical_id = directory_response.get('physical_id')
    machine_id = directory_response.get('machine_id')

    if machine_id is None or machine_id not in MACHINE_URLS.keys():
        return jsonify({"error": "Machine ID is not valid"}), 400

    machine_url = MACHINE_URLS[machine_id]

    if cache_id is not None and cache_id in CACHE_SERVERS_URLS.keys():
        cache_url = CACHE_SERVERS_URLS[cache_id]
        try:
            cache_response = requests.get(f"{cache_url}/read", params={"key": photo_id, "logical_id": logical_id, "physical_id": physical_id, "machine_url": machine_url})
            cache_response.raise_for_status()
            cache_data = cache_response.json()
            return jsonify(cache_data['data'])
        except requests.RequestException as e:
            return jsonify({"error": str(e)}), 500
        
    return jsonify({"error": "Cache ID not found"}), 404

@app.route('/read_similar', methods=['GET'])
def read_similar_request():
    photo_id = request.args.get('photo_id')
    num_of_similar = request.args.get('num_of_similar')

    directory_url = get_next_directory_url()

    try:
        response = requests.get(f"{directory_url}/read", params={"photo_id": photo_id})
        response.raise_for_status()
        directory_response = response.json()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    cache_id = directory_response.get('cache_id')
    logical_id = directory_response.get('logical_id')
    physical_id = directory_response.get('physical_id')
    machine_id = directory_response.get('machine_id')

    if machine_id is None or machine_id not in MACHINE_URLS.keys():
        return jsonify({"error": "Machine ID is not valid"}), 400

    machine_url = MACHINE_URLS[machine_id]

    try:
        response = requests.get(f"http://{machine_url}/get_similar", params={"key": photo_id, "logical_id": logical_id, "physical_id": physical_id, "machine_url": machine_url, "num_of_similar": num_of_similar})
        response.raise_for_status()
        return jsonify(response.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


@app.route('/delete', methods=['DELETE'])
def delete_request():
    photo_id = request.args.get('photo_id')
    

    directory_url = get_next_directory_url()

    try:
        response = requests.delete(f"{directory_url}/delete", params={"photo_id": photo_id})
        response.raise_for_status()
        directory_response = response.json()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    cache_id = directory_response.get('cache_id')
    logical_id = directory_response.get('logical_id')
    physical_ids = directory_response.get('physical_ids')
    machine_ids = directory_response.get('machine_ids')

    machine_urls = []

    for machine_id in machine_ids:
        if machine_id is None or machine_id not in MACHINE_URLS.keys():
            return jsonify({"error": "Machine ID is not valid"}), 400
        
        machine_urls.append(MACHINE_URLS[machine_id])

    print(machine_urls)

    if cache_id is not None and cache_id in CACHE_SERVERS_URLS.keys():
        cache_url = CACHE_SERVERS_URLS[cache_id]
        try:
            physical_ids_str = ','.join(map(str, physical_ids))
            machine_urls_str = ','.join(machine_urls)
            cache_response = requests.delete(f"{cache_url}/remove", params={"key": photo_id, 'logical_id': logical_id, 'physical_ids': physical_ids_str, 'machine_urls': machine_urls_str})
            cache_response.raise_for_status()
        except requests.RequestException as e:
            return jsonify({"error": str(e)}), 500

    return jsonify({'message': 'Photo deleted successfully'})

@app.route('/write', methods=['POST'])
def write_request():
    data = request.json
    if 'photo_data' not in data:
        return jsonify({"error": "photo_data are required"}), 400

    photo_data = data['photo_data']

    initial_directory_url = get_next_directory_url()

    try:
        initial_response = requests.post(f"{initial_directory_url}/get_features_along_other_details", json={'photo_data': photo_data})
        initial_response.raise_for_status()
        initial_result = initial_response.json()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    photo_ids = initial_result['list_of_photo_ids']
    features = initial_result['features']
    actual_id = initial_result['photo_id']

    print(f"Photo ID: {actual_id}")

    if len(photo_ids) >= 20:
        random.shuffle(photo_ids)

        num_directories = len(DIRECTORY_SERVICE_URLS) - 1
        chunk_size = len(photo_ids) // num_directories
        chunks = [photo_ids[i * chunk_size: (i + 1) * chunk_size] for i in range(num_directories)]

        if len(photo_ids) % num_directories != 0:
            chunks[-1].extend(photo_ids[num_directories * chunk_size:])

        combined_result = {
            'nearest_photos_ids' : [],
            'features': features, 
            'photo_id': actual_id,
        }

        def fetch_nearest(directory_url, chunk_photo_ids):
            other_data = {'photo_ids': chunk_photo_ids, 'features': features, 'actual_id': actual_id}
            print(f"Fetching nearest photos from {directory_url}")
            response = requests.post(f"{directory_url}/compute_nearest", json=other_data)
            response.raise_for_status()
            return response.json()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_url = {}
            for directory_url in DIRECTORY_SERVICE_URLS:
                if directory_url == initial_directory_url:
                    continue
                chunk_photo_ids = chunks.pop(0)
                future = executor.submit(fetch_nearest, directory_url, chunk_photo_ids)
                future_to_url[future] = directory_url

            for future in concurrent.futures.as_completed(future_to_url):
                directory_url = future_to_url[future]
                try:
                    result = future.result()
                    combined_result['nearest_photos_ids'].append(result['nearest_photos_id'])
                except Exception as e:
                    return jsonify({"error": str(e)}), 500 
                
    else:
        combined_result = {
            'nearest_photos_ids': [],
            'features': features,
            'photo_id': actual_id
        }

        combined_result['nearest_photos_ids'] = [None for _ in range(len(photo_ids))]

    try:
        final_response = requests.post(f"{initial_directory_url}/write_combined", json=combined_result)
        final_response.raise_for_status()
        final_result = final_response.json()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    physical_ids = final_result['physical_ids']
    logical_id = final_result['logical_id']
    machine_ids = final_result['machine_ids']

    def send_update(direcroty_url):
        response = requests.post(f"{direcroty_url}/update_volume", json={'photo_id': actual_id, 'logical_id':logical_id, 'features': features})
        response.raise_for_status()
        return response.json()
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {}
        for directory_url in DIRECTORY_SERVICE_URLS:
            if directory_url == initial_directory_url:
                continue
            future = executor.submit(send_update, directory_url)
            future_to_url[future] = directory_url

        for future in concurrent.futures.as_completed(future_to_url):
            directory_url = future_to_url[future]
            try:
                result = future.result()
            except Exception as e:
                return jsonify({"error": str(e)}), 500

    def send_request(machine_url, physical_id):
        payload = {
            'photo_data': photo_data,
            'logical_id': str(logical_id),
            'physical_id': str(physical_id),
            'photo_id': str(actual_id)
        }
        try:
            print(f"sending to store {machine_url}")
            machine_response = requests.post(f"http://{machine_url}/write", json=payload)
            machine_response.raise_for_status()
            return {"url": machine_url, "status": "success"}
        except requests.RequestException as e:
            print(f"Error sending request to {machine_url}: {e}")
            return {"url": machine_url, "status": "error", "error": str(e)}

    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for physical_id, machine_id in zip(physical_ids, machine_ids):
            machine_url = MACHINE_URLS[machine_id]
            if machine_url:
                futures.append(executor.submit(send_request, machine_url, physical_id))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)

    errors = [result for result in results if result["status"] == "error"]
    if errors or len(results)!=2:
        return jsonify({"errors": errors}), 500

    return jsonify({'message': 'Photo written successfully'})

@app.route('/write_batch', methods=['POST'])
def write_batch_request():
    data = request.json
    if 'photo_data' not in data:
        return jsonify({"error": "photo_data are required"}), 400

    photo_data = data['photo_data']

    initial_directory_url = get_next_directory_url()

    try:
        initial_response = requests.post(f"{initial_directory_url}/get_features_along_other_details_batch", json={'photo_data': photo_data})
        initial_response.raise_for_status()
        initial_result = initial_response.json()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    photo_ids = initial_result['list_of_photo_ids']
    features = initial_result['features']
    actual_id = initial_result['photo_id']

    print(f"Photo ID: {actual_id} to {actual_id + 15}")

    random.shuffle(photo_ids)

    num_directories = len(DIRECTORY_SERVICE_URLS) - 1
    chunk_size = len(photo_ids) // num_directories
    chunks = [photo_ids[i * chunk_size: (i + 1) * chunk_size] for i in range(num_directories)]

    if len(photo_ids) % num_directories != 0:
        chunks[-1].extend(photo_ids[num_directories * chunk_size:])

    combined_result = {
        'nearest_photos_ids' : [[] for _ in range(16)],
        'features': features, 
        'photo_id': actual_id,
    }

    def fetch_nearest(directory_url, chunk_photo_ids):
        other_data = {'photo_ids': chunk_photo_ids, 'features': features, 'actual_id': actual_id}
        print(f"Fetching nearest photos from {directory_url}")
        response = requests.post(f"{directory_url}/compute_nearest_batch", json=other_data)
        response.raise_for_status()
        return response.json()
    
    results = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {}
        for directory_url in DIRECTORY_SERVICE_URLS:
            if directory_url == initial_directory_url:
                continue
            chunk_photo_ids = chunks.pop(0)
            future = executor.submit(fetch_nearest, directory_url, chunk_photo_ids)
            future_to_url[future] = directory_url

        for future in concurrent.futures.as_completed(future_to_url):
            directory_url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                return jsonify({"error": str(e)}), 500 
            
    for result in results:
        for i in range(16):
            combined_result['nearest_photos_ids'][i].append(result['nearest_photos_id'][i])
    
    try:
        final_response = requests.post(f"{initial_directory_url}/write_combined_batch", json=combined_result)
        final_response.raise_for_status()
        final_result = final_response.json()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    physical_ids = final_result['physical_ids']
    logical_id = final_result['logical_id']
    machine_ids = final_result['machine_ids']

    def send_update(direcroty_url):
        response = requests.post(f"{direcroty_url}/update_volume_batch", json={'photo_id': actual_id, 'logical_id':logical_id, 'features': features})
        response.raise_for_status()
        return response.json()
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {}
        for directory_url in DIRECTORY_SERVICE_URLS:
            if directory_url == initial_directory_url:
                continue
            future = executor.submit(send_update, directory_url)
            future_to_url[future] = directory_url

        for future in concurrent.futures.as_completed(future_to_url):
            directory_url = future_to_url[future]
            try:
                result = future.result()
            except Exception as e:
                return jsonify({"error": str(e)}), 500

    def send_request(machine_url, physical_id, photo_data, logical_id, actual_id):
        payload = {
            'photo_data': photo_data,
            'logical_id': str(logical_id),
            'physical_id': str(physical_id),
            'photo_id': str(actual_id)
        }
        try:
            print(f"sending to store {machine_url}")
            machine_response = requests.post(f"http://{machine_url}/write", json=payload)
            machine_response.raise_for_status()
            return {"url": machine_url, "status": "success"}
        except requests.RequestException as e:
            print(f"Error sending request to {machine_url}: {e}")
            return {"url": machine_url, "status": "error", "error": str(e)}

    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for i in range(16):
            photo_data_temp = photo_data[i]
            logical_id_temp = logical_id[i]
            physical_id_temp = physical_ids[i]
            machine_id_temp = machine_ids[i]
            actual_id_temp = actual_id + i

            for physical_id_f, machine_id_f in zip(physical_id_temp, machine_id_temp):
                machine_url_f = MACHINE_URLS[machine_id_f]
                if machine_url_f:
                    futures.append(executor.submit(send_request, machine_url_f, physical_id_f, photo_data_temp, logical_id_temp, actual_id_temp))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)

    errors = [result for result in results if result["status"] == "error"]
    if errors or len(results)!=2*16:
        return jsonify({"errors": errors}), 500

    return jsonify({'message': 'Photo written successfully'})



if __name__ == '__main__':
    app.run(port=8000)