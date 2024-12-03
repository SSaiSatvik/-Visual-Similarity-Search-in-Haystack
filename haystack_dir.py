from flask import Flask, request, jsonify
import random
import numpy as np
import faiss
import os
import argparse
import json
from tensorflow.keras.models import load_model

app = Flask(__name__)

class HaystackDirectory:
    def __init__(self):
        self.logical_id_to_physical_id = {0: [], 1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: [], 9: [], 10: [], 11: [], 12: [], 13: [], 14: [], 15: [], 16: [], 17: [], 18: [], 19: []} 

        for i in range(40):
            self.logical_id_to_physical_id[int(i/2)].append(i) 

        self.write_enabled_volumes_id = set() 

        for i in range(20):
            self.write_enabled_volumes_id.add(i) 

        self.photo_id_to_logical_volume_id = {}
        self.photo_id_to_features = {}
        self.physical_id_to_machine_id = {}

        for i in range(40):
            self.physical_id_to_machine_id[i] = (i%2)

        self.photo_id_counter = 0

        model_filename = 'mobilenetv2_embeddings_model.h5'
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        model_path = os.path.join(current_dir, model_filename)
        print(f"Checking for model at {model_path}")
        
        if os.path.exists(model_path):
            try:
                self.model = load_model(model_path)
                print(f"Model loaded from {model_path}")
            except Exception as e:
                print(f"Failed to load model from {model_path}: {e}")
        else:
            print(f"Model file does not exist at {model_path}")
        

    def hash_function(self, photo_id):
        return int(photo_id) % 3

    def add_mapping(self, logical_id, physical_id):
        if logical_id not in self.logical_id_to_physical_id:
            self.logical_id_to_physical_id[logical_id] = []
        self.logical_id_to_physical_id[logical_id].append(physical_id)
        self.write_enabled_volumes.add(logical_id)
        print(f"Mapping added: logical volume {logical_id} to physical volume {physical_id}")

    def mark_volume_read_only(self, logical_id):
        if logical_id in self.write_enabled_volumes:
            self.write_enabled_volumes.remove(logical_id)
            print(f"Volume {logical_id} marked as read-only")

    def compute_features_for_photo(self, photo):
        photo_array = np.array(photo)
        photo_array = np.resize(photo_array, (1, 224, 224, 3))
        try:
            prediction = self.model.predict(photo_array)
            print(f"Prediction done")
        except Exception as e:
            print(f"Error during prediction: {e}")
        return prediction
    
    def compute_features_for_photo_batch(self, photos):
        photo_array = np.array(photos)
        photo_array = np.resize(photo_array, (16, 224, 224, 3))
        try:
            prediction = self.model.predict(photo_array)
            print(f"Prediction done")
        except Exception as e:
            print(f"Error during prediction: {e}")
        return prediction

    def nearest_photos_features(self, photo_ids, feature):
        features = []

        for photo_id in photo_ids:
            features.append(self.photo_id_to_features[photo_id])
        features = np.array(features)
        features = features.astype('float32')
        features = features.reshape((len(features), 64))

        index = faiss.IndexFlatL2(64)
        index.add(features)

        query_feature = np.array([feature], dtype='float32').reshape(1, 64)

        distances, indices = index.search(query_feature, k=1)

        near = photo_ids[indices[0][0]]
        nearest_photos_feature=self.photo_id_to_features[near]
        nearest_photos_id=near
        
        return nearest_photos_feature, nearest_photos_id
    
    def nearest_photos_features_batch(self, photo_ids, feature):
        features = []

        for photo_id in photo_ids:
            features.append(self.photo_id_to_features[photo_id])
        features = np.array(features)
        features = features.astype('float32')
        features = features.reshape((len(features), 64))

        index = faiss.IndexFlatL2(64)
        index.add(features)

        query_feature = np.array([feature], dtype='float32').reshape(16, 64)

        distances, indices = index.search(query_feature, k=1)

        nearest_photos_features = []
        nearest_photos_ids = []

        for i in range(16):
            near = photo_ids[indices[i][0]]
            nearest_photos_feature=self.photo_id_to_features[near]
            nearest_photos_id=near

            nearest_photos_features.append(nearest_photos_feature)
            nearest_photos_ids.append(nearest_photos_id)
        
        return nearest_photos_features, nearest_photos_ids
    
    def nearest_photos_features_batch_diff(self, photo_ids, feature):
        nearest_photos_features = []
        nearest_photos_ids = []

        for i in range(16):
            temp_1, temp_2 = self.nearest_photos_features(photo_ids[i], feature[i])
            nearest_photos_features.append(temp_1)
            nearest_photos_ids.append(temp_2)

        return nearest_photos_features, nearest_photos_ids

    
@app.route('/read', methods=['GET'])
def read_request():
    photo_id = int(request.args.get('photo_id'))

    logical_id = directory.photo_id_to_logical_volume_id[photo_id]
    
    physical_ids = directory.logical_id_to_physical_id[logical_id]
    
    physical_id = random.choice(physical_ids)
    cache_id = directory.hash_function(photo_id)

    if physical_id not in directory.physical_id_to_machine_id:
        return jsonify({"error": "No machine found for the given physical ID"}), 404

    machine_id = directory.physical_id_to_machine_id[physical_id]
    
    return jsonify({"logical_id": logical_id, "physical_id": physical_id, "cache_id": cache_id, "machine_id": machine_id})

@app.route('/delete', methods=['DELETE'])
def delete_request():
    photo_id = int(request.args.get('photo_id'))

    if photo_id not in directory.photo_id_to_logical_volume_id.keys():
        return jsonify({"error": "photo_id not found"}), 404

    logical_id = directory.photo_id_to_logical_volume_id[photo_id]

    del directory.photo_id_to_logical_volume_id[photo_id]
    del directory.photo_id_to_features[photo_id]

    physical_ids = directory.logical_id_to_physical_id[logical_id]

    machine_ids = []

    for m_id in physical_ids:
        machine_ids.append(directory.physical_id_to_machine_id[m_id])

    if len(machine_ids)==0:
        return jsonify({"error": "No physical volumes found for the given logical ID"}), 404
    
    cache_id = directory.hash_function(photo_id)

    return jsonify({"logical_id": logical_id, "physical_ids": physical_ids, "cache_id": cache_id, "machine_ids": machine_ids})

@app.route('/get_features_along_other_details', methods=['POST'])
def get_features_along_other_details():
    try:
        data = request.json
        photo_data = data['photo_data']
        if not photo_data:
            return jsonify({"error": "photo_data is required"}), 400

        photo_features = directory.compute_features_for_photo(photo_data)
        
        photo_id = directory.photo_id_counter
        directory.photo_id_counter += 1

        list_of_photo_ids = list(directory.photo_id_to_features.keys())

        directory.photo_id_to_features[photo_id] = photo_features

        photo_features = photo_features.tolist()

        result = {
            'features': photo_features,
            'list_of_photo_ids': list_of_photo_ids,
            'photo_id': photo_id
        }

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/get_features_along_other_details_batch', methods=['POST'])
def get_features_along_other_details_batch():
    try:
        data = request.json
        photo_data = data['photo_data']
        if not photo_data:
            return jsonify({"error": "photo_data is required"}), 400

        photo_features = directory.compute_features_for_photo_batch(photo_data)
        
        photo_id = directory.photo_id_counter
        directory.photo_id_counter += 16

        list_of_photo_ids = list(directory.photo_id_to_features.keys())

        for i in range(16):
            directory.photo_id_to_features[photo_id+i] = photo_features[i]

        photo_features = photo_features.tolist()

        result = {
            'features': photo_features,
            'list_of_photo_ids': list_of_photo_ids,
            'photo_id': photo_id
        }

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/compute_nearest', methods=['POST'])
def compute_nearest():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "other_data is required"}), 400

        if isinstance(data, str):
            data = json.loads(data)

        if not isinstance(data, dict):
            return jsonify({"error": "data must be a dictionary"}), 400
        
        directory.photo_id_to_features[data['actual_id']] = data['features']

        nearest_photos_feature, nearest_photos_id = directory.nearest_photos_features(data['photo_ids'], data['features'])

        result ={'nearest_photos_id': nearest_photos_id}
        return jsonify(result), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/compute_nearest_batch', methods=['POST'])
def compute_nearest_batch():
    try:

        data = request.get_json()

        if not data:
            return jsonify({"error": "other_data is required"}), 400

        if isinstance(data, str):
            data = json.loads(data)

        if not isinstance(data, dict):
            return jsonify({"error": "data must be a dictionary"}), 400

        for i in range(16):
            directory.photo_id_to_features[data['actual_id']+i] = data['features'][i]

        nearest_photos_feature, nearest_photos_id = directory.nearest_photos_features_batch(data['photo_ids'], data['features'])

        result ={'nearest_photos_id': nearest_photos_id}
        return jsonify(result), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/write_combined', methods=['POST'])
def write_combined_request():
    data = request.get_json()

    if not data:
        return jsonify({"error": "other_data is required"}), 400

    if isinstance(data, str):
        data = json.loads(data)

    if not isinstance(data, dict):
        return jsonify({"error": "data must be a dictionary"}), 400

    possible_ids = []

    print(data['nearest_photos_ids'])

    flag=1

    for id in data['nearest_photos_ids']:
        if id is None:
            flag=0
        
    if flag==1:    
        for id in data['nearest_photos_ids']:
            log_vol = directory.photo_id_to_logical_volume_id[id]

            if log_vol in directory.write_enabled_volumes_id:
                possible_ids.append(id)

        if len(possible_ids)==0:
            logical_volume = random.choice(list(directory.write_enabled_volumes_id))
        else:
            _, final_photo_id = directory.nearest_photos_features(possible_ids, data['features'])

            logical_volume = directory.photo_id_to_logical_volume_id[final_photo_id]
    else:
        empty_keys = set(directory.write_enabled_volumes_id)
        for id, value in directory.photo_id_to_logical_volume_id.items():
            if value in empty_keys:
                empty_keys.discard(value)

        empty_keys = list(empty_keys)

        logical_volume = random.choice(empty_keys)

    directory.photo_id_to_logical_volume_id[data['photo_id']] = logical_volume

    machine_ids = []

    physical_ids = list(directory.logical_id_to_physical_id[logical_volume])
    for p_id in physical_ids:
        machine_ids.append(directory.physical_id_to_machine_id[p_id])

    result = {
        'logical_id': logical_volume,
        'physical_ids': physical_ids,
        'machine_ids': machine_ids
    }


    return jsonify(result)

@app.route('/write_combined_batch', methods=['POST'])
def write_combined_batch_request():
    data = request.get_json()

    if not data:
        return jsonify({"error": "other_data is required"}), 400

    if isinstance(data, str):
        data = json.loads(data)

    if not isinstance(data, dict):
        return jsonify({"error": "data must be a dictionary"}), 400

    physical_ids_ans = []
    machine_ids_ans = []
    logical_ids_ans = []

    _, final_photo_id = directory.nearest_photos_features_batch_diff(data['nearest_photos_ids'], data['features'])

    print(final_photo_id)

    for i in range(16):
        logical_volume = directory.photo_id_to_logical_volume_id[final_photo_id[i]]
        directory.photo_id_to_logical_volume_id[data['photo_id']+i] = logical_volume
        logical_ids_ans.append(logical_volume)

        machine_ids = []

        physical_ids = list(directory.logical_id_to_physical_id[logical_volume])
        for p_id in physical_ids:
            machine_ids.append(directory.physical_id_to_machine_id[p_id])

        physical_ids_ans.append(physical_ids)
        machine_ids_ans.append(machine_ids)

    result = {
        'logical_id': logical_ids_ans,
        'physical_ids': physical_ids_ans,
        'machine_ids': machine_ids_ans
    }


    return jsonify(result)

@app.route('/update_volume', methods=['POST'])
def update_volume():
    data = request.get_json()

    if not data:
        return jsonify({"error": "other_data is required"}), 400

    if isinstance(data, str):
        data = json.loads(data)

    if not isinstance(data, dict):
        return jsonify({"error": "data must be a dictionary"}), 400

    logical_id = data['logical_id']
    photo_id = data['photo_id']

    directory.photo_id_to_logical_volume_id[photo_id] = logical_id
    directory.photo_id_to_features[photo_id] = data['features']
    directory.photo_id_counter += 1

    return jsonify({'message': 'Mapping added successfully'}), 200

@app.route('/update_volume_batch', methods=['POST'])
def update_volume_batch():
    data = request.get_json()

    if not data:
        return jsonify({"error": "other_data is required"}), 400

    if isinstance(data, str):
        data = json.loads(data)

    if not isinstance(data, dict):
        return jsonify({"error": "data must be a dictionary"}), 400

    logical_id = data['logical_id']
    photo_id = data['photo_id']

    for i in range(16):
        directory.photo_id_to_logical_volume_id[photo_id+i] = logical_id[i]
        directory.photo_id_to_features[photo_id+i] = data['features'][i]
        directory.photo_id_counter += 1

    return jsonify({'message': 'Mapping added successfully'}), 200
    

if __name__ == '__main__':
    directory = HaystackDirectory()

    parser = argparse.ArgumentParser(description='Run the Flask web server.')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the web server on')
    args = parser.parse_args()
    
    app.run(port=args.port)






        
        

    