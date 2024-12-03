from flask import Flask, jsonify, request
import os
import pickle
import json

#C:\Users\rushi\Desktop\DS\DS Project\photo_store

app = Flask(__name__)

class Needle:
    def __init__(self, photo_id, data, flags=0):
        self.photo_id = photo_id 
        self.flags = flags  # Default flag is 0 (not deleted) 
        self.data = data  

    def to_dict(self):
        return {
            'photo_id': self.photo_id,
            'flags': self.flags,
            'data': self.data
        }

    @classmethod
    def from_dict(cls, data):
        return cls(photo_id=data['photo_id'], data=data['data'], flags=data['flags'])

class HaystackStore:
    def __init__(self, base_path):
        self.base_path = base_path 
        os.makedirs(self.base_path, exist_ok=True)  
        self.index_data = {}  

    def add_needle(self, needle: Needle, phy_volume):
        try:
            volume_path = os.path.join(self.base_path, f"{phy_volume}.pkl")

            with open(volume_path, "ab") as f:
                position = f.tell()
                pickle.dump(needle.to_dict(), f)
                size = f.tell() - position

            if phy_volume not in self.index_data:
                self.index_data[phy_volume] = {}
            self.index_data[phy_volume][needle.photo_id] = (position, size)

        except Exception as e:
            return f"Error while adding needle: {str(e)}"

    def read_photo(self, photo_id, phy_volume):
        try:
            volume_path = os.path.join(self.base_path, f"{phy_volume}.pkl")

            if phy_volume in self.index_data and photo_id in self.index_data[phy_volume]:
                position, size = self.index_data[phy_volume][photo_id]

                with open(volume_path, "rb") as f:
                    f.seek(position)
                    data_bytes = f.read(size)
                    needle_dict = pickle.loads(data_bytes)
                    needle = Needle.from_dict(needle_dict)

                    if needle.flags == 1:
                        return f"Error: Photo {photo_id} is deleted."
                    return needle.data 
            else:
                return f"Error: Photo {photo_id} not found in volume {phy_volume}"
        except Exception as e:
            return f"Error while reading photo: {str(e)}"
        
    def get_adjacent_keys(self, photo_id, num_similar, phy_volume):
        if phy_volume not in self.index_data:
            print(f"Volume {phy_volume} not found in index data.")
            return []
        
        keys = list(self.index_data[phy_volume].keys())

        if photo_id not in keys:
            print(f"Photo ID {photo_id} not found in volume {phy_volume}.")
            return []
        
        result = []
        index = keys.index(photo_id)

        left_index = index 
        right_index = index
        
        while len(result) < int(num_similar) and (left_index > 0 or right_index < len(keys) - 1):
            if left_index - 1 >= 0:
                left_index -= 1
                result.append(keys[left_index])
            if right_index + 1 < len(keys):
                right_index += 1
                result.append(keys[right_index])
            
        return result

    def read_sim_photo(self, photo_id, phy_volume, num_similar):
        try:

            volume_path = os.path.join(self.base_path, f"{phy_volume}.pkl")


            if phy_volume in self.index_data and photo_id in self.index_data[phy_volume]:
                comibined_data = {'actual': None, 'similar': []}

                adjacent_keys = self.get_adjacent_keys(photo_id, num_similar, phy_volume)

                for key in adjacent_keys:
                    position, size = self.index_data[phy_volume][key]

                    with open(volume_path, "rb") as f:
                        f.seek(position)
                        data_bytes = f.read(size)
                        needle_dict = pickle.loads(data_bytes)
                        needle = Needle.from_dict(needle_dict)

                        if needle.flags ==0:
                            comibined_data['similar'].append(needle.data)

                position, size = self.index_data[phy_volume][photo_id]

                with open(volume_path, "rb") as f:
                    f.seek(position)
                    data_bytes = f.read(size)
                    needle_dict = pickle.loads(data_bytes)
                    needle = Needle.from_dict(needle_dict)

                    if needle.flags == 1:
                        return f"Error: Photo {photo_id} is deleted."
                    else:
                        comibined_data['actual'] = needle.data

                return comibined_data 
            else:
                return f"Error: Photo {photo_id} not found in volume {phy_volume}"
        except Exception as e:
            return f"Error while reading photo: {str(e)}"

    def delete_photo(self, photo_id, phy_volume):
        try:
            if phy_volume in self.index_data and photo_id in self.index_data[phy_volume]:
                position, size = self.index_data[phy_volume][photo_id]
                volume_path = os.path.join(self.base_path, f"{phy_volume}.pkl")

                with open(volume_path, "r+b") as f:  
                    f.seek(position)
                    data_bytes = f.read(size)
                    needle_dict = pickle.loads(data_bytes)
                    needle = Needle.from_dict(needle_dict)

                    needle.flags = 1

                    f.seek(position)
                    pickle.dump(needle.to_dict(), f)

                return f"Photo {photo_id} marked as deleted."
            else:
                return f"Error: Photo {photo_id} not found in volume {phy_volume}"

        except Exception as e:
            return f"Error while deleting photo: {str(e)}"


@app.route('/get', methods=['GET'])
def read_photo():
    try:
        photo_id = request.args.get('key')
        phy_volume = request.args.get('physical_id')
        logical_volume = request.args.get('logical_id')

        photo_data = haystack_store.read_photo(photo_id, phy_volume)

        if "Error" in photo_data:
            return jsonify({"error": photo_data}), 404
        print("Physical volume: ", phy_volume, "Logical volume: ", logical_volume," ID: ", photo_id)
        
        print("Photo data read successfully")

        return jsonify({'data': photo_data}), 200
    
    except Exception as e:
        return jsonify({"error": f"An error occurred while processing the request: {str(e)}"}), 500
    
@app.route('/get_similar', methods=['GET'])
def get_similar_photo():
    try:
        print("gm")
        photo_id = request.args.get('key')
        phy_volume = request.args.get('physical_id')
        logical_volume = request.args.get('logical_id')
        num_similar = request.args.get('num_of_similar')
        print("gm1")

        photo_data = haystack_store.read_sim_photo(photo_id, phy_volume, num_similar)

        if "Error" in photo_data:
            return jsonify({"error": photo_data}), 404
        print("Physical volume: ", phy_volume, "Logical volume: ", logical_volume," ID: ", photo_id)
        
        print("Photo data read successfully")

        return jsonify(photo_data), 200
    
    except Exception as e:
        return jsonify({"error": f"An error occurred while processing the request: {str(e)}"}), 500

@app.route('/write', methods=['POST'])
def upload_photo():
    try:
        data = request.get_json()
        photo_id = data['photo_id']
        flags = 0
        phy_volume = data['physical_id']
        photo_data = data['photo_data']
        logical_volume = data['logical_id']
        
        needle = Needle(photo_id=photo_id, data=photo_data, flags=flags)

        haystack_store.add_needle(needle, phy_volume)
        print("Physical volume: ", phy_volume, "Logical volume: ", logical_volume," ID: ", photo_id)
        print("Photo uploaded successfully!")

        return jsonify({"message": "Photo uploaded successfully!"}), 200
    except Exception as e:
        return jsonify({"error": f"An error occurred while processing the request: {str(e)}"}), 500

@app.route('/remove', methods=['DELETE'])
def delete_photo():
    try:
        photo_id = request.args.get('key')
        phy_volume = request.args.get('physical_id')
        logical_volume = request.args.get('logical_id')

        result = haystack_store.delete_photo(photo_id, phy_volume)

        if "Error" in result:
            return jsonify({"error": result, "status": "error"}), 404

        
        return jsonify({"message": result, "status": "success"}), 200
    except Exception as e:
        return jsonify({"error": f"An error occurred while processing the request: {str(e)}", "status": "error"}), 500


if __name__ == '__main__':
    haystack_store = HaystackStore(base_path="./photo_store")
    app.run(host='0.0.0.0', port=7000)
