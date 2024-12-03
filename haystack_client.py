import argparse
import requests
import json
from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
import math
import os
from tensorflow.keras.applications.mobilenet_v2 import preprocess_input

def client_write20():
    results = []
    for i in range(20):
        file_path = f"tiny-imagenet-png/class_{i}_image_{i*500}.png"
        print(f"Writing {file_path}...")
        if not os.path.exists(file_path):
            result = {"error": f"File not found: {file_path}"}
            results.append(result)
           
            continue
        result = client_write(file_path)
        results.append(result)
        print(result)
    return {"results": "results"}

def client_write(file_path):
    try:
        with open(file_path, 'rb') as file:
            image = Image.open(file).convert('RGB') 
            image = image.resize((224, 224))  
            image_array = np.array(image)
            
            if image_array.shape != (224, 224, 3):
                return {"error": f"Invalid image shape: {image_array.shape}"}
            
            preprocessed_image = preprocess_input(image_array)
            
            image_list = preprocessed_image.tolist()
        response = requests.post("http://localhost:8000/write", json={'photo_data': image_list})
        response.raise_for_status()
        return response.json()
    except FileNotFoundError:
        return {"error": f"File not found: {file_path}"}
    except requests.RequestException as e:
        return {"error": f"Request failed: {e}"}
    except Exception as e:
        return {"error": f"Unexpected error: {e}"}

def client_write_batch(image_paths):
    if len(image_paths) != 16:
        return {"error": "Exactly 16 image paths are required."}

    images_data = []
    for file_path in image_paths:
        try:
            with open(file_path, 'rb') as file:
                image = Image.open(file).convert('RGB')
                image = image.resize((224, 224))
                image_array = np.array(image)

                if image_array.shape != (224, 224, 3):
                    return {"error": f"Invalid image shape: {image_array.shape}"}

                preprocessed_image = preprocess_input(image_array)
                image_list = preprocessed_image.tolist()
                images_data.append(image_list)
        except FileNotFoundError:
            return {"error": f"File not found: {file_path}"}
        except Exception as e:
            return {"error": f"Unexpected error: {e}"}

    try:
        response = requests.post("http://localhost:8000/write_batch", json={'photo_data': images_data})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return {"error": f"Request failed: {e}"}

def client_read(id):
    try:
        response = requests.get(f"http://localhost:8000/read", params={'photo_id': id})
        response.raise_for_status()
        data = response.json()
        data = np.array(data)
        
        # Normalize data from [-1, 1] to [0, 1]
        data = (data + 1) / 2
        
        # Ensure data is in the correct range
        data = np.clip(data, 0, 1)

        # Convert back to image
        image = Image.fromarray((data * 255).astype(np.uint8))
        display_image_with_matplotlib(image)
        return {"status": "success", "message": "Image printed successfully"}

    except requests.RequestException as e:
        return {"error": str(e)}

def client_read_sim(photo_id, num_of_similar, num_cols=5):
    try:
        response = requests.get(f"http://localhost:8000/read_similar", params={'photo_id': photo_id, 'num_of_similar': num_of_similar})
        response.raise_for_status()
        data = response.json()
        actual_img = data['actual']
        similar_imgs = data['similar']

        actual_img = np.array(actual_img)
        actual_img = (actual_img + 1) / 2
        
        # Ensure data is in the correct range
        actual_img = np.clip(actual_img, 0, 1)

        # Convert back to image
        original_image = Image.fromarray((actual_img * 255).astype(np.uint8))

        # Calculate the number of rows needed
        num_rows = math.ceil((len(similar_imgs) + 1) / num_cols)

        # Plotting the images
        fig, axes = plt.subplots(nrows=num_rows, ncols=num_cols, figsize=(15, 5 * num_rows))

        # Flatten the axes array for easy indexing
        axes = axes.flatten()

        axes[0].imshow(original_image)
        axes[0].set_title('Actual Image')
        axes[0].axis('off')

        # Plot the similar images
        for i, sim_img in enumerate(similar_imgs):
            sim_img = np.array(sim_img)
            sim_img = (sim_img + 1) / 2
            sim_img = np.clip(sim_img, 0, 1)
            similar = Image.fromarray((sim_img * 255).astype(np.uint8))
            axes[i + 1].imshow(similar)
            axes[i + 1].set_title(f'Similar Image {i + 1}')
            axes[i + 1].axis('off')

        # Hide any unused subplots
        for j in range(len(similar_imgs) + 1, len(axes)):
            axes[j].axis('off')

        plt.tight_layout()
        plt.show()

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
def display_image_with_matplotlib(image):
    """
    Render the image using matplotlib.
    """
    plt.imshow(image)
    plt.axis('off')  # Hide axes for better visualization
    plt.show()

def client_delete(id):
    try:
        response = requests.delete(f"http://localhost:8000/delete", params={'photo_id': id})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return {"error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="Client for interacting with the server.")
    subparsers = parser.add_subparsers(dest="command")

    write_parser = subparsers.add_parser("write", help="Write data to the server")
    write_parser.add_argument("file_path", type=str, help="JSON data to write")

    read_parser = subparsers.add_parser("read", help="Read data from the server")
    read_parser.add_argument("id", type=int, help="ID of the data to read")

    delete_parser = subparsers.add_parser("delete", help="Delete data from the server")
    delete_parser.add_argument("id", type=int, help="ID of the data to delete")

    read_sim_parser = subparsers.add_parser("read_sim", help="Read data from the server")
    read_sim_parser.add_argument("photo_id", type=int, help="ID of the data to read")
    read_sim_parser.add_argument("num_of_similar", type=list, help="Data to write")

    write_batch_parser = subparsers.add_parser("write_batch", help="Write a batch of data to the server")
    write_batch_parser.add_argument("image_paths", nargs=16, help="16 space-separated image paths")


    subparsers.add_parser("write20", help="Write the first element of all 20 classes in the dataset")

    while True:
        args = parser.parse_args(input("Enter command: ").split())

        if args.command == "write":
            result = client_write(args.file_path)
        elif args.command == "read":
            result = client_read(args.id)
        elif args.command == "delete":
            result = client_delete(args.id)
        elif args.command == "write20":
            result = client_write20()
        elif args.command == "write_batch":
            result = client_write_batch(args.image_paths)
        elif args.command == "read_sim":
            result = client_read_sim(args.photo_id, args.num_of_similar)
        else:
            parser.print_help()
            continue

        print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()