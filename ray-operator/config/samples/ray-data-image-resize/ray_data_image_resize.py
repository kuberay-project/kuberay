from typing import Dict

import numpy as np
import ray
import io
import os

import torch
from torchvision import transforms

from google.cloud import storage

bucket_name = os.environ["BUCKET_NAME"]
prefix = os.environ["BUCKET_PREFIX"]
allowed_extensions = ('.png', '.jpg', '.jpeg', '.tif', '.tiff', '.bmp', '.gif')

def list_blobs(bucket_name, prefix):
    client = storage.Client().create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    blob_files = []
    for blob in blobs:
        if blob.name.lower().endswith(allowed_extensions):
            blob_files.append(blob.name)
    return blob_files

def download_blob(blob_name):
    client = storage.Client().create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name['item'])
    data = blob.download_as_bytes()

    from PIL import Image
    Image.MAX_IMAGE_PIXELS = None
    image = np.array(Image.open(io.BytesIO(data)))

    return {"image": image}

def transform_frame(row: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Resize((256, 256)), transforms.ConvertImageDtype(torch.float)]
    )
    row["image"] = transform(row["image"])
    return row

def main():
    """
    This is a CPU-only job that reads images from
    Google Cloud Storage and resizes them.
    """
    ray.init()

    blobs = list_blobs(bucket_name, prefix)
    dataset = ray.data.from_items(blobs)
    dataset = dataset.map(download_blob)
    dataset = dataset.map(transform_frame)

    dataset_iter = dataset.iter_batches(batch_size=None)
    for _ in dataset_iter:
        pass


if __name__ == "__main__":
    main()
