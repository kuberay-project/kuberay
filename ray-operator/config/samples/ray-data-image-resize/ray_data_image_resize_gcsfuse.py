from typing import Dict, List
import numpy as np
import ray
import os

import torch
from torchvision import transforms
from PIL import Image

allowed_extensions = ('.png', '.jpg', '.jpeg', '.tif', '.tiff', '.bmp', '.gif')
bucket_prefix = os.environ["BUCKET_PREFIX"]
prefix = "/data/" + bucket_prefix

def find_image_files(directory):
    image_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(allowed_extensions):
                # print ("found file ", file)
                image_files.append(os.path.join(root, file))

    return image_files

class ReadImageFiles:
    def __call__(self, text_batch: List[str]):
        Image.MAX_IMAGE_PIXELS = None
        text = text_batch['item']
        images = []
        for t in text:
            a = np.array(Image.open(t))
            images.append(a)

        return {'results': list(zip(text, images))}

class TransformImages:
    def __init__(self):
        self.transform = transforms.Compose(
            [transforms.ToTensor(), transforms.Resize((256, 256)), transforms.ConvertImageDtype(torch.float)]
        )
    def __call__(self, image_batch: Dict[str, List]):
        images = image_batch['results']
        images_transformed = []
        # input is a tuple of (filepath str, image ndarray)
        for t in images:
            images_transformed.append(self.transform(t[1]))

        return {'results': images_transformed}

def main():
    """
    This is a CPU-only job that reads images from a Google Cloud Storage bucket and resizes them.
    The bucket is mounted as a volume to the underlying pod by the GKE GCSFuse CSI driver.
    """
    ray.init()
    print("Enumerate files in prefix ", prefix)
    image_files = find_image_files(prefix)
    print("For prefix ", prefix, " number of image_files", len(image_files))
    if len(image_files) == 0:
        print ("no files to process")
        return

    dataset = ray.data.from_items(image_files)
    dataset = dataset.flat_map(lambda row: [{'item': row['item']}])
    dataset = dataset.map_batches(ReadImageFiles, batch_size=16, concurrency=2)
    dataset = dataset.map_batches(TransformImages, batch_size=16, concurrency=2)

    dataset_iter = dataset.iter_batches(batch_size=None)
    for _ in dataset_iter:
        pass
    print("done")

if __name__ == "__main__":
    main()
