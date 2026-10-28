import os
import warnings
from tempfile import TemporaryDirectory

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from torchvision import datasets, models, transforms
import numpy as np

import ray.train as train
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig, RunConfig, CheckpointConfig, Checkpoint

# Data augmentation and normalization for training
# Just normalization for validation
data_transforms = {
    "train": transforms.Compose(
        [
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
        ]
    ),
    "val": transforms.Compose(
        [
            transforms.Resize(224),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
        ]
    ),
}

def download_datasets():
    os.system(
        "wget https://download.pytorch.org/tutorial/hymenoptera_data.zip >/dev/null 2>&1"
    )
    os.system("unzip hymenoptera_data.zip >/dev/null 2>&1")

# Download and build torch datasets
def build_datasets():
    torch_datasets = {}
    for split in ["train", "val"]:
        torch_datasets[split] = datasets.ImageFolder(
            os.path.join("./hymenoptera_data", split), data_transforms[split]
        )
    return torch_datasets

def initialize_model():
    # Load pretrained model params
    model = models.resnet50(pretrained=True)

    # Replace the original classifier with a new Linear layer
    num_features = model.fc.in_features
    model.fc = nn.Linear(num_features, 2)

    # Ensure all params get updated during finetuning
    for param in model.parameters():
        param.requires_grad = True
    return model


def evaluate(logits, labels):
    _, preds = torch.max(logits, 1)
    corrects = torch.sum(preds == labels).item()
    return corrects

train_loop_config = {
    "input_size": 224,  # Input image size (224 x 224)
    "batch_size": 32,  # Batch size for training
    "num_epochs": 10,  # Number of epochs to train for
    "lr": 0.001,  # Learning Rate
    "momentum": 0.9,  # SGD optimizer momentum
}

def train_loop_per_worker(configs):
    warnings.filterwarnings("ignore")

    # Calculate the batch size for a single worker
    worker_batch_size = configs["batch_size"]

    # Download dataset once on local rank 0 worker
    if train.get_context().get_local_rank() == 0:
        download_datasets()
    torch.distributed.barrier()

    # Build datasets on each worker
    torch_datasets = build_datasets()

    # Prepare dataloader for each worker
    dataloaders = dict()
    dataloaders["train"] = DataLoader(
        torch_datasets["train"], batch_size=worker_batch_size, shuffle=True
    )
    dataloaders["val"] = DataLoader(
        torch_datasets["val"], batch_size=worker_batch_size, shuffle=False
    )

    # Distribute
    dataloaders["train"] = train.torch.prepare_data_loader(dataloaders["train"])
    dataloaders["val"] = train.torch.prepare_data_loader(dataloaders["val"])

    device = train.torch.get_device()

    # Prepare DDP Model, optimizer, and loss function.
    model = initialize_model()

    # Reload from checkpoint if exists.
    start_epoch = 0
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            state_dict = torch.load(os.path.join(checkpoint_dir, "checkpoint.pt"))
            model.load_state_dict(state_dict['model'])

            start_epoch = state_dict['epoch'] + 1

    model = train.torch.prepare_model(model)

    optimizer = optim.SGD(
        model.parameters(), lr=configs["lr"], momentum=configs["momentum"]
    )
    criterion = nn.CrossEntropyLoss()

    # Start training loops
    for epoch in range(start_epoch, configs["num_epochs"]):
        # Each epoch has a training and validation phase
        for phase in ["train", "val"]:
            if phase == "train":
                model.train()  # Set model to training mode
            else:
                model.eval()  # Set model to evaluate mode

            running_loss = 0.0
            running_corrects = 0

            if train.get_context().get_world_size() > 1:
                dataloaders[phase].sampler.set_epoch(epoch)

            for inputs, labels in dataloaders[phase]:
                inputs = inputs.to(device)
                labels = labels.to(device)

                # zero the parameter gradients
                optimizer.zero_grad()

                # forward
                with torch.set_grad_enabled(phase == "train"):
                    # Get model outputs and calculate loss
                    outputs = model(inputs)
                    loss = criterion(outputs, labels)

                    # backward + optimize only if in training phase
                    if phase == "train":
                        loss.backward()
                        optimizer.step()

                # calculate statistics
                running_loss += loss.item() * inputs.size(0)
                running_corrects += evaluate(outputs, labels)

            size = len(torch_datasets[phase])
            epoch_loss = running_loss / size
            epoch_acc = running_corrects / size

            if train.get_context().get_world_rank() == 0:
                print(
                    "Epoch {}-{} Loss: {:.4f} Acc: {:.4f}".format(
                        epoch, phase, epoch_loss, epoch_acc
                    )
                )

            # Report metrics and checkpoint every epoch
            if phase == "val":
                with TemporaryDirectory() as tmpdir:
                    state_dict = {
                        "epoch": epoch,
                        "model": model.module.state_dict(),
                        "optimizer_state_dict": optimizer.state_dict(),
                    }

                    # In standard DDP training, where the model is the same across all ranks,
                    # only the global rank 0 worker needs to save and report the checkpoint
                    if train.get_context().get_world_rank() == 0:
                        torch.save(state_dict, os.path.join(tmpdir, "checkpoint.pt"))

                    train.report(
                        metrics={"loss": epoch_loss, "acc": epoch_acc},
                        checkpoint=Checkpoint.from_directory(tmpdir),
                    )

if __name__ == "__main__":
    num_workers = int(os.environ.get("NUM_WORKERS", "4"))
    scaling_config = ScalingConfig(
        num_workers=num_workers, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
    )

    checkpoint_config = CheckpointConfig(num_to_keep=3)
    run_config = RunConfig(
        name="finetune-resnet",
        storage_path="/mnt/cluster_storage",
        checkpoint_config=checkpoint_config,
    )

    experiment_path = os.path.expanduser("/mnt/cluster_storage/finetune-resnet")
    if TorchTrainer.can_restore(experiment_path):
        trainer = TorchTrainer.restore(experiment_path,
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            scaling_config=scaling_config,
            run_config=run_config,
        )
    else:
        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            scaling_config=scaling_config,
            run_config=run_config,
        )

    result = trainer.fit()
    print(result)
