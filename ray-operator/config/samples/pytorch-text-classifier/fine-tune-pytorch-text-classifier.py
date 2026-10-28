import ray
import torch
import numpy as np
import pytorch_lightning as pl
import torch.nn.functional as F
import ray.train
from torch.utils.data import DataLoader, random_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from datasets import load_dataset
from torchmetrics.classification import BinaryMatthewsCorrCoef
from ray.train.lightning import (
    prepare_trainer,
    RayDDPStrategy,
    RayLightningEnvironment,
    RayTrainReportCallback,
)
from ray.train.torch import TorchTrainer
from ray.train import RunConfig, ScalingConfig, CheckpointConfig, DataConfig

class SentimentModel(pl.LightningModule):
    def __init__(self, lr=2e-5, eps=1e-8):
        super().__init__()
        self.lr = lr
        self.eps = eps
        self.num_classes = 2
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "bert-base-cased", num_labels=self.num_classes
        )
        self.metric = BinaryMatthewsCorrCoef()

    def forward(self, batch):
        input_ids, attention_mask = batch["input_ids"], batch["attention_mask"]
        outputs = self.model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        return logits

    def training_step(self, batch, batch_idx):
        labels = batch["label"]
        logits = self.forward(batch)
        loss = F.cross_entropy(logits.view(-1, self.num_classes), labels)
        self.log("train_loss", loss)
        return loss

    def validation_step(self, batch, batch_idx):
        labels = batch["label"]
        logits = self.forward(batch)
        preds = torch.argmax(logits, dim=1)
        self.metric.update(preds, labels)

    def on_validation_epoch_end(self):
        # torchmetrics aggregates state across workers, so compute() returns the
        # Matthews correlation over the full validation set.
        self.log("matthews_correlation", self.metric.compute(), sync_dist=True)
        self.metric.reset()

    def configure_optimizers(self):
        return torch.optim.AdamW(self.parameters(), lr=self.lr, eps=self.eps)

def tokenize_sentence(batch):
    outputs = tokenizer(
        batch["sentence"].tolist(),
        max_length=128,
        truncation=True,
        padding="max_length",
        return_tensors="np",
    )
    outputs["label"] = batch["label"]
    return outputs

train_func_config = {
    "lr": 1e-5,
    "eps": 1e-8,
    "batch_size": 16,
    "max_epochs": 5,
}

def train_func(config):
    # Unpack the input configs passed from `TorchTrainer(train_loop_config)`
    lr = config["lr"]
    eps = config["eps"]
    batch_size = config["batch_size"]
    max_epochs = config["max_epochs"]

    # Fetch the Dataset shards
    train_ds = ray.train.get_dataset_shard("train")
    val_ds = ray.train.get_dataset_shard("validation")

    # Create a dataloader for Ray Datasets
    train_ds_loader = train_ds.iter_torch_batches(batch_size=batch_size)
    val_ds_loader = val_ds.iter_torch_batches(batch_size=batch_size)

    # Model
    model = SentimentModel(lr=lr, eps=eps)

    trainer = pl.Trainer(
        max_epochs=max_epochs,
        accelerator="auto",
        devices="auto",
        strategy=RayDDPStrategy(),
        plugins=[RayLightningEnvironment()],
        callbacks=[RayTrainReportCallback()],
        enable_progress_bar=False,
    )

    trainer = prepare_trainer(trainer)

    trainer.fit(model, train_dataloaders=train_ds_loader, val_dataloaders=val_ds_loader)


if __name__ == "__main__":
    dataset = load_dataset("glue", "cola")

    train_dataset = ray.data.from_huggingface(dataset["train"])
    validation_dataset = ray.data.from_huggingface(dataset["validation"])

    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    train_dataset = train_dataset.map_batches(tokenize_sentence, batch_format="numpy")
    validation_dataset = validation_dataset.map_batches(tokenize_sentence, batch_format="numpy")

    # Save the top-2 checkpoints according to the evaluation metric
    # The checkpoints and metrics are reported by `RayTrainReportCallback`
    run_config = RunConfig(
        name="ptl-sent-classification",
        checkpoint_config=CheckpointConfig(
            num_to_keep=2,
            checkpoint_score_attribute="matthews_correlation",
            checkpoint_score_order="max",
        ),
    )

    # Schedule 2 workers for DDP training (1 GPU/worker by default)
    scaling_config = ScalingConfig(num_workers=1, use_gpu=True)

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=train_func_config,
        scaling_config=scaling_config,
        run_config=run_config,
        datasets={"train": train_dataset, "validation": validation_dataset}, # <- Feed the Ray Datasets here
    )

    result = trainer.fit()
    print(result)
