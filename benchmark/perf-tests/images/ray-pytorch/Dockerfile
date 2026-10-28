FROM rayproject/ray:2.46.0

RUN pip install torch torchvision numpy
RUN wget https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/pytorch-mnist/ray_train_pytorch_mnist.py
RUN wget https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-data-image-resize/ray_data_image_resize.py
