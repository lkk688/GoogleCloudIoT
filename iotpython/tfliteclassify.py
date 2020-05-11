from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import argparse
from PIL import Image
import time
import io
import tflite_runtime.interpreter as tflite

def load_labels(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f.readlines()]

# Load TFLite model and allocate tensors.
interpreter = tflite.Interpreter(model_path='mobilenet/mobilenet_v1_1.0_224_quant.tflite')
interpreter.allocate_tensors()

# Get input tensor details
input_details = interpreter.get_input_details()
print(input_details)
output_details = interpreter.get_output_details()
print(output_details)


# check the type of the input tensor
input_details[0]['dtype']
floating_model = input_details[0]['dtype'] == np.float32

# NxHxWxC, H:1, W:2
height = input_details[0]['shape'][1]
width = input_details[0]['shape'][2]
img = Image.open('data/00871.jpg').resize((width, height))

# add N dim
input_data = np.expand_dims(img, axis=0)

if floating_model:
    input_data = (np.float32(input_data) - 127.5) / 127.5

interpreter.set_tensor(input_details[0]['index'], input_data)

interpreter.invoke()

output_data = interpreter.get_tensor(output_details[0]['index'])
print(output_data)
results = np.squeeze(output_data)
print(results)

top_k = results.argsort()[-5:][::-1]
print(top_k)
labels = load_labels('mobilenet/labels_mobilenet_quant_v1_224.txt')
for i in top_k:
    if floating_model:
        print('{:08.6f}: {}'.format(float(results[i]), labels[i]))
    else:
        print('{:08.6f}: {}'.format(float(results[i] / 255.0), labels[i]))