from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import argparse
from PIL import Image
import time
import io
import tflite_runtime.interpreter as tflite
import cv2

def load_labels(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f.readlines()]

def set_input_tensor(interpreter, image):
    tensor_index = interpreter.get_input_details()[0]['index']
    input_tensor = interpreter.tensor(tensor_index)()[0]
    input_tensor[:, :] = image

def classify_image(interpreter, image, top_k=1):
    """Returns a sorted array of classification results."""
    set_input_tensor(interpreter, image)
    interpreter.invoke()
    output_details = interpreter.get_output_details()[0]
    output = np.squeeze(interpreter.get_tensor(output_details['index']))
    #print(output)

    # If the model is quantized (uint8 data), then dequantize the results
    if output_details['dtype'] == np.uint8:
        scale, zero_point = output_details['quantization']
        output = scale * (output - zero_point)

    ordered = np.argpartition(-output, top_k)
    return [(i, output[i]) for i in ordered[:top_k]]

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-i',
        '--image',
        default='data/00871.jpg',
        help='image to be classified')
    parser.add_argument(
        '--model', default='mobilenet/mobilenet_v1_1.0_224_quant.tflite', help='File path of .tflite file.')
    parser.add_argument(
        '--labels', default='mobilenet/labels_mobilenet_quant_v1_224.txt', help='File path of labels file.')#, required=True
    args = parser.parse_args()

    labels = load_labels(args.labels)

    # Load TFLite model and allocate tensors.
    interpreter = tflite.Interpreter(args.model)
    interpreter.allocate_tensors()

    # Get input tensor details
    input_details = interpreter.get_input_details()
    print(input_details)
    output_details = interpreter.get_output_details()
    print(output_details)

    # check the type of the input tensor
    #input_details[0]['dtype']
    floating_model = input_details[0]['dtype'] == np.float32

    # NxHxWxC, H:1, W:2
    height = input_details[0]['shape'][1]
    width = input_details[0]['shape'][2]

    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        raise Exception("Could not open video device")
    # Set properties. Each returns === True on success (i.e. correct resolution)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)

    while(True):
        ret, frame = cap.read()
        #rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2BGRA)
        

        resized = cv2.resize(frame, (width, height)) 
        start_time = time.time()
        results = classify_image(interpreter, resized)
        elapsed_ms = (time.time() - start_time) * 1000
        label_id, prob = results[0]
        result_text = '%s %.2f\n%.1fms' % (labels[label_id], prob, elapsed_ms)
        cv2.putText(frame, result_text, (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 0, 255), 3)
        cv2.imshow('frame', frame)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            out = cv2.imwrite('capture.jpg', frame)
            break

    cap.release()
    cv2.destroyAllWindows()

    # img = Image.open(args.image).resize((width, height))

    
    
    # print(result_text)

if __name__ == '__main__':
    main()