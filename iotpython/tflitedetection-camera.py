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
import re

CAMERA_WIDTH = 640
CAMERA_HEIGHT = 480


def load_labels(path):
    """Loads the labels file. Supports files with or without index numbers."""
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        labels = {}
        for row_number, content in enumerate(lines):
            pair = re.split(r'[:\s]+', content.strip(), maxsplit=1)
            if len(pair) == 2 and pair[0].strip().isdigit():
                labels[int(pair[0])] = pair[1].strip()
            else:
                labels[row_number] = pair[0].strip()
    return labels

def set_input_tensor(interpreter, image):
    tensor_index = interpreter.get_input_details()[0]['index']
    input_tensor = interpreter.tensor(tensor_index)()[0]
    input_tensor[:, :] = image

def get_output_tensor(interpreter, index):
    """Returns the output tensor at the given index."""
    output_details = interpreter.get_output_details()[index]
    tensor = np.squeeze(interpreter.get_tensor(output_details['index']))
    return tensor

def detect_objects(interpreter, image, threshold):
    """Returns a list of detection results, each a dictionary of object info."""
    set_input_tensor(interpreter, image)
    interpreter.invoke()

    # Get all output details
    boxes = get_output_tensor(interpreter, 0)
    classes = get_output_tensor(interpreter, 1)
    scores = get_output_tensor(interpreter, 2)
    count = int(get_output_tensor(interpreter, 3))

    results = []
    print(classes)
    print(scores)
    for i in range(count):
        if scores[i] >= threshold: # i>0 and 
            result = {
                'bounding_box': boxes[i],
                'class_id': classes[i],
                'score': scores[i]
            }
            results.append(result)
    return results

def visualize_objects(img, results, labels):
    """Draws the bounding box and label for each object in the results."""
    for obj in results:
        # Convert the bounding box figures from relative coordinates
        # to absolute coordinates based on the original resolution
        ymin, xmin, ymax, xmax = obj['bounding_box']
        xmin = int(xmin * CAMERA_WIDTH)
        xmax = int(xmax * CAMERA_WIDTH)
        ymin = int(ymin * CAMERA_HEIGHT)
        ymax = int(ymax * CAMERA_HEIGHT)

        # Overlay the box, label, and score on the camera preview
        startpoint = (xmin, ymin)
        end_point = (xmax, ymax)
        cv2.rectangle(img, startpoint, end_point ,color=(0, 255, 0), thickness=1) # Draw Rectangle with the coordinates
        #annotator.bounding_box([xmin, ymin, xmax, ymax])
        textlabel = '%s  %.2f' % (labels[obj['class_id']], obj['score'])
        # print(obj)
        #print(int(obj['class_id']))
        # print(textlabel)
        text_size = 1
        cv2.putText(img, textlabel, startpoint,  cv2.FONT_HERSHEY_SIMPLEX, text_size, (0,255,0),thickness=1)
        #annotator.text([xmin, ymin], '%s\n%.2f' % (labels[obj['class_id']], obj['score']))

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-i',
        '--image',
        default='data/00871.jpg',
        help='image to be classified')
    parser.add_argument(
        '--model', default='coco_ssd_mobilenet_v1_1/detect.tflite', help='File path of .tflite file.')
    parser.add_argument(
        '--labels', default='coco_ssd_mobilenet_v1_1/labelmap.txt', help='File path of labels file.')#, required=True
    parser.add_argument(
        '--threshold',
        help='Score threshold for detected objects.',
        required=False,
        type=float,
        default=0.4)
    args = parser.parse_args()

    labels = load_labels(args.labels)
    print(labels[61])
    print(labels[83])

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
    print(height)
    width = input_details[0]['shape'][2]
    print(width)

    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        raise Exception("Could not open video device")
    # Set properties. Each returns === True on success (i.e. correct resolution)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, CAMERA_WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT)

    while(True):
        ret, frame = cap.read()
        #rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2BGRA)
        resized = cv2.resize(frame, (width, height)) 
        start_time = time.time()
        results = detect_objects(interpreter, resized, args.threshold)
        elapsed_ms = (time.time() - start_time) * 1000

        visualize_objects(frame, results, labels)
        cv2.putText(frame, '%.1fms' % (elapsed_ms), (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 0, 255), 3)
        cv2.imshow('frame', frame)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            out = cv2.imwrite('capture.jpg', frame)
            break
    
    cap.release()
    cv2.destroyAllWindows()

if __name__ == '__main__':
    main()