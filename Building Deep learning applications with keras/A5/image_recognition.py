import numpy as np
from keras.utils import img_to_array,load_img
from keras.applications import ResNet50
from keras.applications.resnet import preprocess_input,decode_predictions

# Load Keras' ResNet50 model that was pre-trained against the ImageNet database
model = ResNet50()

# Load the image file, resizing it to 224x224 pixels (required by this model)
img = load_img("/workspaces/tyagisx/Building Deep learning applications with keras/A5/bay.jpg",target_size=(224,224))

# Convert the image to a numpy array
x = img_to_array(img)

# Add a forth dimension since Keras expects a list of images
x = np.expand_dims(x,axis=0)

# Scale the input image to the range used in the trained network
x = preprocess_input(x)

# Run the image through the deep neural network to make a prediction
predictions = model.predict(x)

# Look up the names of the predicted classes. Index zero is the results for the first image.
predicted_classes = decode_predictions(predictions,top=9)

print("This is an image of:")

for imagenet_id, name, likelihood in predicted_classes[0]:
    print(" - {}: {:2f} likelihood".format(name, likelihood))

