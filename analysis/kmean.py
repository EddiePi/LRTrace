import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import extractor

file_paths = ['../data/sliding_window0.json', '../data/sliding_window1.json', '../data/sliding_window2.json', '../data/sliding_window3.json', '../data/sliding_window4.json']
metric_name = 'memory'
use_metric_rate = 1
log_type = 'periodMessages'
log_key = 'task'
aggregation = 'count'

window_size = 2
window_interval = 1

x_train_list = []
y_train_list = []
# x_train_list, y_train_list = extractor.extract(file_paths, metric_name, use_metric_rate, log_type, log_key, aggregation)

for path in file_paths:
	current_x, current_y = extractor.extract(path, metric_name, use_metric_rate, log_type, log_key, aggregation)
	current_x = extractor.build_window(current_x, window_size, window_interval, "sum")
	current_y = extractor.build_window(current_y, window_size, window_interval, "max")

	x_train_list.extend(current_x)
	y_train_list.extend(current_y)


x_train = np.array(x_train_list)
y_train = [i / 1000000 for i in np.array(y_train_list)]


X = []
for index in range(len(x_train)):
	X.append([x_train[index], y_train[index]])

X_array = np.array(X)

y_pred = KMeans(n_clusters = 2, random_state = 0).fit_predict(X_array)
plt.scatter(X_array[:, 0], X_array[:, 1], c = y_pred)
plt.show()

