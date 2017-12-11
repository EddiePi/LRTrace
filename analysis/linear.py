import extractor
import matplotlib.pyplot as plt
import numpy as np
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

file_paths = ['../data/sliding_window0.json', '../data/sliding_window1.json', '../data/sliding_window2.json', '../data/sliding_window3.json', '../data/sliding_window4.json']
metric_name = 'memory'
use_metric_rate = 0
log_type = 'periodMessages'
log_key = 'task'
aggregation = 'count'

window_size = 6
window_interval = 5

x_train_list = []
y_train_list = []
# x_train_list, y_train_list = extractor.extract(file_paths, metric_name, use_metric_rate, log_type, log_key, aggregation)

for path in file_paths:
	current_x, current_y = extractor.extract(path, metric_name, use_metric_rate, log_type, log_key, aggregation)
	current_x = extractor.build_window(current_x, window_size, window_interval, "sum")
	current_y = extractor.build_window(current_y, window_size, window_interval, "max")

	x_train_list.extend(current_x)
	y_train_list.extend(current_y)


x_train = np.array(x_train_list)[:,np.newaxis]
y_train = [i / 1000000 for i in np.array(y_train_list)]


regr = linear_model.LinearRegression()
regr.fit(x_train, y_train)


y_pred = regr.predict(x_train)
# The coefficients
print('Coefficients: \n', regr.coef_)
# The mean squared error
print("Mean squared error: %.2f"
      % mean_squared_error(y_train, y_train))
# Explained variance score: 1 is perfect prediction
print('Variance score: %.2f' % r2_score(y_train, y_pred))

# Plot outputs
plt.scatter(x_train, y_train,  color='black')
plt.plot(x_train, y_pred, color='blue', linewidth=3)

plt.xticks(())
plt.yticks(())
plt.show()