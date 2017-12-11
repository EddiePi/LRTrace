import json
from pprint import pprint

def extract(path, metric_name, use_metric_rate, log_type, log_key, aggregation):
	X = []
	Y = []
	with open(path) as data_file:
		all_window = json.load(data_file)

		key_range = dict()
		for window_key, window_value in sorted(all_window.items()):
			for container_key, container_value in window_value.iteritems():
				if container_key == "null" or container_key.endswith("000001"):
					continue

				if container_key not in key_range.keys():
					key_range[container_key] = [0, 0]

				if log_key in container_value[log_type].keys():
					if key_range[container_key][0] == 0:
						key_range[container_key][0] = (container_value["timestamp"])
					else:
						key_range[container_key][1] = (container_value["timestamp"])



		last_metric_value = dict()
		for window_key, window_value in sorted(all_window.items()):
			for container_key, container_value in window_value.iteritems():

				if container_key == "null" or container_key.endswith("000001"):
					continue
				#if we 	enter the elif, it means we will add a new value to (X,y) regardless of whether the key appears in the message. If the key does not exist, we fill by '0'
				elif container_value["timestamp"] >= key_range[container_key][0] and container_value["timestamp"] <= key_range[container_key][1]:

					if container_key not in last_metric_value.keys():
						last_metric_value[container_key] = 0

					if log_key not in container_value[log_type]:
						X.append(0)
					else:
						if aggregation == "sum":
							X.append(log_sum(container_value[log_type][log_key]))
						elif aggregation == "count":
							X.append(log_count(container_value[log_type][log_key]))
						elif aggregation == "avg":
							X.append(log_avg(container_value[log_type][log_key]))

					if use_metric_rate:
						Y.append(container_value[metric_name] - last_metric_value[container_key])
						last_metric_value[container_key] = container_value[metric_name]
					else:
						Y.append(container_value[metric_name])

	# TEST			
	# print(X)
	return X, Y


def log_avg(log_list):
	sum = 0
	for log in log_list:
		sum += log["value"]
	return sum / len(log)

def log_sum(log_list):
	sum = 0
	for log in log_list:
		sum += log["value"]
	return sum

def log_count(log_list):
	return len(log_list)

def build_window(data_list, window_size, window_interval, aggregation):
	index = 0
	parsed_window = []
	total_size = len(data_list)
	need_break = 0
	while 1:
		if index + window_size <= total_size:
			current_window = data_list[index: index + window_size]
		else:
			current_window = data_list[index:]
			need_break = 1

		if aggregation == "avg":
			value_to_add = sum(current_window) / len(current_window)

		elif aggregation == "max":
			value_to_add = max(current_window)

		elif aggregation == "min":
			value_to_add = min(current_window)

		elif aggregation == "delta":
			value_to_add = max(current_window) - min(current_window)

		elif aggregation == "sum":
			value_to_add = sum(current_window)

		elif aggregation == "count":
			value_to_add = len(current_window)

		parsed_window.append(value_to_add)
		if need_break:
			break
		index += window_interval

	return parsed_window




# test_data_path = ['../data/sliding_window0.json', '../data/sliding_window1.json', '../data/sliding_window2.json', '../data/sliding_window3.json', '../data/sliding_window4.json']
# test_metric_name = 'memory'
# test_use_metric_rate = 0
# test_log_key = 'task'
# test_log_type = 'periodMessages'
# test_aggregation = 'count'

# testx, testy = extract(test_data_path, test_metric_name, test_use_metric_rate, test_log_type, test_log_key, test_aggregation)

