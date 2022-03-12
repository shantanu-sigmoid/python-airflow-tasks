import csv  

# header = ['state', 'description', 'temp', 'feels_like_temp', 'min_temp', 'max_temp', 'humidity', 'clouds']
# data = ['Punjab', 'clear sky', 87.87, 91.83, 91.83, 18, 2]

def write_to_csv(header, data, filepath):
    with open(filepath, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header)
        # write the data
        writer.writerow(data)
# write_to_csv(header, data, 'countries.csv')