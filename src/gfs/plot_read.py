import matplotlib.pyplot as plt
import os

output_folder = '../../static'
output_file = 'read_speed.png'
output_path = os.path.join(output_folder, output_file)

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def read_read_speed_results(filename):
    clients = []
    read_speed = []

    with open(filename, 'r') as file:
        for line in file:
            parts = line.split(',')
            clients.append(int(parts[0].split(':')[1]))
            speed = float(parts[1].split(':')[1].strip().split()[0])
            read_speed.append(speed)

    return clients, read_speed

def plot_read_speed_results(filename, output_file):
    clients, read_speed = read_read_speed_results(filename)

    plt.plot(clients, read_speed, marker='o')
    plt.xlabel('Clients')
    plt.ylabel('Average read Speed (MB/s)')
    plt.title('Average read Speed vs. Clients')
    plt.grid(True)
    plt.savefig(output_file)

plot_read_speed_results('read_speed_results.txt', output_path)
