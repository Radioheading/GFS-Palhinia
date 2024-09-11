import matplotlib.pyplot as plt
import os

output_folder = '../../static'
output_file = 'append_speed.png'
output_path = os.path.join(output_folder, output_file)

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def read_append_speed_results(filename):
    clients = []
    append_speed = []

    with open(filename, 'r') as file:
        for line in file:
            parts = line.split(',')
            clients.append(int(parts[0].split(':')[1]))
            speed = float(parts[1].split(':')[1].strip().split()[0])
            append_speed.append(speed)

    return clients, append_speed

def plot_append_speed_results(filename, output_file):
    clients, append_speed = read_append_speed_results(filename)

    plt.plot(clients, append_speed, marker='o')
    plt.xlabel('Clients')
    plt.ylabel('Average Write Speed (MB/s)')
    plt.title('Average Write Speed vs. Clients')
    plt.grid(True)
    plt.savefig(output_file)

plot_append_speed_results('append_speed_results.txt', output_path)
