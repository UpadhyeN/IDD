# Import required modules
from TransportInputModule import *
import paho.mqtt.publish as publish
from time import sleep

# Initialize variables

Conveyor = {'A': 0, 'B': 0, 'D': 0, 'H': 0, 'I': 0, 'P': 0, 'R': 0, 'VL': 0, 'Q': 0}
Switch = {'N': 0, 'T': 0, 'F': 0, 'W': 0, 'E': 0, 'G': 0, 'K': 0, 'S': 0, 'O': 0}

# Initialize the Transport Input Module
TIM = TransportInputModule("192.168.200.235")
TIM.set_conveyor_speed_all(0)


def check_workpiece_end_of_conveyor(conveyor_name, next_conveyor_name, switch_name, switch_pre_pos, switch_post_pos):
    # Wait until a workpiece is detected at the end of the conveyor
    while not TIM.check_conveyor_workpiece_end(conveyor_name):
        sleep(0.5)

    # Move the switch to the desired position
    TIM.set_switch(switch_name, pos=switch_pre_pos)

    sleep(1)

    # Update the status of the conveyor and switch
    Conveyor[conveyor_name] = "0"
    publish.single("Transport_Ingoing/Conveyor_" + conveyor_name + "/Status", Conveyor[conveyor_name],
                   hostname="192.168.200.161")

    while not TIM.check_switch_position_reached(switch_name):
        sleep(0.5)

    print("Found workpiece at the switch " + switch_name)
    Switch[switch_name] = "Found workpiece at the switch " + switch_name
    publish.single("Transport_Ingoing/Switch_" + switch_name + "/Status", Switch[switch_name],
                   hostname="192.168.200.161")

    # Wait until the workpiece reaches inside the switch
    while not TIM.check_switch_workpiece(switch_name):
        sleep(0.5)
    TIM.set_switch(switch_name, pos=switch_post_pos)

    # Update the status of the switch
    Switch[switch_name] = "0"
    publish.single("Transport_Ingoing/Switch_" + switch_name + "/Status", Switch[switch_name],
                   hostname="192.168.200.161")

    # Update the status of the next conveyor

    print("Found workpiece at the conveyor " + next_conveyor_name)
    Conveyor[next_conveyor_name] = "Found workpiece at the conveyor " + next_conveyor_name
    publish.single("Transport_Ingoing/Conveyor_" + next_conveyor_name + "/Status", Conveyor[next_conveyor_name],
                   hostname="192.168.200.161")

    sleep(1)


def main():
    TIM = TransportInputModule("192.168.200.235")
    TIM.set_conveyor_speed_all(0)

    for conveyor_id in ['A', 'B', 'D', 'H', 'I', 'L', 'P', 'R', 'V', 'Q']:
        TIM.conveyor_forward(conveyor_id)  # Move conveyor forward

    for switch_id in ['N', 'T', 'F', 'W', 'E', 'G', 'K', 'S', 'O']:
        TIM.set_switch(switch_id, pos=0)
    sleep(10)

    check_workpiece_end_of_conveyor("L", "Q", "N", 3, 1)
    check_workpiece_end_of_conveyor("Q", "H", "T", 3, 1)
    check_workpiece_end_of_conveyor("H", "D", "F", 3, 1)
    check_workpiece_end_of_conveyor("D", "A", "w", 3, 2)
    check_workpiece_end_of_conveyor("A", "B", "E", 3, 2)
    check_workpiece_end_of_conveyor("B", "I", "G", 3, 1)
    check_workpiece_end_of_conveyor("I", "R", "K", 1, 3)
    check_workpiece_end_of_conveyor("R", "P", "S", 2, 1)
    check_workpiece_end_of_conveyor("P", "VL", "O", 1, 3)


if __name__ == "__main__":
    main()