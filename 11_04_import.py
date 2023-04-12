from TransportOutputModule import *
from pyModbusTCP.client import ModbusClient
from pyModbusTCP.utils import get_bits_from_int
from pyModbusTCP.utils import set_bit
from pyModbusTCP.utils import reset_bit
from pyModbusTCP.utils import test_bit
import paho.mqtt.client as mqtt
from time import sleep
out_1 = TransportOutputModule("192.168.200.236")
mqtt_broker = "192.168.200.161"
mqtt_topic = "Transport_out"
mqtt_client = mqtt.Client()
mqtt_client.connect(mqtt_broker)
mqtt_client.loop_start()

while True:
    print("working")
    sleep(2)
    print(out_1.check_conveyor_workpiece_end('B'))
    read_reg=out_1.get_output_register(8002,1)
    print(read_reg)

    if read_reg == 32768:
        x = 1
        mqtt_client.publish("Transport_out/storage", "The value is {} ".format(x))
    else:
        mqtt_client.publish("Transport_out/storage", "Wrong value is getting")

    mqtt_client.publish("Transport_out/workpiece_end")
    val = out_1.get_output_register()
    #mqtt_client.publish("Transport_out/storage", "The value is {} ".format(val))
    if val[0] == 5141:
        x = 1
        mqtt_client.publish("Transport_out/storage", "The value is {} ".format(x))
    else:
        mqtt_client.publish("Transport_out/storage","Wrong value is getting")
